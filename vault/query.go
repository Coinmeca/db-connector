package modelvault

import (
	"strings"

	"github.com/coinmeca/go-common/commonmethod/vault"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (v *VaultDB) BsonForInfo(info *vault.Vault) (bson.M, bson.M) {
	var zero primitive.Decimal128
	var update bson.M

	filter := bson.M{
		"chainId": info.ChainId,
		"address": info.Address,
	}

	if info.Value != zero {
		update = bson.M{
			"$set": bson.M{
				"chainId":  info.ChainId,
				"address":  info.Address,
				"key":      info.Key,
				"name":     info.Name,
				"symbol":   info.Symbol,
				"decimals": info.Decimals,
				"rate":     info.Rate,
				"ratio":    info.Ratio,
				"weight":   info.Weight,
				"need":     info.Need,
				"require":  info.Require,
				"locked":   info.Locked,
				// todo: $cond 로 변경?
				"value": info.Value,
			},
		}
	} else {
		update = bson.M{
			"$set": bson.M{
				"chainId":  info.ChainId,
				"address":  info.Address,
				"key":      info.Key,
				"name":     info.Name,
				"symbol":   info.Symbol,
				"decimals": info.Decimals,
				"rate":     info.Rate,
				"ratio":    info.Ratio,
				"weight":   info.Weight,
				"need":     info.Need,
				"require":  info.Require,
				"locked":   info.Locked,
			},
			"$setOnInsert": bson.M{
				"value": zero,
			},
		}
	}

	return filter, update
}

func (v *VaultDB) BsonForValue(chainId, address *string, value *primitive.Decimal128) (bson.M, bson.M) {
	filter := bson.M{"chainId": chainId, "address": strings.ToLower(*address)}
	update := bson.M{"$set": bson.M{"value": value}}
	return filter, update
}

func (v *VaultDB) BsonForChart(t *vault.Chart, interval *int64) (bson.M, bson.M) {
	var zero primitive.Decimal128
	var open primitive.Decimal128

	filter := bson.M{
		"chainId":  t.ChainId,
		"address":  t.Address,
		"time":     t.Time,
		"interval": interval,
	}

	update := bson.M{
		"$set": bson.M{
			"close": t.Close,
		},
		"$max": bson.M{
			"high": t.Close,
		},
		"$min": bson.M{
			"low": t.Close,
		},
		"$setOnInsert": bson.M{
			"chainId":  t.ChainId,
			"address":  t.Address,
			"time":     t.Time,
			"interval": interval,
			"open":     open,
			"volume":   zero,
		},
	}

	return filter, update
}

func (v *VaultDB) BsonForChartByIntervals(t *vault.Chart) *[]bson.M {
	intervals := []int64{5, 15, 30, 60, 120, 240, 1440, 10080, 43200}

	var zero primitive.Decimal128
	var pipeline []bson.M
	pipeline = append(pipeline, bson.M{
		"$match": bson.M{
			"chainId": t.ChainId,
			"address": t.Address,
		},
	})

	for _, interval := range intervals {
		time := commonutils.TruncateUnix(t.Time, interval)
		last := v.GetChartLast(&t.ChainId, &t.Address, &interval)
		open := t.Close

		if last != nil && last.Time == time {
			open = last.Close
		}

		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"time":     time,
				"interval": interval,
			},
			"$set": bson.M{
				"close": t.Close,
			},
			"$max": bson.M{
				"high": t.Close,
			},
			"$min": bson.M{
				"low": t.Close,
			},
			"$setOnInsert": bson.M{
				"chainId":  t.ChainId,
				"address":  t.Address,
				"time":     time,
				"interval": interval,
				"open":     open,
				"volume":   zero,
			},
		})
	}

	return &pipeline
}

func (v *VaultDB) BsonForChartSub(t *vault.ChartSub) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": t.ChainId,
		"address": t.Address,
		"time":    t.Time,
	}

	update := bson.M{
		"$set": bson.M{
			"chainId":     t.ChainId,
			"address":     t.Address,
			"time":        t.Time,
			"weight":      t.Weight,
			"locked":      t.Locked,
			"value":       t.Value,
			"valueLocked": t.ValueLocked,
		},
	}

	return filter, update
}

// using Trader vault update
// update = bson.A{
// 	bson.M{
// 		"$set": bson.M{
// 			"chainId": t.ChainId,
// 			"address": t.Address,
// 			"time":    t.Time,
// 			"weight":  t.Weight,
// 			"locked":  t.Locked,
// 			"value": bson.M{
// 				"$cond": bson.M{
// 					"if":   bson.M{"$eq": bson.A{bson.M{"$type": "$value"}, "missing"}},
// 					"then": nil,
// 					"else": t.Value,
// 				},
// 			},
// 			"valueLocked": bson.M{
// 				"$cond": bson.M{
// 					"if":   bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$value", nil}}, nil}},
// 					"then": nil,
// 					"else": bson.M{
// 						"$divide": bson.A{
// 							bson.M{"$multiply": bson.A{t.Value, t.Locked}},
// 							math.Pow10(18),
// 						},
// 					},
// 				},
// 			},
// 		},
// 	},
// }

func (v *VaultDB) BsonForVaultWeight(recent *vault.Recent) (bson.M, bson.M) {
	filter := bson.M{"chainId": recent.ChainId, "address": recent.Address}
	update := bson.M{}

	if recent.Type == vault.TradeTypeDeposit {
		update = bson.M{
			"$inc": bson.M{
				"locked": recent.Amount,
				"weight": recent.Meca,
				"mint":   recent.Meca,
				// TODO:
			},
		}
	} else {
		var zero primitive.Decimal128
		update = bson.M{
			"$inc": bson.M{
				"burn":   recent.Meca,
				"locked": commonutils.SubDecimal128(&zero, &recent.Amount),
				"weight": commonutils.SubDecimal128(&zero, &recent.Meca),
				// TODO:
			},
		}
	}
	return filter, update
}

func (v *VaultDB) BsonForVaultRecent(recent *vault.Recent) (bson.M, bson.M) {
	filter := bson.M{"txHash": recent.TxHash}
	update := bson.M{
		"$set": bson.M{
			"chainId":  recent.ChainId,
			"address":  recent.Address,
			"user":     recent.User,
			"time":     recent.Time,
			"type":     recent.Type,
			"amount":   recent.Amount,
			"meca":     recent.Meca,
			"share":    recent.Share,
			"txHash":   recent.TxHash,
			"updateAt": recent.UpdateAt,
		},
	}

	return filter, update
}

func (v *VaultDB) BsonForVaultChart(chart *vault.Chart) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chart.ChainId,
		"address": chart.Address,
		"time":    chart.Time,
	}
	update := bson.M{
		"$set": bson.M{"close": chart.Close},
		"$max": bson.M{"high": chart.Close},
		"$min": bson.M{"low": chart.Close},
		"$setOnInsert": bson.M{
			"chainId": chart.ChainId,
			"address": chart.Address,
			"time":    chart.Time,
			"open":    chart.Open,
		},
		"$inc": bson.M{
			"volume": chart.Volume,
		},
	}

	return filter, update
}

func (v *VaultDB) BsonForVaultChartByIntervals(chart *vault.Chart) *[]bson.M {
	intervals := []int64{5, 15, 30, 60, 120, 240, 1440, 10080, 43200}

	var open primitive.Decimal128
	var pipeline []bson.M
	pipeline = append(pipeline, bson.M{
		"$match": bson.M{
			"chainId": chart.ChainId,
			"address": chart.Address,
		},
	})

	for _, interval := range intervals {
		time := commonutils.TruncateUnix(chart.Time, interval)
		last := v.GetChartLast(&chart.ChainId, &chart.Address, &interval)

		if last != nil && last.Time == chart.Time {
			open = last.Close
		} else {
			open = chart.Close
		}

		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"time":     time,
				"interval": interval,
			},
			"$set": bson.M{"close": chart.Close},
			"$max": bson.M{"high": chart.Close},
			"$min": bson.M{"low": chart.Close},
			"$setOnInsert": bson.M{
				"chainId": chart.ChainId,
				"address": chart.Address,
				"time":    chart.Time,
				"open":    open,
			},
			"$inc": bson.M{
				"volume": chart.Volume,
			},
		})
	}

	return &pipeline
}

func (v *VaultDB) BsonForVaultChartVolumesByIntervals(chart *vault.Chart) *[]bson.M {
	intervals := []int64{5, 15, 30, 60, 120, 240, 1440, 10080, 43200}

	var open primitive.Decimal128
	var pipeline []bson.M
	pipeline = append(pipeline, bson.M{
		"$match": bson.M{
			"chainId": chart.ChainId,
			"address": chart.Address,
		},
	})

	for _, interval := range intervals {
		time := commonutils.TruncateUnix(chart.Time, interval)
		last := v.GetChartLast(&chart.ChainId, &chart.Address, &interval)

		if last != nil && last.Time == chart.Time {
			open = last.Close
		} else {
			open = chart.Close
		}

		pipeline = append(pipeline, bson.M{
			"$match": bson.M{
				"time":     time,
				"interval": interval,
			},
			"$setOnInsert": bson.M{
				"interval": interval,
				"chainId":  chart.ChainId,
				"address":  chart.Address,
				"time":     chart.Time,
				"open":     open,
				"high":     open,
				"low":      open,
				"close":    open,
			},
			"$inc": bson.M{
				"volume": chart.Volume,
			},
		})
	}

	return &pipeline
}

func (v *VaultDB) BsonForVaultChartVolume(chart *vault.Chart, interval *int64) (bson.M, bson.M) {
	filter := bson.M{
		"interval": interval,
		"chainId":  chart.ChainId,
		"address":  chart.Address,
		"time":     chart.Time,
	}
	update := bson.M{
		"$setOnInsert": bson.M{
			"interval": interval,
			"chainId":  chart.ChainId,
			"address":  chart.Address,
			"time":     chart.Time,
			"open":     chart.Open,
			"high":     chart.Open,
			"low":      chart.Open,
			"close":    chart.Open,
		},
		"$inc": bson.M{
			"volume": chart.Volume,
		},
	}
	return filter, update
}

func (v *VaultDB) BsonForVaultChartSub(chart *vault.ChartSub) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chart.ChainId,
		"address": chart.Address,
		"time":    chart.Time,
	}
	update := bson.M{
		"$set": bson.M{
			"weight":      chart.Weight,
			"locked":      chart.Locked,
			"value":       chart.Value,
			"valueLocked": chart.ValueLocked,
		},
	}
	return filter, update
}

func (v *VaultDB) BsonForVaultChartSubAtTime(chart *vault.ChartSub) mongo.Pipeline {
	return mongo.Pipeline{
		{{"$facet", bson.M{
			"gte": bson.A{
				bson.M{"$match": bson.M{
					"chainId": chart.ChainId,
					"address": chart.Address,
					"time":    bson.M{"$gte": chart.Time},
				}},
				bson.M{"$sort": bson.M{"time": 1}},
				bson.M{"$limit": 1},
			},
			"lte": bson.A{
				bson.M{"$match": bson.M{
					"chainId": chart.ChainId,
					"address": chart.Address,
					"time":    bson.M{"$lte": chart.Time},
				}},
				bson.M{"$sort": bson.M{"time": -1}},
				bson.M{"$limit": 1},
			},
		}}},
		{{"$project", bson.M{
			"result": bson.M{
				"$cond": bson.M{
					"if": bson.M{
						"$and": bson.A{
							bson.M{"$gt": bson.A{bson.M{"$size": "$gte"}, 0}},
							bson.M{"$gt": bson.A{bson.M{"$size": "$lte"}, 0}},
						},
					},
					"then": bson.M{
						"$cond": bson.M{
							"if": bson.M{
								"$lt": bson.A{
									bson.M{"$abs": bson.M{"$subtract": bson.A{bson.M{"$arrayElemAt": bson.A{"$gte", 0}}, chart.Time}}},
									bson.M{"$abs": bson.M{"$subtract": bson.A{chart.Time, bson.M{"$arrayElemAt": bson.A{"$lte", 0}}}}},
								},
							},
							"then": bson.M{"$arrayElemAt": bson.A{"$gte", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$lte", 0}},
						},
					},
					"else": bson.M{
						"$cond": bson.M{
							"if":   bson.M{"$gt": bson.A{bson.M{"$size": "$gte"}, 0}},
							"then": bson.M{"$arrayElemAt": bson.A{"$gte", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$lte", 0}},
						},
					},
				},
			},
		}}},
	}
}

func (v *VaultDB) BsonForVaultChartSubsAtTime(time *int64, chainId string, addresses []string) mongo.Pipeline {
	facet := bson.M{}
	for _, address := range addresses {
		address := strings.ToLower(address)
		facet[address+"_gt"] = bson.A{
			bson.M{"$match": bson.M{
				"chainId": chainId,
				"address": address,
				"time":    bson.M{"$gte": time},
			}},
			bson.M{"$sort": bson.M{"time": 1}},
			bson.M{"$limit": 1},
		}
		facet[address+"_lte"] = bson.A{
			bson.M{"$match": bson.M{
				"chainId": chainId,
				"address": address,
				"time":    bson.M{"$lte": time},
			}},
			bson.M{"$sort": bson.M{"time": -1}},
			bson.M{"$limit": 1},
		}
	}

	var result []bson.A
	for _, address := range addresses {
		address := strings.ToLower(address)
		result = append(result, bson.A{
			address,
			bson.M{
				"$cond": bson.M{
					"if": bson.M{
						"$and": bson.A{
							bson.M{"$gt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$" + address + "_gte", bson.A{}}}}, 0}},
							bson.M{"$gt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$" + address + "_lte", bson.A{}}}}, 0}},
						},
					},
					"then": bson.M{
						"$cond": bson.M{
							"if": bson.M{
								"$lt": bson.A{
									bson.M{"$abs": bson.M{"$subtract": bson.A{bson.M{"$arrayElemAt": bson.A{"$" + address + "_gte", 0}}, time}}},
									bson.M{"$abs": bson.M{"$subtract": bson.A{time, bson.M{"$arrayElemAt": bson.A{"$" + address + "_lte", 0}}}}},
								},
							},
							"then": bson.M{"$arrayElemAt": bson.A{"$" + address + "_gte", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$" + address + "_lte", 0}},
						},
					},
					"else": bson.M{
						"$cond": bson.M{
							"if":   bson.M{"$gt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$" + address + "_gte", bson.A{}}}}, 0}},
							"then": bson.M{"$arrayElemAt": bson.A{"$" + address + "_gte", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$" + address + "_lte", 0}},
						},
					},
				},
			},
		})
	}

	return mongo.Pipeline{
		{{"$facet", facet}},
		{{"$project", bson.M{
			"result": bson.M{"$arrayToObject": bson.A{result}},
		}}},
	}
}

func (v *VaultDB) BsonForValueAtTime(time *int64, chainId string, address string) mongo.Pipeline {
	return mongo.Pipeline{
		{{"$facet", bson.M{
			"gte": bson.A{
				bson.M{"$match": bson.M{
					"chainId": chainId,
					"address": address,
					"time":    bson.M{"$gte": time},
				}},
				bson.M{"$sort": bson.M{"time": 1}},
				bson.M{"$limit": 1},
			},
			"lte": bson.A{
				bson.M{"$match": bson.M{
					"chainId": chainId,
					"address": address,
					"time":    bson.M{"$lte": time},
				}},
				bson.M{"$sort": bson.M{"time": -1}},
				bson.M{"$limit": 1},
			},
		}}},
		{{"$project", bson.M{
			"value": bson.M{
				"$cond": bson.M{
					"if": bson.M{
						"$and": bson.A{
							bson.M{"$gt": bson.A{bson.M{"$size": "$gte"}, 0}},
							bson.M{"$gt": bson.A{bson.M{"$size": "$lte"}, 0}},
						},
					},
					"then": bson.M{
						"$cond": bson.M{
							"if": bson.M{
								"$lt": bson.A{
									bson.M{"$abs": bson.M{"$subtract": bson.A{bson.M{"$arrayElemAt": bson.A{"$gte.value", 0}}, time}}},
									bson.M{"$abs": bson.M{"$subtract": bson.A{time, bson.M{"$arrayElemAt": bson.A{"$lte.value", 0}}}}},
								},
							},
							"then": bson.M{"$arrayElemAt": bson.A{"$gte.value", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$lte.value", 0}},
						},
					},
					"else": bson.M{
						"$cond": bson.M{
							"if":   bson.M{"$gt": bson.A{bson.M{"$size": "$gte"}, 0}},
							"then": bson.M{"$arrayElemAt": bson.A{"$gte.value", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$lte.value", 0}},
						},
					},
				},
			},
		}}},
	}
}

func (v *VaultDB) BsonForValuesAtTime(time *int64, chainId string, addresses []string) mongo.Pipeline {
	facet := bson.M{}
	for _, address := range addresses {
		address := strings.ToLower(address)
		facet[address+"_gt"] = bson.A{
			bson.M{"$match": bson.M{
				"chainId": chainId,
				"address": address,
				"time":    bson.M{"$gte": time},
			}},
			bson.M{"$sort": bson.M{"time": 1}},
			bson.M{"$limit": 1},
		}
		facet[address+"_lte"] = bson.A{
			bson.M{"$match": bson.M{
				"chainId": chainId,
				"address": address,
				"time":    bson.M{"$lte": time},
			}},
			bson.M{"$sort": bson.M{"time": -1}},
			bson.M{"$limit": 1},
		}
	}

	var result []bson.A
	for _, address := range addresses {
		address := strings.ToLower(address)
		result = append(result, bson.A{
			address,
			bson.M{
				"$cond": bson.M{
					"if": bson.M{
						"$and": bson.A{
							bson.M{"$gt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$" + address + "_gte", bson.A{}}}}, 0}},
							bson.M{"$gt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$" + address + "_lte", bson.A{}}}}, 0}},
						},
					},
					"then": bson.M{
						"$cond": bson.M{
							"if": bson.M{
								"$lt": bson.A{
									bson.M{"$abs": bson.M{"$subtract": bson.A{bson.M{"$arrayElemAt": bson.A{"$" + address + "_gte.value", 0}}, time}}},
									bson.M{"$abs": bson.M{"$subtract": bson.A{time, bson.M{"$arrayElemAt": bson.A{"$" + address + "_lte.value", 0}}}}},
								},
							},
							"then": bson.M{"$arrayElemAt": bson.A{"$" + address + "_gte.value", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$" + address + "_lte.value", 0}},
						},
					},
					"else": bson.M{
						"$cond": bson.M{
							"if":   bson.M{"$gt": bson.A{bson.M{"$size": bson.M{"$ifNull": bson.A{"$" + address + "_gte", bson.A{}}}}, 0}},
							"then": bson.M{"$arrayElemAt": bson.A{"$" + address + "_gte.value", 0}},
							"else": bson.M{"$arrayElemAt": bson.A{"$" + address + "_lte.value", 0}},
						},
					},
				},
			},
		})
	}

	return mongo.Pipeline{
		{{"$facet", facet}},
		{{"$project", bson.M{
			"result": bson.M{"$arrayToObject": bson.A{result}},
		}}},
	}
}
