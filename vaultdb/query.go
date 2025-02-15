package vaultdb

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

	filter := bson.M{
		"chainId": info.ChainId,
		"address": info.Address,
	}

	update := bson.M{
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
	}

	if info.Value != zero {
		update["value"] = info.Value
	} else {
		update["value"] = zero
	}

	update = bson.M{"$set": update}
	return filter, update
}

func (v *VaultDB) BsonForValue(chainId, address *string, value *primitive.Decimal128) (bson.M, bson.M) {
	filter := bson.M{"chainId": chainId, "address": strings.ToLower(*address)}
	update := bson.M{"$set": bson.M{"value": value}}
	return filter, update
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

func (v *VaultDB) BsonForVaultWeight(recent *vault.Recent) (bson.M, bson.M) {
	filter := bson.M{"chainId": recent.ChainId, "address": recent.Address}
	update := bson.M{}

	if recent.Type == vault.TradeTypeDeposit {
		update = bson.M{
			"$inc": bson.M{
				"locked": recent.Amount,
				"weight": recent.Meca,
				"mint":   recent.Meca,
			},
		}
	} else {
		var zero primitive.Decimal128
		update = bson.M{
			"$inc": bson.M{
				"burn":   recent.Meca,
				"locked": commonutils.SubDecimal128(&zero, &recent.Amount),
				"weight": commonutils.SubDecimal128(&zero, &recent.Meca),
			},
		}
	}
	return filter, update
}

func (v *VaultDB) BsonForChart(chart *vault.Chart, interval *int64) (bson.M, bson.M) {
	filter := bson.M{
		"interval": interval,
		"chainId":  chart.ChainId,
		"address":  chart.Address,
		"time":     chart.Time,
	}
	update := bson.M{
		"$set": bson.M{"close": chart.Close},
		"$max": bson.M{"high": chart.Close},
		"$min": bson.M{"low": chart.Close},
		"$setOnInsert": bson.M{
			"interval": interval,
			"time":     chart.Time,
			"chainId":  chart.ChainId,
			"address":  chart.Address,
			"open":     chart.Open,
		},
		"$inc": bson.M{
			"volume": chart.Volume,
		},
	}
	return filter, update
}

func (v *VaultDB) BsonForChartPrice(chart *vault.Chart, interval *int64) (bson.M, bson.M) {
	var zero primitive.Decimal128

	filter := bson.M{
		"chainId":  chart.ChainId,
		"address":  chart.Address,
		"time":     chart.Time,
		"interval": interval,
	}

	update := bson.M{
		"$set": bson.M{"close": chart.Close},
		"$max": bson.M{"high": chart.Close},
		"$min": bson.M{"low": chart.Close},
		"$setOnInsert": bson.M{
			"interval": interval,
			"time":     chart.Time,
			"chainId":  chart.ChainId,
			"address":  chart.Address,
			"open":     chart.Open,
			"volume":   zero,
		},
	}

	return filter, update
}

func (m *VaultDB) BsonForChartByIntervals(chart *vault.Chart) []bson.M {
	intervals := []int64{1, 5, 15, 30, 60, 120, 240, 1440, 10080, 43200}

	var updates []bson.M

	for _, interval := range intervals {
		time := commonutils.TruncateUnix(chart.Time, interval)
		last := m.GetChartLast(&chart.ChainId, &chart.Address, &interval)
		open := chart.Close

		if last != nil && time != last.Time {
			open = last.Close
		}

		filter := bson.M{
			"chainId":  chart.ChainId,
			"address":  chart.Address,
			"interval": interval,
			"time":     time,
		}

		update := bson.M{
			"$set": bson.M{
				"close": chart.Close,
			},
			"$max": bson.M{
				"high": chart.Close,
			},
			"$min": bson.M{
				"low": chart.Close,
			},
			"$setOnInsert": bson.M{
				"chainId":  chart.ChainId,
				"address":  chart.Address,
				"interval": interval,
				"time":     time,
				"open":     open,
			},
			"$inc": bson.M{
				"volume": chart.Volume,
			},
		}

		updates = append(updates, bson.M{"filter": filter, "update": update})
	}

	return updates
}

func (v *VaultDB) BsonForChartSub(chart *vault.ChartSub) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chart.ChainId,
		"address": chart.Address,
		"time":    chart.Time,
	}

	update := bson.M{
		"$set": bson.M{
			"chainId":     chart.ChainId,
			"address":     chart.Address,
			"time":        chart.Time,
			"weight":      chart.Weight,
			"locked":      chart.Locked,
			"value":       chart.Value,
			"valueLocked": chart.ValueLocked,
		},
	}

	return filter, update
}

func (v *VaultDB) BsonForChartSubAtTime(time *int64, chainId, address string) mongo.Pipeline {
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
									bson.M{"$abs": bson.M{"$subtract": bson.A{bson.M{"$arrayElemAt": bson.A{"$gte", 0}}, time}}},
									bson.M{"$abs": bson.M{"$subtract": bson.A{time, bson.M{"$arrayElemAt": bson.A{"$lte", 0}}}}},
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

func (v *VaultDB) BsonForChartSubsAtTime(time *int64, chainId string, addresses []string) mongo.Pipeline {
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

// using Trader vault update
// update = bson.A{
// 	bson.M{
// 		"$set": bson.M{
// 			"chainId": chart.ChainId,
// 			"address": chart.Address,
// 			"time":    chart.Time,
// 			"weight":  chart.Weight,
// 			"locked":  chart.Locked,
// 			"value": bson.M{
// 				"$cond": bson.M{
// 					"if":   bson.M{"$eq": bson.A{bson.M{"$type": "$value"}, "missing"}},
// 					"then": nil,
// 					"else": chart.Value,
// 				},
// 			},
// 			"valueLocked": bson.M{
// 				"$cond": bson.M{
// 					"if":   bson.M{"$eq": bson.A{bson.M{"$ifNull": bson.A{"$value", nil}}, nil}},
// 					"then": nil,
// 					"else": bson.M{
// 						"$divide": bson.A{
// 							bson.M{"$multiply": bson.A{chart.Value, chart.Locked}},
// 							math.Pow10(18),
// 						},
// 					},
// 				},
// 			},
// 		},
// 	},
// }

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
