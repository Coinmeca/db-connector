package marketdb

import (
	"fmt"

	"github.com/coinmeca/go-common/commonmethod/market"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (m *MarketDB) BsonForInfo(info *market.Market) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": info.ChainId,
		"address": info.Address,
	}
	update := bson.M{"$set": bson.M{
		"chainId": info.ChainId,
		"address": info.Address,
		// "nft":	  	 info.Nft,
		"name":      info.Name,
		"symbol":    info.Symbol,
		"base":      info.Base,
		"quote":     info.Quote,
		"price":     info.Price,
		"tick":      info.Tick,
		"fee":       info.Fee,
		"threshold": info.Threshold,
		"lock":      info.Lock,
		"orderbook": info.Orderbook,
		// "liquidity": info.Liquidity,
		// "volume":
	}}
	return filter, update
}

func (m *MarketDB) BsonForChart(chart *market.Chart, interval *int64) (bson.M, bson.M) {
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
			"chainId":  chart.ChainId,
			"address":  chart.Address,
			"time":     chart.Time,
			"open":     chart.Open,
		},
		"$inc": bson.M{
			"volume.base":  chart.Volume.Base,
			"volume.quote": chart.Volume.Quote,
		},
	}

	return filter, update
}

func (m *MarketDB) BsonForChartPrice(chart *market.Chart, interval *int64) (bson.M, bson.M) {
	var zero primitive.Decimal128

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
			"interval":     interval,
			"chainId":      chart.ChainId,
			"address":      chart.Address,
			"time":         chart.Time,
			"open":         chart.Open,
			"volume.base":  zero,
			"volume.quote": zero,
		},
	}

	return filter, update
}

func (m *MarketDB) BsonForChartVolume(chart *market.Chart, interval *int64) (bson.M, bson.M) {
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
			"chainId":  chart.ChainId,
			"address":  chart.Address,
			"time":     chart.Time,
			"open":     chart.Open,
		},
		"$inc": bson.M{
			"volume.base":  chart.Volume.Base,
			"volume.quote": chart.Volume.Quote,
		},
	}
	return filter, update
}

func (m *MarketDB) BsonForChartByIntervals(chart *market.Chart) []bson.M {
	intervals := []int64{1, 5, 15, 30, 60, 120, 240, 1440, 10080, 43200}

	var updates []bson.M

	for _, interval := range intervals {
		time := commonutils.TruncateUnix(chart.Time, interval)
		fmt.Println(time)
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
				"volume.base":  chart.Volume.Base,
				"volume.quote": chart.Volume.Quote,
			},
		}

		updates = append(updates, bson.M{"filter": filter, "update": update})
	}

	return updates
}

func (m *MarketDB) BsonForMarketRecent(recent *market.Recent) (bson.M, bson.M) {
	filter := bson.M{"txHash": recent.TxHash}
	update := bson.M{
		"$set": bson.M{
			"chainId":  recent.ChainId,
			"address":  recent.Address,
			"time":     recent.Time,
			"type":     recent.Type,
			"user":     recent.User,
			"sell":     recent.Sell,
			"amount":   recent.Amount,
			"price":    recent.Price,
			"buy":      recent.Buy,
			"quantity": recent.Quantity,
			"txHash":   recent.TxHash,
			"updateAt": recent.UpdateAt,
		},
	}

	return filter, update
}

func (m *MarketDB) BsonForMarketLiquidity(chainId, address *string, liquidity *[]*market.MarketLiquidity) (bson.M, bson.A) {
	var zero primitive.Decimal128
	filter := bson.M{"chainId": *chainId, "address": *address}

	set := bson.M{}
	for _, asset := range *liquidity {
		field := "liquidity.base"
		if asset.Type {
			field = "liquidity.quote"
		}
		set[field] = bson.M{
			"$cond": bson.A{
				bson.M{"$lt": bson.A{
					bson.M{"$add": bson.A{
						bson.M{"$ifNull": bson.A{"$" + field, zero}},
						asset.Amount,
					}},
					zero,
				}},
				zero,
				bson.M{"$add": bson.A{
					bson.M{"$ifNull": bson.A{"$" + field, zero}},
					asset.Amount,
				}},
			},
		}
	}

	return filter, bson.A{bson.M{"$set": set}}
}
