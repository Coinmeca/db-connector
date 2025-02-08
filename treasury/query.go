package modeltreasury

import (
	"time"

	"github.com/coinmeca/go-common/commonmethod/treasury"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (v *TreasuryDB) BsonForTokens(token *bson.M) (bson.M, bson.M) {
	filter := bson.M{
		"symbol": (*token)["symbol"],
		"name":   (*token)["name"],
	}
	update := bson.M{
		"$set": bson.M{
			"symbol": (*token)["symbol"],
			"name":   (*token)["name"],
		},
	}
	return filter, update
}

func (t *TreasuryDB) BsonForSetValue(symbol, name string, value *primitive.Decimal128) (bson.M, bson.M) {
	filter := bson.M{
		"symbol": symbol,
		"name":   name,
	}
	update := bson.M{
		"$set": bson.M{
			"symbol": symbol,
			"name":   name,
			"value":  value,
		},
	}
	return filter, update
}

func (t *TreasuryDB) BsonForChart(chart *treasury.Chart) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chart.ChainId,
		"time":    chart.Time,
	}
	update := bson.M{
		"$set": bson.M{
			"tvl": chart.Tvl,
			"tw":  chart.Tw,
		},
	}
	return filter, update
}

func (t *TreasuryDB) BsonForChartTVL(chainId *string, time *int64, totalValueLocked *primitive.Decimal128) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chainId,
		"time":    time,
	}
	update := bson.M{
		"$set": bson.M{
			"tvl": totalValueLocked,
		},
	}
	return filter, update
}

func (t *TreasuryDB) BsonForChartTV(chainId *string, time *int64, totalVolume *primitive.Decimal128) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chainId,
		"time":    time,
	}
	update := bson.M{
		"$set": bson.M{
			"tv": totalVolume,
		},
	}
	return filter, update
}

func (t *TreasuryDB) BsonForChartTW(chainId *string, totalWeight *primitive.Decimal128) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chainId,
	}
	update := bson.M{
		"$set": bson.M{
			"tw": totalWeight,
		},
	}
	return filter, update
}

func (t *TreasuryDB) BsonForTradingVolume(recent *treasury.Chart) (bson.M, bson.M) {
	startOfDay := time.Unix(recent.Time, 0).Truncate(24 * time.Hour).Unix()
	endOfDay := startOfDay + 86400

	filter := bson.M{
		"chainId": recent.ChainId,
		"time":    bson.M{"$gte": startOfDay, "$lt": endOfDay},
	}
	update := bson.M{
		"$set": bson.M{
			"chainId": recent.ChainId,
			"time":    recent.Time,
		},
		"$inc": bson.M{
			"tv": recent.Tv,
		},
	}

	return filter, update
}
