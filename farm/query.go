package modelfarm

import (
	"github.com/coinmeca/go-common/commonmethod/farm"
	"go.mongodb.org/mongo-driver/bson"
)

func (f *FarmDB) BsonForInfo(info *farm.Farm) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": info.ChainId,
		"address": info.Address,
	}
	update := bson.M{
		"$set": bson.M{
			"address":   info.Address,
			"id":        info.Id,
			"main":      info.Main,
			"master":    info.Master,
			"name":      info.Name,
			"symbol":    info.Symbol,
			"decimals":  info.Decimals,
			"stake":     info.Stake,
			"earn":      info.Earn,
			"start":     info.Start,
			"period":    info.Period,
			"duration":  info.Duration,
			"staked":    info.Staked,
			"interest":  info.Interest,
			"total":     info.Total,
			"apr":       info.Apr,
			"claimable": info.Claimable,
		},
	}

	return filter, update
}

func (f *FarmDB) BsonForChart(t *farm.Chart) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": t.ChainId,
		"address": t.Address,
		"time":    t.Time,
	}

	update := bson.M{
		"$set": bson.M{
			"chainId":  t.ChainId,
			"address":  t.Address,
			"time":     t.Time,
			"staked":   t.Staked,
			"interest": t.Interest,
			"value":    t.Value,
			"apr":      t.Apr,
			"earned":   t.Earned,
			"total":    t.Total,
		},
	}

	return filter, update
}

func (f *FarmDB) BsonForFarmRecent(recent *farm.Recent) (bson.M, bson.M) {
	filter := bson.M{"txHash": recent.TxHash}
	update := bson.M{
		"$set": bson.M{
			"chainId":  recent.ChainId,
			"address":  recent.Address,
			"time":     recent.Time,
			"type":     recent.Type,
			"user":     recent.User,
			"amount":   recent.Amount,
			"share":    recent.Share,
			"txHash":   recent.TxHash,
			"updateAt": recent.UpdateAt,
		},
	}

	return filter, update
}
