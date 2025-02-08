package farmdb

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/farm"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

func (f *FarmDB) GetStaking24h(nowTime *int64, last *farm.Last) {
	ago24h := *nowTime - 86400
	ago48h := ago24h - 86400

	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (primitive.Decimal128, error) {
		cursor, err := f.colHistory.Aggregate(ctx, pipeline)
		if err != nil {
			return primitive.Decimal128{}, err
		}
		defer cursor.Close(ctx)

		var result struct {
			TotalStaking primitive.Decimal128 `bson:"TotalStaking"`
		}
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return primitive.Decimal128{}, err
			}
		}
		return result.TotalStaking, nil
	}

	createPipeline := func(start, end int64, chainID, address string, txType int) mongo.Pipeline {
		return mongo.Pipeline{
			{{
				"$match", bson.M{
					"time":    bson.M{"$gte": start, "$lt": end},
					"chainId": chainID,
					"address": address,
					"type":    txType,
				},
			}},
			{{
				"$group", bson.M{
					"_id":          nil,
					"TotalStaking": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
				},
			}},
		}
	}

	staking24h, err := executePipeline(createPipeline(ago24h, *nowTime, last.ChainId, last.Address, 0))
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Staking 24h", err.Error()))
	}
	staking48h24h, err := executePipeline(createPipeline(ago48h, ago24h, last.ChainId, last.Address, 0))
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Staking 48h24h", err.Error()))
	}

	unstaking24h, err := executePipeline(createPipeline(ago24h, *nowTime, last.ChainId, last.Address, 1))
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Unstaking 24h", err.Error()))
	}
	unstaking48h24h, err := executePipeline(createPipeline(ago48h, ago24h, last.ChainId, last.Address, 1))
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Unstaking 48h24h", err.Error()))
	}

	last.Staking24h = staking24h

	if !staking24h.IsZero() || !staking48h24h.IsZero() {
		last.Staking24hChange = *commonutils.SubDecimal128(&staking24h, &staking48h24h)
	} else {
		last.Staking24hChange = primitive.NewDecimal128(0, 0)
	}

	last.UnStaking24h = unstaking24h

	if !unstaking24h.IsZero() || !unstaking48h24h.IsZero() {
		last.UnStaking24hChange = *commonutils.SubDecimal128(&unstaking24h, &unstaking48h24h)
	} else {
		last.UnStaking24hChange = primitive.NewDecimal128(0, 0)
	}
}

func (f *FarmDB) GetStaking24hBackup(nowTime *int64, last *farm.Last) {
	ago24h := *nowTime - 86400
	ago48h := ago24h - 86400

	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := f.colHistory.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var result bson.M
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// staking (24h ~ now)
	stakingPipeline24h := mongo.Pipeline{
		{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago24h},
				"chainId": last.ChainId,
				"address": last.Address,
				"type":    0,
			},
		}},
		{{
			"$group", bson.M{
				"_id":          nil,
				"TotalStaking": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
			},
		}},
	}

	// staking (48h ~ 24h)
	stakingPipeline48h24h := mongo.Pipeline{
		{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago48h, "$lt": ago24h},
				"chainId": last.ChainId,
				"address": last.Address,
				"type":    0,
			},
		}},
		{{
			"$group", bson.M{
				"_id":          nil,
				"TotalStaking": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
			},
		}},
	}

	// unstaking (24h ~ now)
	unstakingPipeline24h := mongo.Pipeline{
		{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago24h},
				"chainId": last.ChainId,
				"address": last.Address,
				"type":    1,
			},
		}},
		{{
			"$group", bson.M{
				"_id":            nil,
				"TotalUnstaking": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
			},
		}},
	}

	// unstaking (48h ~ 24h)
	unstakingPipeline48h24h := mongo.Pipeline{
		{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago48h, "$lt": ago24h},
				"chainId": last.ChainId,
				"address": last.Address,
				"type":    1,
			},
		}},
		{{
			"$group", bson.M{
				"_id":            nil,
				"TotalUnstaking": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
			},
		}},
	}

	staking24h, err := executePipeline(stakingPipeline24h)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Staking 24h", err.Error()))
	}
	staking48h24h, err := executePipeline(stakingPipeline48h24h)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Staking 48h24h", err.Error()))
	}
	unstaking24h, err := executePipeline(unstakingPipeline24h)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Unstaking 24h", err.Error()))
	}
	unstaking48h24h, err := executePipeline(unstakingPipeline48h24h)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Unstaking 48h24h", err.Error()))
	}

	if staking24h != nil {
		last.Staking24h = staking24h["TotalStaking"].(primitive.Decimal128)
	}
	if staking48h24h != nil {
		totalStaking48h24h := staking48h24h["TotalStaking"].(primitive.Decimal128)
		diffStaking := commonutils.SubDecimal128(&totalStaking48h24h, &last.Staking24h)
		last.Staking24hChange = *diffStaking
	}
	if unstaking24h != nil {
		last.UnStaking24h = unstaking24h["TotalUnstaking"].(primitive.Decimal128)
	}
	if unstaking48h24h != nil {
		totalUnstaking48h24h := unstaking48h24h["TotalUnstaking"].(primitive.Decimal128)
		diffUnstaking := commonutils.SubDecimal128(&totalUnstaking48h24h, &last.UnStaking24h)
		last.UnStaking24hChange = *diffUnstaking
	}
}

func (f *FarmDB) GetInterest24h(nowTime *int64, last *farm.Last) {
	ctx := context.Background()

	latestPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}

	ago24h := *nowTime - 86400
	previous24hPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
				"time":    bson.M{"$lte": ago24h},
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}

	executePipeline := func(pipeline mongo.Pipeline) (primitive.Decimal128, error) {
		cursor, err := f.colChart.Aggregate(ctx, pipeline)
		if err != nil {
			return primitive.Decimal128{}, err
		}
		defer cursor.Close(ctx)

		var result struct {
			InterestAmount primitive.Decimal128 `bson:"interest.amount"`
		}
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return primitive.Decimal128{}, err
			}
		}
		return result.InterestAmount, nil
	}

	latestInterestAmount, err := executePipeline(latestPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Latest Interest Amount", err.Error()))
	}

	previous24hInterestAmount, err := executePipeline(previous24hPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Previous 24h Interest Amount", err.Error()))
	}

	last.Interest24h = latestInterestAmount

	if !previous24hInterestAmount.IsZero() {
		last.Interest24hChange = *commonutils.SubDecimal128(&latestInterestAmount, &previous24hInterestAmount)
	} else {
		last.Interest24hChange = primitive.NewDecimal128(0, 0)
	}
}

func (f *FarmDB) GetInterest24hBackup(nowTime *int64, last *farm.Last) {
	ago24h := *nowTime - 86400
	ago48h := ago24h - 86400

	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := f.colChart.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var result bson.M
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// interest (24h ~ now)
	interestPipeline24h := mongo.Pipeline{
		{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago24h},
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$group", bson.M{
				"_id":           nil,
				"TotalInterest": bson.M{"$sum": bson.M{"$toDecimal": "$interest.amount"}},
			},
		}},
	}

	// interest (48h ~ 24h)
	interestPipeline48h24h := mongo.Pipeline{
		{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago48h, "$lt": ago24h},
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$group", bson.M{
				"_id":           nil,
				"TotalInterest": bson.M{"$sum": bson.M{"$toDecimal": "$interest.amount"}},
			},
		}},
	}

	interest24h, err := executePipeline(interestPipeline24h)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Interest 24h", err.Error()))
	}
	interest48h24h, err := executePipeline(interestPipeline48h24h)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Interest 48h24h", err.Error()))
	}

	if interest24h != nil {
		last.Interest24h = interest24h["TotalInterest"].(primitive.Decimal128)
	}
	if interest48h24h != nil {
		totalInterest48h24h := interest48h24h["TotalInterest"].(primitive.Decimal128)
		diffInterest := commonutils.SubDecimal128(&totalInterest48h24h, &last.Interest24h)
		last.Interest24hChange = *diffInterest
	}
}

func (f *FarmDB) GetStakedWithValue(nowTime *int64, last *farm.Last) {
	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := f.colChart.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var result bson.M
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	latestPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}

	ago24h := *nowTime - 86400
	previousPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
				"time":    bson.M{"$lte": ago24h},
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}

	latestDocument, err := executePipeline(latestPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Latest Doc", err.Error()))
	}

	previousDocument, err := executePipeline(previousPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Previous Doc", err.Error()))
	}

	if latestDocument != nil {
		stakedAmount := latestDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		valueStake := latestDocument["value"].(bson.M)["stake"].(primitive.Decimal128)

		last.Staked = stakedAmount
		last.ValueStaked = *commonutils.MulDecimal128(&stakedAmount, &valueStake)
	}

	if previousDocument != nil {
		prevStakedAmount := previousDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		prevValueStake := previousDocument["value"].(bson.M)["stake"].(primitive.Decimal128)

		last.StakedChange = *commonutils.SubDecimal128(&last.Staked, &prevStakedAmount)

		prevValueStaked := *commonutils.MulDecimal128(&prevStakedAmount, &prevValueStake)
		last.ValueStakedChange = *commonutils.SubDecimal128(&last.ValueStaked, &prevValueStaked)

	} else {
		last.StakedChange = last.Staked
		last.ValueStakedChange = last.ValueStaked
	}
}

//func (f *FarmDB) GetStakedWithValue(nowTime *int64, last *farm.Last) {
//	ctx := context.Background()
//
//	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
//		cursor, err := f.colChart.Aggregate(ctx, pipeline)
//		if err != nil {
//			return nil, err
//		}
//		defer cursor.Close(ctx)
//
//		var result bson.M
//		if cursor.Next(ctx) {
//			if err := cursor.Decode(&result); err != nil {
//				return nil, err
//			}
//		}
//		return result, nil
//	}
//
//	latestPipeline := mongo.Pipeline{
//		{{"$match", bson.M{"chainId": last.ChainId, "address": last.Address}}},
//		{{"$sort", bson.M{"time": -1}}},
//		{{"$limit", 1}},
//	}
//
//	ago24h := *nowTime - 86400
//	previousPipeline := mongo.Pipeline{
//		{{"$match", bson.M{"chainId": last.ChainId, "address": last.Address, "time": bson.M{"$lte": ago24h}}}},
//		{{"$sort", bson.M{"time": -1}}},
//		{{"$limit", 1}},
//	}
//
//	latestDocument, err := executePipeline(latestPipeline)
//	if err != nil {
//		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Latest Doc", err.Error()))
//	}
//
//	if latestDocument != nil {
//		stakedAmount := latestDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
//		valueStake := latestDocument["value"].(bson.M)["stake"].(primitive.Decimal128)
//		last.Staked = stakedAmount
//		last.ValueStaked = *commonutils.MulDecimal128(&stakedAmount, &valueStake)
//	}
//
//	previousDocument, err := executePipeline(previousPipeline)
//	if err != nil {
//		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Previous Doc", err.Error()))
//	}
//
//	if previousDocument != nil {
//		prevStakedAmount := previousDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
//		diffStaked := commonutils.SubDecimal128(&last.Staked, &prevStakedAmount)
//		last.StakedChange = *diffStaked
//
//		prevValueStake := previousDocument["value"].(bson.M)["stake"].(primitive.Decimal128)
//		prevValueStaked := *commonutils.MulDecimal128(&prevStakedAmount, &prevValueStake)
//		diffValueStaked := commonutils.SubDecimal128(&last.ValueStaked, &prevValueStaked)
//		last.ValueStakedChange = *diffValueStaked
//	} else {
//
//		last.StakedChange = last.Staked
//		last.ValueStakedChange = last.ValueStaked
//	}
//}

func (f *FarmDB) GetStakedWithValueBackup2(nowTime *int64, last *farm.Last) {
	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := f.colChart.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var result bson.M
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	latestPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}

	ago24h := *nowTime - 86400
	previous24hPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
				"time":    bson.M{"$lte": ago24h},
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}

	latestDocument, err := executePipeline(latestPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Latest Doc", err.Error()))
	}

	previous24hDocument, err := executePipeline(previous24hPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Previous 24h Doc", err.Error()))
	}

	if latestDocument != nil {
		stakedAmount := latestDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		valueStake := latestDocument["value"].(bson.M)["stake"].(primitive.Decimal128)

		last.Staked = stakedAmount
		last.ValueStaked = *commonutils.MulDecimal128(&stakedAmount, &valueStake)
	}

	if previous24hDocument != nil {
		prevStakedAmount := previous24hDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		diffStaked := commonutils.SubDecimal128(&last.Staked, &prevStakedAmount)
		last.StakedChange = *diffStaked

	} else {
		last.StakedChange = last.Staked
	}

	if previous24hDocument != nil {
		prevStakedAmount := previous24hDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		prevValueStake := previous24hDocument["value"].(bson.M)["stake"].(primitive.Decimal128)
		prevValueStaked := *commonutils.MulDecimal128(&prevStakedAmount, &prevValueStake)
		diffValueStaked := commonutils.SubDecimal128(&last.ValueStaked, &prevValueStaked)
		last.ValueStakedChange = *diffValueStaked

	} else {
		last.ValueStakedChange = last.ValueStaked
	}
}

func (f *FarmDB) GetStakedWithValueBackup(nowTime *int64, last *farm.Last) {
	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := f.colChart.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var result bson.M
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	latestPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}
	previousPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
				"time":    bson.M{"$lt": *nowTime},
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$skip", 1,
		}},
		{{
			"$limit", 1,
		}},
	}

	latestDocument, err := executePipeline(latestPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Latest Doc", err.Error()))
	}
	previousDocument, err := executePipeline(previousPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Previous Doc", err.Error()))
	}

	if latestDocument != nil {
		stakedAmount := latestDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		valueStake := latestDocument["value"].(bson.M)["stake"].(primitive.Decimal128)

		last.Staked = stakedAmount
		last.ValueStaked = *commonutils.MulDecimal128(&stakedAmount, &valueStake)
	}

	if previousDocument != nil {
		prevStakedAmount := previousDocument["staked"].(bson.M)["amount"].(primitive.Decimal128)
		prevValueStake := previousDocument["value"].(bson.M)["stake"].(primitive.Decimal128)

		diffStaked := commonutils.SubDecimal128(&last.Staked, &prevStakedAmount)
		prevValueStaked := *commonutils.MulDecimal128(&prevStakedAmount, &prevValueStake)
		diffValueStaked := commonutils.SubDecimal128(&last.ValueStaked, &prevValueStaked)

		last.StakedChange = *diffStaked
		last.ValueStakedChange = *diffValueStaked
	}
}

func (f *FarmDB) GetTotalInterest(nowTime *int64, last *farm.Last) {
	ctx := context.Background()

	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := f.colChart.Aggregate(ctx, pipeline)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var result bson.M
		if cursor.Next(ctx) {
			if err := cursor.Decode(&result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	latestPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$limit", 1,
		}},
	}
	previousPipeline := mongo.Pipeline{
		{{
			"$match", bson.M{
				"chainId": last.ChainId,
				"address": last.Address,
				"time":    bson.M{"$lt": *nowTime},
			},
		}},
		{{
			"$sort", bson.M{"time": -1},
		}},
		{{
			"$skip", 1,
		}},
		{{
			"$limit", 1,
		}},
	}

	latestDoc, err := executePipeline(latestPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Latest Doc", err.Error()))
	}

	previousDoc, err := executePipeline(previousPipeline)
	if err != nil {
		commonlog.Logger.Error("FarmLast", zap.String("GetFarm Previous Doc", err.Error()))
	}

	if latestDoc != nil {
		last.Interest = latestDoc["total"].(primitive.Decimal128)
	}

	if previousDoc != nil {
		prevTotal := previousDoc["total"].(primitive.Decimal128)

		diffTotal := commonutils.SubDecimal128(&last.Interest, &prevTotal)
		last.InterestChange = *diffTotal
	}
}
