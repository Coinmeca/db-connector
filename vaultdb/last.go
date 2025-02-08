package vaultdb

import (
	"context"
	"strings"
	"time"

	"github.com/coinmeca/go-common/commonutils"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/vault"
	va "github.com/coinmeca/go-common/commonmethod/vault"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

func (v *VaultDB) GetLastAll(nowTime *int64, last *vault.Last) error {
	ago24h := *nowTime - 86400

	ctx := context.Background()
	executePipeline := func(pipeline mongo.Pipeline) (bson.M, error) {
		cursor, err := v.ColHistory.Aggregate(ctx, pipeline)
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

	// Define pipelines
	pipelines := map[string]mongo.Pipeline{
		"totalDeposit": {
			{{
				"$match", bson.M{
					"chainId": last.ChainId,
					"address": last.Address,
					"type":    0,
				},
			}},
			{{
				"$group", bson.M{
					"_id":          nil,
					"TotalDeposit": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
				},
			}},
		},
		"totalWithdraw": {
			{{
				"$match", bson.M{
					"chainId": last.ChainId,
					"address": last.Address,
					"type":    1,
				},
			}},
			{{
				"$group", bson.M{
					"_id":           nil,
					"TotalWithdraw": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
				},
			}},
		},
		"deposit24h": {
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
					"TotalDeposit": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
				},
			}},
		},
		"withdraw24h": {
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
					"_id":           nil,
					"TotalWithdraw": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}},
				},
			}},
		},
		"mint24h": {
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
					"_id":       nil,
					"TotalMint": bson.M{"$sum": bson.M{"$toDecimal": "$meca"}},
				},
			}},
		},
		"burn24h": {
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
					"_id":       nil,
					"TotalBurn": bson.M{"$sum": bson.M{"$toDecimal": "$meca"}},
				},
			}},
		},
	}

	results := make(map[string]bson.M)
	for name, pipeline := range pipelines {
		result, err := executePipeline(pipeline)
		if err != nil {
			commonlog.Logger.Error("VaultLast", zap.String(name, err.Error()))
			return err
		}
		results[name] = result
	}

	vaultInfo, err := v.GetVault(&last.ChainId, &last.Address)
	if err != nil {
		return err
	} else {
		last.Deposit = vaultInfo.Deposit
		last.Withdraw = vaultInfo.Withdraw
	}

	if results["deposit24h"] != nil {
		last.Deposit24h = results["deposit24h"]["TotalDeposit"].(primitive.Decimal128)
	}

	if results["withdraw24h"] != nil {
		last.Withdraw24h = results["withdraw24h"]["TotalWithdraw"].(primitive.Decimal128)
	}

	if results["mint24h"] != nil {
		last.Mint = results["mint24h"]["TotalMint"].(primitive.Decimal128)
	}

	if results["burn24h"] != nil {
		last.Burn = results["burn24h"]["TotalBurn"].(primitive.Decimal128)
	}

	deposit24hChange := commonutils.SubDecimal128(&last.Deposit, &last.Deposit24h)
	last.Deposit24hChange = *deposit24hChange

	withdraw24hChange := commonutils.SubDecimal128(&last.Withdraw, &last.Withdraw24h)
	last.Withdraw24hChange = *withdraw24hChange

	// locked, weight, valueLocked
	pipeline := mongo.Pipeline{
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

	cursor, err := v.ColChartSub.Aggregate(ctx, pipeline)
	if err != nil {
		commonlog.Logger.Error("VaultLast",
			zap.String("GetVault valueLocked", err.Error()),
		)
	}
	defer cursor.Close(ctx)

	var chartSub vault.ChartSub
	if cursor.Next(ctx) {
		if err := cursor.Decode(&chartSub); err != nil {
			commonlog.Logger.Error("VaultLast",
				zap.String("GetVault valueLocked&Burn", err.Error()),
			)
		}
		last.Locked = chartSub.Locked
		last.Weight = chartSub.Weight
		last.ValueLocked = chartSub.ValueLocked
	}

	// last 24h ago exchange rate
	pipeline = mongo.Pipeline{
		{{"$match", bson.M{
			"chainId": last.ChainId,
			"address": last.Address,
			"time":    bson.M{"$gte": ago24h},
		}}},
		{{"$sort", bson.M{"timeDiff": 1}}},
		{{"$limit", 1}},
	}

	cursor, err = v.ColChart.Aggregate(ctx, pipeline)
	if err != nil {
		commonlog.Logger.Error("VaultLast",
			zap.String("Aggregate Error:", err.Error()),
		)
	}
	defer cursor.Close(ctx)

	var chart vault.Chart
	if cursor.Next(ctx) {
		if err := cursor.Decode(&chart); err != nil {
			commonlog.Logger.Error("VaultLast",
				zap.String("Decode no data", err.Error()),
			)
		}
		last.Rate = chart.Close
	}
	return nil
}

func (v *VaultDB) GetDeposit24h(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		bson.D{{
			"$match", bson.M{
				"time":    bson.M{"$gte": ago},
				"chainId": chainId,
				"address": strings.ToLower(*address),
				"type":    0,
			},
		}},
		bson.D{{"$group", bson.M{"_id": nil, "total": bson.M{"$sum": bson.M{"$toDecimal": "$amount"}}}}},
	}

	ctx := context.Background()
	cursor, err := v.ColHistory.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result struct {
		Total primitive.Decimal128 `bson:"total"`
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Total, nil
	}
	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetWithdraw24h(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		bson.D{{"$match", bson.M{
			"time":    bson.M{"$gte": ago},
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"type":    1,
		}}},
		bson.D{{"$group", bson.M{"_id": nil, "total": bson.M{"$sum": bson.M{"$toDecimal": "$quantity"}}}}},
	}

	ctx := context.Background()
	cursor, err := v.ColHistory.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result struct {
		Total primitive.Decimal128 `bson:"total"`
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Total, nil
	}
	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetMint24h(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		bson.D{{"$match", bson.M{
			"time":    bson.M{"$gte": ago},
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"type":    0,
		}}},
		bson.D{{"$group", bson.M{"_id": nil, "total": bson.M{"$sum": bson.M{"$toDecimal": "$quantity"}}}}},
	}

	ctx := context.Background()
	cursor, err := v.ColHistory.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result struct {
		Total primitive.Decimal128 `bson:"total"`
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Total, nil
	}
	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetBurn24h(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		bson.D{{"$match", bson.M{
			"time":    bson.M{"$gte": ago},
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"type":    1,
		}}},
		bson.D{{"$group", bson.M{"_id": nil, "total": bson.M{"$sum": bson.M{"$toDecimal": "$quantity"}}}}},
	}

	ctx := context.Background()
	cursor, err := v.ColHistory.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result struct {
		Total primitive.Decimal128 `bson:"total"`
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Total, nil
	}
	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetRate24hAgo(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		// 24시간 전 이후의 데이터 필터링
		{{"$match", bson.M{
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"time":    bson.M{"$gte": ago},
		}}},

		{{"$sort", bson.M{"timeDiff": 1}}},
		// 가장 근접한 문서 1개만 선택
		{{"$limit", 1}},
	}

	ctx := context.Background()
	cursor, err := v.ColChart.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result va.Chart

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Close, nil

	}
	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetLocked24hAgo(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		// 24시간 전 이후의 데이터 필터링
		{{"$match", bson.M{
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"time":    bson.M{"$gte": ago},
		}}},

		{{"$sort", bson.M{"timeDiff": 1}}},
		// 가장 근접한 문서 1개만 선택
		{{"$limit", 1}},
	}

	ctx := context.Background()
	cursor, err := v.ColChartSub.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result va.ChartSub

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Locked, nil

	}
	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetWeight24hAgo(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		// 24시간 전 이후의 데이터 필터링
		{{"$match", bson.M{
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"time":    bson.M{"$gte": ago},
		}}},

		{{"$sort", bson.M{"timeDiff": 1}}},
		// 가장 근접한 문서 1개만 선택
		{{"$limit", 1}},
	}

	ctx := context.Background()
	cursor, err := v.ColChartSub.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result va.ChartSub

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.Weight, nil

	}

	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

func (v *VaultDB) GetVauleLocked24hAgo(chainId, address *string) (*primitive.Decimal128, error) {
	time := time.Now().Unix()
	ago := time - 86400

	pipeline := mongo.Pipeline{
		// 24시간 전 이후의 데이터 필터링
		{{"$match", bson.M{
			"chainId": chainId,
			"address": strings.ToLower(*address),
			"time":    bson.M{"$gte": ago},
		}}},

		{{"$sort", bson.M{"timeDiff": 1}}},
		// 가장 근접한 문서 1개만 선택
		{{"$limit", 1}},
	}

	ctx := context.Background()
	cursor, err := v.ColChartSub.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result va.ChartSub

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		return &result.ValueLocked, nil

	}

	zero := primitive.NewDecimal128(0, 0)
	return &zero, nil
}

// func (v *VaultDB) GetValue24hAgo(chainId *string, token *string) (*primitive.Decimal128, error) {
// 	zero := primitive.NewDecimal128(0, 0)
// 	return &zero, nil
//}
