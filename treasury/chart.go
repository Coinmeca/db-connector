package modeltreasury

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/treasury"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (t *TreasuryDB) SaveTreasuryChart(chart *treasury.Chart) error {
	filter, update := t.BsonForChart(chart)
	option := options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true)

	err := t.colChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(t)
	if err != nil {
		commonlog.Logger.Error("SaveTreasuryChart",
			zap.String("Failed to update chart", err.Error()),
		)
	}
	return nil
}

func (t *TreasuryDB) UpdateOrInsertDailyChart(chart *treasury.Chart) error {
	filter, update := t.BsonForChart(chart)
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := t.colChart.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(chart)
	if err != nil {
		commonlog.Logger.Error("UpdateOrInsertDailyChart", zap.String("Failed to update or insert chart", err.Error()))
		return err
	}
	return nil
}

func (t *TreasuryDB) GetTreasuryChart(chainId *string) ([]*treasury.Chart, error) {
	filter := bson.M{
		"chainId": chainId,
	}

	var chart []*treasury.Chart
	cursor, err := t.colChart.Find(context.Background(), filter)

	if err != nil {
		return chart, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var c *treasury.Chart
		if err := cursor.Decode(&chart); err == nil {
			chart = append(chart, c)
		}
	}

	return chart, nil
}

func (t *TreasuryDB) GetTreasuryChartLast(chainId *string) (*treasury.Last, error) {
	now := time.Now().UTC().Truncate(time.Minute * 60 * 24).Unix()
	var last treasury.Last
	filter := bson.M{
		"chainId": chainId,
		"time":    now,
	}
	var chart *treasury.Chart
	err := t.colChart.FindOne(context.Background(), filter).Decode(&chart)
	if err != nil {
		commonlog.Logger.Error("SaveTreasuryChart",
			zap.String("Failed to update chart", err.Error()),
		)
		return nil, err
	}
	last.ChainId = chart.ChainId
	last.Tv = chart.Tv
	last.Tvl = chart.Tvl
	last.Tw = chart.Tw
	last.Chart = *chart

	return &last, nil
}

func (t *TreasuryDB) GetLatestTVValue(chainId string) (primitive.Decimal128, error) {
	var result struct {
		Tv primitive.Decimal128 `bson:"tv"`
	}
	filter := bson.M{"chainId": chainId}
	opts := options.FindOne().SetSort(bson.D{{"time", -1}})
	err := t.colChart.FindOne(context.Background(), filter, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return primitive.NewDecimal128(0, 0), nil
		}
		return primitive.Decimal128{}, err
	}
	return result.Tv, nil
}
