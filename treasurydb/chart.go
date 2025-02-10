package treasurydb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/treasury"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (t *TreasuryDB) SaveTreasuryChart(chart *treasury.Chart) error {
	filter, update := t.BsonForChart(chart)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := t.ColChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(chart)
	if err != nil {
		commonlog.Logger.Error("SaveTreasuryChart",
			zap.String("Failed to update chart", err.Error()),
		)
	}
	return nil
}

func (t *TreasuryDB) GetTreasuryChart(chainId *string) []*treasury.Chart {
	filter := bson.M{
		"chainId": chainId,
	}

	var chart []*treasury.Chart
	cursor, err := t.ColChart.Find(context.Background(), filter)

	if err != nil {
		return nil
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var c *treasury.Chart
		if err := cursor.Decode(&chart); err == nil {
			chart = append(chart, c)
		}
	}

	return chart
}

func (t *TreasuryDB) GetTreasuryChartLast(chainId *string) (*treasury.Last, error) {
	now := time.Now().UTC().Truncate(time.Minute * 60 * 24).Unix()
	var last treasury.Last
	filter := bson.M{
		"chainId": chainId,
		"time":    now,
	}
	var chart *treasury.Chart
	err := t.ColChart.FindOne(context.Background(), filter).Decode(&chart)
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
	err := t.ColChart.FindOne(context.Background(), filter, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return primitive.NewDecimal128(0, 0), nil
		}
		return primitive.Decimal128{}, err
	}
	return result.Tv, nil
}
