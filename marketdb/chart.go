package marketdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (m *MarketDB) SaveChart(chart *market.Chart, interval int64) error {
	last := m.GetChartLast(&chart.ChainId, &chart.Address, &interval)
	if last != nil && last.Time == chart.Time {
		chart.Open = last.Close
	}

	filter, update := m.BsonForChart(chart, &interval)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	err := m.ColChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(chart)

	if err != nil {
		commonlog.Logger.Error("Market SaveChart",
			zap.String("Failed to update chart", err.Error()),
		)
	}

	return nil
}

func (m *MarketDB) SaveChartByIntervals(chart *market.Chart) error {
	updates := m.BsonForChartByIntervals(chart)

	var models []mongo.WriteModel
	for _, update := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(update["filter"]).
			SetUpdate(update["update"]).
			SetUpsert(true))
	}
	fmt.Println(commonutils.Prettify(models))

	_, err := m.ColChart.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("Market SaveChartByIntervals bulk write failed",
			zap.String("error", err.Error()),
		)
		return err
	}

	return nil
}

func (m *MarketDB) SaveChartVolume(chart *market.Chart, interval int64) error {
	last := m.GetChartLast(&chart.ChainId, &chart.Address, &interval)
	if last != nil && last.Time == chart.Time {
		chart.Open = last.Close
	}

	filter, update := m.BsonForChartVolume(chart, &interval)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := m.ColChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(chart)
	if err != nil {
		commonlog.Logger.Error("Market SaveChartVolume",
			zap.String("Failed to update chart volume", err.Error()),
		)
	}

	return nil
}

func (m *MarketDB) SaveChartVolumesByIntervals(chart *market.Chart) error {
	updates := m.BsonForChartByIntervals(chart)

	var models []mongo.WriteModel
	for _, update := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(update["filter"]).
			SetUpdate(update["update"]).
			SetUpsert(true))
	}

	_, err := m.ColChart.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("Market SaveChartByIntervals bulk write failed",
			zap.String("error", err.Error()),
		)
		return err
	}

	return nil
}

func (m *MarketDB) GetChart(chainId, address *string, interval *int64) ([]*market.Chart, error) {
	filter := bson.M{
		"chainId":  chainId,
		"address":  strings.ToLower(*address),
		"interval": interval,
	}

	var chart []*market.Chart
	cursor, err := m.ColChart.Find(context.Background(), filter)

	if err != nil {
		return chart, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var c *market.Chart
		if err := cursor.Decode(&chart); err == nil {
			chart = append(chart, c)
		}
	}

	return chart, nil
}

func (m *MarketDB) GetChartLast(chainId, address *string, interval *int64) *market.Chart {
	chart := &market.Chart{}

	filter := bson.M{
		"chainId":  chainId,
		"address":  strings.ToLower(*address),
		"interval": interval,
	}

	err := m.ColChart.FindOne(
		context.Background(),
		filter,
		options.FindOne().SetSort(bson.D{{"time", -1}}),
	).Decode(chart)

	if err != nil && err != mongo.ErrNoDocuments {
		commonlog.Logger.Error("Market GetChartLast",
			zap.String("Failed to get chart", err.Error()),
		)
		return nil
	}

	return chart
}
