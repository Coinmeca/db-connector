package marketdb

import (
	"context"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
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

	filter, update := m.BsonForMarketChart(chart, &interval)
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
	_, err := m.ColChart.Aggregate(
		context.Background(),
		m.BsonForChartByIntervals(chart),
	)

	if err != nil {
		commonlog.Logger.Error("Vault SaveChart with pipeline",
			zap.String("Failed to aggregate chart", err.Error()),
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

func (m *MarketDB) SaveChartVolume(chart *market.Chart, interval int64) error {
	last := m.GetChartLast(&chart.ChainId, &chart.Address, &interval)
	if last != nil && last.Time == chart.Time {
		chart.Open = last.Close
	}

	filter, update := m.BsonForMarketChartVolume(chart, &interval)
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
	_, err := m.ColChart.Aggregate(
		context.Background(),
		m.BsonForMarketChartVolumesByIntervals(chart),
	)

	if err != nil {
		commonlog.Logger.Error("Market SaveChartVolumesByIntervals with pipeline",
			zap.String("Failed to aggregate chart", err.Error()),
		)
		return err
	}

	return nil
}
