package farm

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/farm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (f *FarmDB) SaveFarmChart(t *farm.Chart) error {
	filter, update := f.BsonForChart(t)
	option := options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true)

	err := f.colChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(t)
	if err != nil {
		commonlog.Logger.Error("Farm SaveChart",
			zap.String("Failed to update chart", err.Error()),
		)
	}

	return nil
}

func (f *FarmDB) GetChartAtTime(chainId, address *string, time *int64) *farm.Chart {
	var chart *farm.Chart
	err := f.colChart.FindOne(context.Background(), bson.D{{"time", time}}).Decode(chart)
	if err == nil {
		return chart
	} else {
		return nil
	}
}
