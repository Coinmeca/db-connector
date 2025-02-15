package vaultdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/vault"
	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (v *VaultDB) BulkWriteChart(models []mongo.WriteModel) error {
	result, err := v.ColChart.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("VaultDB",
			zap.String("BulkWriteChart", err.Error()),
		)
		return err
	}
	fmt.Printf("Inserted: %v, Updated: %v, Deleted: %v\n", result.InsertedCount, result.ModifiedCount, result.DeletedCount)
	return nil
}

func (v *VaultDB) BulkWriteChartSub(models []mongo.WriteModel) error {
	result, err := v.ColChartSub.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("VaultDB",
			zap.String("BulkWriteChartSub", err.Error()),
		)
		return err
	}
	fmt.Printf("Inserted: %v, Updated: %v, Deleted: %v\n", result.InsertedCount, result.ModifiedCount, result.DeletedCount)
	return nil
}

func (v *VaultDB) SaveChartFromModel(models *[]mongo.WriteModel, exchange *primitive.Decimal128, t *vault.Chart, interval int64) {
	last := v.GetChartLast(&t.ChainId, &t.Address, &interval)
	if last != nil && last.Time == t.Time {
		t.Open = last.Close
	}

	filter, update := v.BsonForChart(t, &interval)
	*models = append(*models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
}

func (v *VaultDB) SaveChart(t *vault.Chart, interval int64) error {
	last := v.GetChartLast(&t.ChainId, &t.Address, &interval)
	if last != nil && last.Time == t.Time {
		t.Open = last.Close
	}

	filter, update := v.BsonForChart(t, &interval)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	err := v.ColChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(t)

	if err != nil {
		commonlog.Logger.Error("Vault SaveChart",
			zap.String("Failed to update chart", err.Error()),
		)
	}

	return nil
}

func (v *VaultDB) SaveChartByIntervals(chart *vault.Chart) error {
	updates := v.BsonForChartByIntervals(chart)

	var models []mongo.WriteModel
	for _, update := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(update["filter"]).
			SetUpdate(update["update"]).
			SetUpsert(true))
	}

	_, err := v.ColChart.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("Vault SaveChartByIntervals bulk write failed",
			zap.String("error", err.Error()),
		)
		return err
	}

	return nil
}

func (v *VaultDB) SaveChartSubFromModel(models *[]mongo.WriteModel, t *vault.ChartSub) {
	filter, update := v.BsonForChartSub(t)
	*models = append(*models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
}

func (v *VaultDB) SaveChartSub(t *vault.ChartSub) error {
	filter, update := v.BsonForChartSub(t)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := v.ColChartSub.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(t)
	if err != nil {
		commonlog.Logger.Error("Vault SaveChartSub",
			zap.String("Failed to update chart", err.Error()),
		)
	}

	return nil
}

func (v *VaultDB) SaveChartVolume(chart *vault.Chart, interval int64) error {
	last := v.GetChartLast(&chart.ChainId, &chart.Address, &interval)
	if last != nil && last.Time == chart.Time {
		chart.Open = last.Close
	}

	filter, update := v.BsonForChart(chart, &interval)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := v.ColChart.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(chart)
	if err != nil {
		commonlog.Logger.Error("SaveChart",
			zap.String("FindOneAndUpdate1", err.Error()),
		)
	}
	return nil
}

func (v *VaultDB) SaveChartVolumesByIntervals(chart *vault.Chart) error {
	updates := v.BsonForChartByIntervals(chart)

	var models []mongo.WriteModel
	for _, update := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(update["filter"]).
			SetUpdate(update["update"]).
			SetUpsert(true))
	}

	_, err := v.ColChart.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("Vault SaveChartByIntervals bulk write failed",
			zap.String("error", err.Error()),
		)
		return err
	}

	return nil
}

func (v *VaultDB) GetChart(chainId, address *string, interval *int64) []*vault.Chart {
	filter := bson.M{
		"chainId":  chainId,
		"address":  strings.ToLower(*address),
		"interval": interval,
	}

	var chart []*vault.Chart
	cursor, err := v.ColChartSub.Find(context.Background(), filter)

	if err != nil {
		return chart
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var c *vault.Chart
		if err := cursor.Decode(&chart); err == nil {
			chart = append(chart, c)
		}
	}

	return chart
}

func (v *VaultDB) GetChartLast(chainId, address *string, interval *int64) *vault.Chart {
	chart := &vault.Chart{}

	filter := bson.M{
		"chainId":  chainId,
		"address":  strings.ToLower(*address),
		"interval": interval,
	}

	err := v.ColChart.FindOne(
		context.Background(),
		filter,
		options.FindOne().SetSort(bson.D{{"time", -1}}),
	).Decode(chart)
	if err != nil && err != mongo.ErrNoDocuments {
		return nil
	}

	return chart
}

func (v *VaultDB) GetChartSub(chainId, address *string) ([]*vault.ChartSub, error) {
	filter := bson.M{
		"chainId": chainId,
		"address": strings.ToLower(*address),
	}

	var chart []*vault.ChartSub
	cursor, err := v.ColChartSub.Find(context.Background(), filter)

	if err != nil {
		return chart, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var c *vault.ChartSub
		if err := cursor.Decode(&chart); err == nil {
			chart = append(chart, c)
		}
	}

	return chart, nil
}

func (v *VaultDB) GetChartSubLast(chainId, address *string) (chartSub *vault.ChartSub) {
	filter := bson.M{
		"chainId": chainId,
		"address": strings.ToLower(*address),
	}

	err := v.ColChartSub.FindOne(
		context.Background(),
		filter,
		options.FindOne().SetSort(bson.D{{"time", -1}}),
	).Decode(chartSub)
	if err != nil && err != mongo.ErrNoDocuments {
		return nil
	}

	return chartSub
}

func (v *VaultDB) GetChartSubAtTime(chainId, address *string, time *int64) (chartSub *vault.ChartSub) {
	cursor, err := v.ColChartSub.Aggregate(
		context.Background(),
		v.BsonForChartSubAtTime(time, *chainId, *address),
	)
	if err != nil {
		commonlog.Logger.Error("Vault",
			zap.String("GetChartSubAtTime Cursor", ""),
		)
		return nil
	}
	defer cursor.Close(context.Background())

	if cursor.Next(context.Background()) {
		err = cursor.Decode(&chartSub)
		if err != nil {
			commonlog.Logger.Error("Vault",
				zap.String("GetChartSubAtTime Decode", ""),
			)
		}
	}

	return chartSub
}

func (v *VaultDB) GetChartSubsAtTime(time *int64, chainId *string, addresses []string) map[string]*vault.ChartSub {
	cursor, err := v.ColChartSub.Aggregate(
		context.Background(),
		v.BsonForChartSubsAtTime(time, *chainId, addresses),
	)

	if err != nil {
		commonlog.Logger.Error(
			"GetChartSubsAtTime",
			zap.String("Cursor Error", err.Error()),
		)
	}
	defer cursor.Close(context.Background())

	result := make(map[string]*vault.ChartSub)

	for cursor.Next(context.Background()) {
		var docs bson.M
		if err := cursor.Decode(&docs); err != nil {
			commonlog.Logger.Error(
				"GetChartSubsAtTime",
				zap.String("Decode Error", err.Error()),
			)
			return nil
		}

		if results, ok := docs["result"].(bson.M); ok {
			for key, value := range results {
				data := &vault.ChartSub{}
				if value == nil {
					result[key] = nil
				} else {
					if err := mapstructure.Decode(value, data); err != nil {
						commonlog.Logger.Error(
							"GetChartSubsAtTime",
							zap.String("Unmarshal Error", err.Error()),
						)
						return nil
					}
					result[key] = data
				}
			}
		}
	}

	if err := cursor.Err(); err != nil {
		commonlog.Logger.Error(
			"GetChartSubAtTime",
			zap.String("Cursor Error", err.Error()),
		)
		return nil
	}

	return result
}
