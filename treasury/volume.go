package treasury

import (
	"context"
	"time"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/treasury"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (t *TreasuryDB) SaveTradingVolume(chart *treasury.Chart) error {
	filter, update := t.BsonForTradingVolume(chart)
	option := options.Update().SetUpsert(true)

	_, err := t.colChart.UpdateOne(context.Background(), filter, update, option)
	if err != nil {
		commonlog.Logger.Error("Treasury SaveTradingVolume",
			zap.String("Failed to update chart", err.Error()),
		)
	}

	return nil
}

func (c *TreasuryDB) UpdateTradingVolume(chainId string, volume *primitive.Decimal128) error {
	now := time.Now().UTC().Truncate(24 * time.Hour)
	startOfDay := now.Unix()

	filter := bson.M{
		"chainId": chainId,
		"time":    startOfDay,
	}
	update := bson.M{
		"$inc": bson.M{
			"tv": volume,
		},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var updatedDoc treasury.Chart
	err := c.colChart.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&updatedDoc)
	if err != nil {
		commonlog.Logger.Error("UpdateTradingVolume", zap.String("Failed to update tv value", err.Error()))
		return err
	}

	commonlog.Logger.Info("UpdateTradingVolume success",
		zap.String("chainId", chainId),
		zap.String("tv", updatedDoc.Tv.String()),
		zap.Int64("time", updatedDoc.Time))

	return nil
}
