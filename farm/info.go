package farm

import (
	"context"
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/farm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (f *FarmDB) BulkWriteInfo(models []mongo.WriteModel) error {
	result, err := f.colFarm.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("FarmDB",
			zap.String("BulkWriteInfo", err.Error()),
		)
		return err
	}
	fmt.Printf("Inserted: %f, Updated: %f, Deleted: %f\n", result.InsertedCount, result.ModifiedCount, result.DeletedCount)
	return nil
}

func (f *FarmDB) SaveFarmInfoFromModel(models *[]mongo.WriteModel, info *farm.Farm) {
	filter, update := f.BsonForInfo(info)
	*models = append(*models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
}

func (f *FarmDB) SaveFarmInfo(info *farm.Farm) error {
	filter, update := f.BsonForInfo(info)
	option := options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true)

	err := f.colFarm.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(info)
	if err != nil {
		commonlog.Logger.Error("UpdateFarmInfo",
			zap.String("SaveFarmInfo", err.Error()),
		)
		return err
	}
	return nil
}

func (f *FarmDB) GetFarm(chainId, address string) *farm.Farm {
	result := &farm.Farm{}

	filter := bson.M{"chainId": chainId, "address": strings.ToLower(address)}
	if err := f.colFarm.FindOne(context.Background(), filter, nil).Decode(&result); err != nil {
		commonlog.Logger.Error("GetFarms",
			zap.String("not found ", err.Error()),
		)
		return nil
	}

	return result
}

func (f *FarmDB) GetFarms(chainId *string) ([]*farm.Farm, error) {
	var farms []*farm.Farm
	filter := bson.M{"chainId": chainId}
	cursor, err := f.colFarm.Find(context.Background(), filter)

	if err != nil {
		commonlog.Logger.Error("GetFarms",
			zap.String("not found ", err.Error()),
		)
		return farms, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var f *farm.Farm
		if err := cursor.Decode(&f); err == nil {
			farms = append(farms, f)
		}
	}

	return farms, nil
}
