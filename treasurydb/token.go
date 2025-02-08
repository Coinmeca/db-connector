package treasurydb

import (
	"context"
	"fmt"

	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (t *TreasuryDB) BulkWriteTokens(models *[]mongo.WriteModel) error {
	result, err := t.ColToken.BulkWrite(context.Background(), *models)
	if err != nil {
		commonlog.Logger.Error("TreasuryDB",
			zap.String("BulkWriteTokens", err.Error()),
		)
		return err
	}
	fmt.Printf("Inserted: %v, Updated: %v, Deleted: %v\n", result.InsertedCount, result.ModifiedCount, result.DeletedCount)
	return nil
}

func (t *TreasuryDB) SetValue(symbol, name string, value *primitive.Decimal128) error {
	filter, update := t.BsonForSetValue(symbol, name, value)
	opts := options.Update().SetUpsert(false)

	_, err := t.ColToken.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		return err
	}

	return nil
}

func (t *TreasuryDB) GetValue(name, symbol *string) (*primitive.Decimal128, error) {
	filter := bson.M{
		"name":   name,
		"symbol": symbol,
	}

	value := &primitive.Decimal128{}
	err := t.ColToken.FindOne(context.Background(), filter).Decode(&value)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (t *TreasuryDB) GetValues() (*map[string]primitive.Decimal128, error) {
	cursor, err := t.ColToken.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	valueMap := make(map[string]primitive.Decimal128)

	for cursor.Next(context.Background()) {
		var token bson.M
		if err := cursor.Decode(&token); err != nil {
			continue
		}

		value, ok := token["value"].(primitive.Decimal128)
		if ok {
			valueMap[token["symbol"].(string)] = value
		}
	}

	return &valueMap, nil
}

func (t *TreasuryDB) GetTokens() ([]*bson.M, error) {
	var result []*bson.M
	pipeline := mongo.Pipeline{
		{{"$project", bson.D{
			{"_id", 0},
			{"symbol", 1},
			{"name", 1},
			{"value", 1},
		}}},
	}

	cursor, err := t.ColToken.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("TreasuryDB",
			zap.String("GetTokens ", err.Error()),
		)
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var model *bson.M
		if err := cursor.Decode(&model); err != nil {
			commonlog.Logger.Error("TreasuryDB",
				zap.String("Decode ", err.Error()),
			)
		}
		result = append(result, model)
	}

	return result, nil
}
