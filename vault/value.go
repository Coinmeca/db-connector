package modelvault

import (
	"context"
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/vault"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// flag == false == default insert
func (v *VaultDB) SaveValue(chainId, address *string, value *primitive.Decimal128) error {
	filter, update := v.BsonForValue(chainId, address, value)
	opts := options.Update().SetUpsert(false)

	_, err := v.colVault.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		return err
	}

	return nil
}

func (v *VaultDB) GetValue(chainId, address *string) *primitive.Decimal128 {
	filter := bson.M{"chainId": chainId, "address": strings.ToLower(*address)}
	var vault *vault.Vault
	if err := v.colVault.FindOne(context.Background(), filter, nil).Decode(&vault); err != nil {
		return nil
	} else {
		return &vault.Value
	}
}

func (v *VaultDB) GetValueAtTime(time *int64, chainId *string, address *string) *primitive.Decimal128 {
	pipeline := v.BsonForValueAtTime(time, *chainId, *address)

	cursor, err := v.colChartSub.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error(
			"GetValueAtTime",
			zap.String("Cursor Error", err.Error()),
		)
		return nil
	}
	defer cursor.Close(context.Background())

	var result struct {
		Value *primitive.Decimal128 `bson:"value"`
	}

	if cursor.Next(context.Background()) {
		if err := cursor.Decode(&result); err != nil {
			commonlog.Logger.Error(
				"GetValueAtTime",
				zap.String("Decode Error", err.Error()),
			)
			return nil
		}
		return result.Value
	}

	if err := cursor.Err(); err != nil {
		commonlog.Logger.Error(
			"GetValueAtTime",
			zap.String("Cursor Error", err.Error()),
		)
		return nil
	}

	return nil
}

func (v *VaultDB) GetValuesAtTime(time *int64, chainId *string, addresses []string) map[string]*primitive.Decimal128 {
	pipeline := v.BsonForValuesAtTime(time, *chainId, addresses)
	cursor, err := v.colChartSub.Aggregate(context.Background(), pipeline)

	if err != nil {
		commonlog.Logger.Error(
			"GetValuesAtTime",
			zap.String("Cursor Error", err.Error()),
		)
		return nil
	}
	defer cursor.Close(context.Background())

	result := make(map[string]*primitive.Decimal128)

	for cursor.Next(context.Background()) {
		var docs bson.M
		if err := cursor.Decode(&docs); err != nil {
			commonlog.Logger.Error(
				"GetValuesAtTime",
				zap.String("Decode Error", err.Error()),
			)
			return nil
		}

		if results, ok := docs["result"].(bson.M); ok {
			for key, value := range results {
				if value == nil {
					result[key] = nil
				} else {
					// Assert the type of value
					if data, ok := value.(primitive.Decimal128); ok {
						result[key] = &data
					} else {
						// Handle the case where the type assertion fails
						commonlog.Logger.Error(
							"GetValuesAtTime",
							zap.String("Type Assertion Error", fmt.Sprintf("Expected primitive.Decimal128, but got %T", value)),
						)
						return nil
					}
				}
			}
		}
	}

	if err := cursor.Err(); err != nil {
		commonlog.Logger.Error(
			"GetValuesAtTime",
			zap.String("Cursor Error", err.Error()),
		)
		return nil
	}

	return result
}
