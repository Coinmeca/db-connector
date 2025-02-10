package vaultdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/vault"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (v *VaultDB) BulkWriteInfo(models []mongo.WriteModel) error {
	result, err := v.ColVault.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("VaultDB",
			zap.String("BulkWriteInfo", err.Error()),
		)
		return err
	}
	fmt.Printf("Inserted: %v, Updated: %v, Deleted: %v\n", result.InsertedCount, result.ModifiedCount, result.DeletedCount)
	return nil
}

// below kbi works : abi -> inset db
func (v *VaultDB) SaveVaultInfoFromModel(models *[]mongo.WriteModel, info *vault.Vault) {
	filter, update := v.BsonForInfo(info)
	*models = append(*models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
}

// below kbi works : abi -> inset db
func (v *VaultDB) SaveVaultInfo(info *vault.Vault) error {
	filter, update := v.BsonForInfo(info)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := v.ColVault.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(info)
	if err != nil {
		commonlog.Logger.Error("UpdateVaultInfo",
			zap.String("SaveVaultInfo", err.Error()),
		)
		return err
	}
	return nil
}

func (v *VaultDB) SaveVaultInfoWithWeight(recent *vault.Recent, info *vault.Vault) error {
	filter, update := v.BsonForVaultWeight(recent)
	option := options.FindOneAndUpdate().SetUpsert(true)

	err := v.ColVault.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(info)
	if err != nil {
		commonlog.Logger.Error("SaveVaultInfo",
			zap.String("BsonforSaveVaultInfo", err.Error()),
		)
		return err
	}

	return nil
}

func (v *VaultDB) GetVault(chainId, address *string) (*vault.Vault, error) {
	var vault vault.Vault
	filter := bson.M{"chainId": chainId, "address": strings.ToLower(*address)}
	if err := v.ColVault.FindOne(context.Background(), filter, nil).Decode(&vault); err != nil {
		commonlog.Logger.Error("GetVaults",
			zap.String("not found ", err.Error()),
		)
		return nil, err
	}
	return &vault, nil
}

func (v *VaultDB) GetVaults(chainId *string) ([]*vault.Vault, error) {
	var vaults []*vault.Vault
	filter := bson.M{"chainId": chainId}
	cursor, err := v.ColVault.Find(context.Background(), filter)

	if err != nil {
		commonlog.Logger.Error("GetVaults",
			zap.String("not found ", err.Error()),
		)
		return vaults, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var v *vault.Vault
		if err := cursor.Decode(&v); err == nil {
			vaults = append(vaults, v)
		}
	}

	return vaults, nil
}

func (v *VaultDB) GetAllVaults() ([]*vault.Vault, error) {
	var vaults []*vault.Vault
	cursor, err := v.ColVault.Find(context.Background(), bson.M{})

	if err != nil {
		return vaults, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var v *vault.Vault
		if err := cursor.Decode(&v); err == nil {
			vaults = append(vaults, v)
		}
	}

	return vaults, nil
}

func (v *VaultDB) GetKeyTokens(chainId *string) ([]*vault.Vault, error) {
	var keys []*vault.Vault
	filter := bson.M{"chainId": chainId, "key": true}
	cursor, err := v.ColVault.Find(context.Background(), filter)
	if err != nil {
		commonlog.Logger.Error("GetKeyTokens",
			zap.String("GetKeyTokens ", err.Error()),
		)
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var v *vault.Vault
		if err := cursor.Decode(&keys); err != nil {
			commonlog.Logger.Error("GetKeyTokens",
				zap.String("GetKeyTokens ", err.Error()),
			)
			return nil, err
		}
		keys = append(keys, v)
	}

	return keys, nil
}

func (v *VaultDB) GetAllKeyTokenSymbols() ([]*vault.Vault, error) {
	var result []*vault.Vault
	pipeline := mongo.Pipeline{
		{{"$match", bson.M{"key": true}}},
		{{"$project", bson.D{
			{"_id", 0},
			{"chainId", 1},
			{"symbol", 1},
			{"address", 1},
		}}},
	}

	cursor, err := v.ColVault.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("GetAllKeyTokenSymbols",
			zap.String("GetAllKeyTokenSymbols ", err.Error()),
		)
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var model *vault.Vault
		if err := cursor.Decode(&model); err != nil {
			commonlog.Logger.Error("GetAllKeyTokenSymbols",
				zap.String("GetAllKeyTokenSymbols ", err.Error()),
			)
		}
		result = append(result, model)
	}

	return result, nil
}

func (v *VaultDB) GetKeyTokenSymbols(chainId *string) ([]*vault.Vault, error) {
	var result []*vault.Vault
	pipeline := mongo.Pipeline{
		{{"$match", bson.M{"chainId": chainId, "key": true}}},
		{{"$group", bson.D{{"symbol", "$symbol"}, {"address", "$address"}}}},
	}

	cursor, err := v.ColVault.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("GetKeyTokenSymbols",
			zap.String("GetKeyTokenSymbols ", err.Error()),
		)
		return nil, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var model *vault.Vault
		if err := cursor.Decode(&model); err != nil {
			commonlog.Logger.Error("GetKeyTokenSymbols",
				zap.String("GetKeyTokenSymbols ", err.Error()),
			)
		}
		result = append(result, model)
	}
	return result, nil
}

// func (v *VaultDB) GetAllNonKeyTokenSymbols() ([]*vault.Vault, error) {
// 	pipeline := mongo.Pipeline{
// 		{{"$match", bson.M{"key": false}}},
// 		{{"$group", bson.D{{"symbol", "$symbol"}, {"chainId", "$chainId"}, {"address", "$address"}}}},
// 	}

// 	cursor, err := v.ColVault.Aggregate(context.Background(), pipeline)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer cursor.Close(context.Background())

// 	var results []bson.M
// 	if err = cursor.All(context.Background(), &results); err != nil {
// 		fmt.Println(err)
// 		return nil, err
// 	}

// 	var tokens []*vault.Vault
// 	for _, result := range results {
// 		token := &vault.Vault{
// 			ChainId: result["chainId"].(string),
// 			Address: result["address"].(string),
// 			Symbol:  result["symbol"].(string),
// 		}
// 		tokens = append(tokens, token)
// 	}

// 	return tokens, nil
// }

func (v *VaultDB) GetNonKeyTokenSymbols(chainId *string) ([]*vault.Vault, error) {
	pipeline := mongo.Pipeline{
		{{"$match", bson.M{"chainId": chainId, "key": false}}},
		{{"$group", bson.D{{"symbol", "$symbol"}, {"address", "$address"}}}},
	}

	cursor, err := v.ColVault.Aggregate(context.Background(), pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		fmt.Println(err)
		return nil, err
	}

	var tokens []*vault.Vault
	for _, result := range results {
		token := &vault.Vault{
			ChainId: *chainId,
			Address: result["address"].(string),
			Symbol:  result["symbol"].(string),
		}
		tokens = append(tokens, token)
	}

	return tokens, nil
}

// Check if the document exists
// existingVault := vault.Vault{}
// err := v.ColHistory.FindOne(context.Background(), filter).Decode(&existingVault)
// if err != nil && err != mongo.ErrNoDocuments {
// 	commonlog.Logger.Error("SaveVaultMintAndBurn - FindOne",
// 		zap.String("FindOne error", err.Error()),
// 	)
// 	return err
// }

// if err == mongo.ErrNoDocuments {
// 	initialUpdate := bson.M{
// 		"$setOnInsert": bson.M{
// 			"mint": primitive.NewDecimal128(0, 0),
// 			"burn": primitive.NewDecimal128(0, 0),
// 		},
// 	}
// 	_, err := v.ColHistory.UpdateOne(context.Background(), filter, initialUpdate, options.Update().SetUpsert(true))
// 	if err != nil {
// 		commonlog.Logger.Error("SaveVaultMintAndBurn - Initial UpdateOne",
// 			zap.String("UpdateOne error", err.Error()),
// 		)
// 		return err
// 	}
// }
