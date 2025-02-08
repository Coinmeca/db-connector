package accountdb

import (
	"context"
	"errors"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/account"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (a *AccountDB) GetAccountAsset(chainId, user, asset *string) *account.Asset {
	result := &account.Asset{}

	filter := bson.M{
		"chainId": *chainId,
		"account": *user,
		"address": *asset,
	}

	err := a.ColAssets.FindOne(
		context.Background(),
		filter,
	).Decode(result)
	if err != nil {
		commonlog.Logger.Error("Vault SaveChart",
			zap.String("Failed to update chart", err.Error()),
		)
		return nil
	}

	return result
}

func (a *AccountDB) GetAccountAssets(chainId, user *string, assets *[]*string) []*account.Asset {
	filter := a.BsonForGetAccountAssets(*chainId, *user, assets)

	var result []*account.Asset
	cursor, err := a.ColAssets.Find(context.Background(), filter)
	if err != nil {
		commonlog.Logger.Error("GetAccountAssets",
			zap.String("Find", err.Error()),
		)
		return result
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var a *account.Asset
		if err := cursor.Decode(&result); err == nil {
			result = append(result, a)
		}
	}

	return result
}

func (a *AccountDB) UpdateAccountAssets(chainId, user string, assets *[]account.Trade) error {
	var addresses []*string
	for _, asset := range *assets {
		addresses = append(addresses, asset.Address)
	}

	// BulkLoad
	assetlist := a.GetAccountAssets(&chainId, &user, &addresses)
	if assetlist == nil {
		commonlog.Logger.Error("UpdateAccountAssets",
			zap.String("Filtered Asset Results:", "nil"),
		)
		return errors.New("Cannot find any asset.")
	}

	trades := make(map[string]*account.Trade)
	for _, trade := range *assets {
		trades[*trade.Address] = &trade
	}

	var updatedAsset []*account.Asset
	for _, asset := range assetlist {
		updatedAsset = append(
			updatedAsset,
			a.CalculateAccountAsset(
				trades[asset.Address].Type,
				trades[asset.Address].Amount,
				trades[asset.Address].Value,
				asset,
			),
		)
	}

	// BulkUpdate
	models := a.BsonForUpdateAccountAssets(chainId, user, assetlist)
	_, err := a.ColAssets.BulkWrite(context.Background(), *models)
	if err != nil {
		commonlog.Logger.Error("UpdateAccountAssets",
			zap.String("GetAccountAssets", err.Error()),
		)
		return err
	}

	return nil
}

func (a *AccountDB) UpdateAccountAsset(
	tradeType bool,
	chainId, user, address *string,
	amount, value *primitive.Decimal128,
) error {
	asset := a.GetAccountAsset(chainId, user, address)

	a.CalculateAccountAsset(
		tradeType,
		amount,
		value,
		asset,
	)

	filter, update := a.BsonForUpdateAccountAsset(*chainId, *user, asset)
	// fixme: 상황에 따라 필요하면 업데이트된 값 리턴, update에서는 당장 필요없을듯함
	// return nil / err 로 업데이트 성공 / 실패 여부만 열려주면 될듯함
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := a.ColAssets.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(asset)
	if err != nil {
		commonlog.Logger.Error("UpdateVaultInfo",
			zap.String("SaveVaultInfo", err.Error()),
		)
		return err
	}

	return nil
}
