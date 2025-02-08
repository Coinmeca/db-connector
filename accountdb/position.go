package accountdb

import (
	"context"
	"log"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/account"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (a *AccountDB) GetAccountPosition(chainId, user, pay, item *string) *account.Position {
	var result struct {
		Positions []account.Position `bson:"position"`
	}

	filter, projection := a.BsonForGetAccountPosition(chainId, user, pay, item)

	option := options.FindOne().SetProjection(projection)
	err := a.ColAssets.FindOne(
		context.Background(),
		filter,
		option,
	).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}

	if len(result.Positions) == 0 {
		commonlog.Logger.Error("GetAccountPosition",
			zap.String("Account:", *user),
			zap.String("Pay:", *pay),
			zap.String("Item:", *item),
		)
		return nil
	}

	return &result.Positions[0]
}

func (a *AccountDB) GetAccountPositions(chainId, user, asset *string) *[]account.Position {
	var result struct {
		Positions []account.Position `bson:"position"`
	}

	filter, projection := a.BsonForGetAccountPositions(chainId, user, asset)

	option := options.FindOne().SetProjection(projection)
	err := a.ColAssets.FindOne(
		context.Background(),
		filter,
		option,
	).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}

	if len(result.Positions) == 0 {
		commonlog.Logger.Error("GetAccountPosition",
			zap.String("Account:", *user),
			zap.String("Asset:", *asset),
		)
		return nil
	}

	return &result.Positions
}

func (a *AccountDB) GetAccountAssetByPosition(chainId, user, pay, item *string) *account.Asset {
	asset := &account.Asset{}
	filter, projection := a.BsonForGetAccountPosition(chainId, user, pay, item)

	option := options.FindOne().SetProjection(projection)
	err := a.ColAssets.FindOne(
		context.Background(),
		filter,
		option,
	).Decode(&asset)
	if err != nil {
		commonlog.Logger.Error("GetAccountPosition",
			zap.String("Account:", *user),
			zap.String("Pay:", *pay),
			zap.String("Item:", *item),
		)
		return nil
	}

	return asset
}

func (a *AccountDB) UpdateAccountPosition(
	tradeType account.TradeType,
	position *account.Asset,
	margin, value *primitive.Decimal128,
) error {
	filter := bson.M{}
	update := bson.M{}

	asset := a.GetAccountAssetByPosition(
		&position.ChainId,
		&position.Account,
		&position.Address,
		&position.Position[0].Address,
	)
	init := asset.Position == nil
	a.CalculateAccountPosition(
		tradeType,
		&position.Position[0].Address,
		&position.Position[0].Size,
		&position.Leverage,
		margin,
		value,
		asset,
	)

	if init {
		filter, update = a.BsonForInitAccountPosition(asset)
	} else {
		filter, update = a.BsonForUpdateAccountPosition(asset)
	}

	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	err := a.ColAssets.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(asset)
	if err != nil {
		commonlog.Logger.Error("UpdateAccountPosition",
			zap.String("Failed to update", err.Error()),
		)
		return err
	}

	return nil
}
