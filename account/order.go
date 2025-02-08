package account

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/account"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (a *AccountDB) UpdateAccountOrder(
	tradeType account.TradeType,
	chainId, user, asset *string,
	amount *primitive.Decimal128,
	count int64,
) error {
	filter, update := a.BsonForUpdateAccountOrder(tradeType, chainId, user, asset, amount, count)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := a.colAssets.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(asset)
	if err != nil {
		commonlog.Logger.Error("UpdateAccountOrder",
			zap.String(*asset, err.Error()),
		)
		return err
	}

	return nil
}

func (a *AccountDB) UpdateAccountAssetUse(
	tradeType account.TradeType,
	chainId, user, asset *string,
	amount *primitive.Decimal128,
	count int64,
) error {
	filter, update := a.BsonForUpdateAccountAssetUse(tradeType, chainId, user, asset, amount, count)
	option := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	err := a.colAssets.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(asset)
	if err != nil {
		commonlog.Logger.Error("UpdateAccountUse",
			zap.String(*asset, err.Error()),
		)
		return err
	}

	return nil
}
