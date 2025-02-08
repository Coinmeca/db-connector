package accountdb

import (
	"context"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/account"
	"github.com/coinmeca/go-common/commonmethod/market"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.uber.org/zap"
)

type AccountDB struct {
	config *conf.Config

	client    *mongo.Client
	ColAssets *mongo.Collection

	start chan struct{}
}

type AccountDBInterface interface {
	// query
	BsonForGetAccountAssetByPosition(chainId *string, user *string, pay *string, item *string) (bson.M, bson.M)
	BsonForGetAccountAssets(chainId string, account string, assets *[]*string) *bson.M
	BsonForGetAccountPosition(chainId *string, user *string, pay *string, item *string) (bson.M, bson.M)
	BsonForGetAccountPositions(chainId *string, user *string, asset *string) (bson.M, bson.M)
	BsonForInitAccountPosition(asset *account.Asset) (bson.M, bson.M)
	BsonForUpdateAccountAsset(chainId string, account string, asset *account.Asset) (bson.M, bson.M)
	BsonForUpdateAccountAssetUse(tradeType account.TradeType, chainId *string, account *string, asset *string, amount *primitive.Decimal128, count int64) (bson.M, bson.A)
	BsonForUpdateAccountAssets(chainId string, account string, assets []*account.Asset) *[]mongo.WriteModel
	BsonForUpdateAccountOrder(tradeType account.TradeType, chainId *string, account *string, asset *string, amount *primitive.Decimal128, count int64) (bson.M, bson.A)
	BsonForUpdateAccountPosition(asset *account.Asset) (bson.M, bson.M)

	// calculate
	CalculateAccountAsset(tradeType bool, amount *primitive.Decimal128, value *primitive.Decimal128, asset *account.Asset) *account.Asset
	CalculateAccountPosition(tradeType account.TradeType, item *string, size *primitive.Decimal128, leverage *primitive.Decimal128, margin *primitive.Decimal128, value *primitive.Decimal128, asset *account.Asset)

	// getter
	GetAccountAsset(chainId *string, user *string, asset *string) *account.Asset
	GetAccountAssetByPosition(chainId *string, user *string, pay *string, item *string) *account.Asset
	GetAccountAssets(chainId *string, user *string, assets *[]*string) []*account.Asset
	GetAccountPosition(chainId *string, user *string, pay *string, item *string) *account.Position
	GetAccountPositions(chainId *string, user *string, asset *string) *[]account.Position

	// update
	UpdateAccountAsset(tradeType bool, chainId *string, user *string, address *string, amount *primitive.Decimal128, value *primitive.Decimal128) error
	UpdateAccountAssetUse(tradeType account.TradeType, chainId *string, user *string, asset *string, amount *primitive.Decimal128, count int64) error
	UpdateAccountAssets(chainId string, user string, assets *[]account.Trade) error
	UpdateAccountOrder(tradeType account.TradeType, chainId *string, user *string, asset *string, amount *primitive.Decimal128, count int64) error
	UpdateAccountPosition(tradeType account.TradeType, position *account.Asset, margin *primitive.Decimal128, value *primitive.Decimal128) error

	Transfer(chainId *string, event *market.EventTransferOrder) error
	TransferPosition(tradeType account.TradeType, chainId *string, event *market.EventTransferPosition, value *primitive.Decimal128) error

	Start() error
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &AccountDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["accountDB"]["username"].(string),
		Password: r.config.Repositories["accountDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["accountDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["accountDB"]["db"].(string))
		r.ColAssets = db.Collection("assets")
	} else {
		return nil, err
	}

	commonlog.Logger.Debug("load repository",
		zap.String("accountDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (e *AccountDB) Start() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		close(e.start)
		return
	}()
}
