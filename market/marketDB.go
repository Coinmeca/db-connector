package market

import (
	"context"
	"db-connector/conf"

	"github.com/coinmeca/go-common/commondatabase"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type MarketDB struct {
	config *conf.Config

	client     *mongo.Client
	colMarket  *mongo.Collection
	colChart   *mongo.Collection
	colHistory *mongo.Collection

	start chan struct{}
}

type MarketInfos struct {
	ChainId       string
	OrderbookInfo []OrderbookInfo
}

type OrderbookInfo struct {
	Address   string
	Orderbook market.OutputOrderbook
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &MarketDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["marketDB"]["username"].(string),
		Password: r.config.Repositories["marketDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["marketDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["marketDB"]["db"].(string))
		r.colMarket = db.Collection("market")
		r.colChart = db.Collection("chart")
		r.colHistory = db.Collection("history")
	} else {
		return nil, err
	}

	if err := marketIndex(r.colMarket); err != nil {
		return nil, err
	}

	if err := chartIndex(r.colChart); err != nil {
		return nil, err
	}

	if err := historyIndex(r.colHistory); err != nil {
		return nil, err
	}

	commonlog.Logger.Debug("load repository",
		zap.String("marketDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (e *MarketDB) Start() error {
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

func marketIndex(col *mongo.Collection) error {
	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "chainId", Value: 1},
			{Key: "address", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetSparse(true),
	}

	_, err := col.Indexes().CreateOne(context.Background(), index)
	return err
}

func chartIndex(col *mongo.Collection) error {
	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "chainId", Value: 1},
			{Key: "address", Value: 1},
			{Key: "interval", Value: 1},
		},
		Options: options.Index().SetUnique(false).SetSparse(true),
	}

	_, err := col.Indexes().CreateOne(context.Background(), index)
	return err
}

func historyIndex(col *mongo.Collection) error {
	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "user", Value: 1},
			{Key: "txHash", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetSparse(true),
	}

	_, err := col.Indexes().CreateOne(context.Background(), index)
	return err
}
