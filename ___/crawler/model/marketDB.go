package model

import (
	"coinmeca-crawler/conf"
	"context"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonprotocol"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.uber.org/zap"
)

type MarketDB struct {
	config *conf.Config

	client     *mongo.Client
	colMarket  *mongo.Collection
	colHistory *mongo.Collection
	colChart   *mongo.Collection

	start chan struct{}
}

func NewMarketDB(config *conf.Config) (commondatabase.IRepository, error) {
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

func (m *MarketDB) GetAllMarketAddresses() ([]*commonprotocol.Contract, error) {
	var markets []*commonprotocol.Contract
	option := options.Find().SetProjection(bson.M{
		"chainId": 1,
		"address": 1,
	})
	cursor, err := m.colMarket.Find(context.Background(), bson.M{}, option)

	if err != nil {
		return markets, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		mk := &commonprotocol.Contract{}
		if err := cursor.Decode(&mk); err == nil {
			mk.Cate = "abstract"
			mk.Name = "market"
			markets = append(markets, mk)
		}
	}

	return markets, nil
}
