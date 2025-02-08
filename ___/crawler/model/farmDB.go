package model

import (
	"coinmeca-crawler/conf"
	"context"
	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/farm"
	"github.com/coinmeca/go-common/commonprotocol"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"strings"
)

type FarmDB struct {
	config *conf.Config

	client     *mongo.Client
	colFarm    *mongo.Collection
	colHistory *mongo.Collection

	start chan struct{}
}

func NewFarmDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &FarmDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["farmDB"]["username"].(string),
		Password: r.config.Repositories["farmDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["farmDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["farmDB"]["db"].(string))
		r.colFarm = db.Collection("farm")
		r.colHistory = db.Collection("history")
	} else {
		return nil, err
	}
	commonlog.Logger.Debug("load repository",
		zap.String("farmDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (f *FarmDB) Start() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		close(f.start)
		return
	}()
}

func (f *FarmDB) GetAllFarmAddresses() ([]*commonprotocol.Contract, error) {
	var farms []*commonprotocol.Contract
	option := options.Find().SetProjection(bson.M{
		"chainId": 1,
		"address": 1,
		"main":    1,
		"name":    1,
	})
	cursor, err := f.colFarm.Find(context.Background(), bson.M{}, option)
	if err != nil {
		return farms, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		f := &farm.Farm{}
		if err := cursor.Decode(&f); err == nil {
			farm := &commonprotocol.Contract{
				Cate:    "abstract",
				ChainId: f.ChainId,
				Address: f.Address,
			}
			if strings.ToLower(f.Main) == strings.ToLower(f.Address) {
				farm.Name = "farm-main"
			} else {
				farm.Name = "farm-derive"
			}
			farms = append(farms, farm)
		}
	}

	return farms, nil
}
