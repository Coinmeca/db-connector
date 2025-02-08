package modelvault

import (
	"coinmeca-trader/conf"
	"context"

	"github.com/coinmeca/go-common/commondatabase"

	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type VaultDB struct {
	config *conf.Config

	client      *mongo.Client
	colVault    *mongo.Collection
	colChart    *mongo.Collection
	colChartSub *mongo.Collection
	colHistory  *mongo.Collection

	start chan struct{}
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &VaultDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["vaultDB"]["username"].(string),
		Password: r.config.Repositories["vaultDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["vaultDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["vaultDB"]["db"].(string))
		r.colVault = db.Collection("vault")
		r.colChart = db.Collection("chart")
		r.colChartSub = db.Collection("chart_sub")
		r.colHistory = db.Collection("history")
	} else {
		return nil, err
	}

	if err := historyIndex(r.colHistory); err != nil {
		return nil, err
	}

	commonlog.Logger.Debug("load repository",
		zap.String("vaultDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (v *VaultDB) Start() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		close(v.start)
		return
	}()
}
