package modelfarm

import (
	"coinmeca-trader/conf"
	"context"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type FarmDB struct {
	config *conf.Config

	client     *mongo.Client
	colFarm    *mongo.Collection
	colChart   *mongo.Collection
	colHistory *mongo.Collection

	start chan struct{}
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
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
		r.colChart = db.Collection("chart")
		r.colHistory = db.Collection("history")
	} else {
		return nil, err
	}

	if err := historyIndex(r.colHistory); err != nil {
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
