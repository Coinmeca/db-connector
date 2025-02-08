package account

import (
	"context"
	"db-connector/conf"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.uber.org/zap"
)

type AccountDB struct {
	config *conf.Config

	client    *mongo.Client
	colAssets *mongo.Collection

	start chan struct{}
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
		r.colAssets = db.Collection("assets")
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
