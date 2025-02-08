package modeltreasury

import (
	"coinmeca-trader/conf"
	"context"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"go.uber.org/zap"
)

type TreasuryDB struct {
	config *conf.Config

	client      *mongo.Client
	colTreasury *mongo.Collection
	colChart    *mongo.Collection
	colToken    *mongo.Collection

	start chan struct{}
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &TreasuryDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["treasuryDB"]["username"].(string),
		Password: r.config.Repositories["treasuryDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["treasuryDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["treasuryDB"]["db"].(string))
		r.colTreasury = db.Collection("treasury")
		r.colToken = db.Collection("token")
		r.colChart = db.Collection("chart")
	} else {
		return nil, err
	}

	commonlog.Logger.Debug("load repository",
		zap.String("treasuryDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (t *TreasuryDB) Start() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		close(t.start)
		return
	}()
}
