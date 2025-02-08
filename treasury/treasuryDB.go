package modeltreasury

import (
	"coinmeca-batch/conf"
	"context"

	"github.com/coinmeca/go-common/commonmethod/treasury"
	"go.mongodb.org/mongo-driver/bson"

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

	if err := tokenIndex(r.colToken); err != nil {
		return nil, err
	}

	if err := chartIndex(r.colChart); err != nil {
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

func tokenIndex(col *mongo.Collection) error {
	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "name", Value: 1},
			{Key: "symbol", Value: 1},
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

func (t *TreasuryDB) GetLatestChart(chainId string) (*treasury.Chart, error) {
	var latestChart treasury.Chart
	filter := bson.M{"chainId": chainId}
	opts := options.FindOne().SetSort(bson.D{{"time", -1}})

	err := t.colChart.FindOne(context.Background(), filter, opts).Decode(&latestChart)
	if err != nil {
		return nil, err
	}
	return &latestChart, nil
}
