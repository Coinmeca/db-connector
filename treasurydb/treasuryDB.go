package treasurydb

import (
	"context"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/go-common/commonmethod/treasury"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type TreasuryDB struct {
	config *conf.Config

	client      *mongo.Client
	ColTreasury *mongo.Collection
	ColChart    *mongo.Collection
	ColToken    *mongo.Collection

	start chan struct{}
}

type TreasuryDBInterface interface {
	// query
	BsonForChart(chart *treasury.Chart) (bson.M, bson.M)
	BsonForChartTV(chainId *string, time *int64, totalVolume *primitive.Decimal128) (bson.M, bson.M)
	BsonForChartTVL(chainId *string, time *int64, totalValueLocked *primitive.Decimal128) (bson.M, bson.M)
	BsonForChartTW(chainId *string, totalWeight *primitive.Decimal128) (bson.M, bson.M)
	BsonForSetValue(symbol string, name string, value *primitive.Decimal128) (bson.M, bson.M)
	BsonForTokens(token *bson.M) (bson.M, bson.M)
	BsonForTradingVolume(recent *treasury.Chart) (bson.M, bson.M)
	BulkWriteTokens(models *[]mongo.WriteModel) error

	// getter
	GetLatestChart(chainId string) (*treasury.Chart, error)
	GetLatestTVValue(chainId string) (primitive.Decimal128, error)
	GetTokens() ([]*bson.M, error)
	GetTreasuryChart(chainId *string) ([]*treasury.Chart, error)
	GetTreasuryChartLast(chainId *string) (*treasury.Last, error)
	GetValue(name *string, symbol *string) (*primitive.Decimal128, error)
	GetValues() (*map[string]primitive.Decimal128, error)

	// setter
	SaveTradingVolume(chart *treasury.Chart) error
	SaveTreasuryChart(chart *treasury.Chart) error
	SetValue(symbol string, name string, value *primitive.Decimal128) error

	// update
	UpdateTradingVolume(chainId string, volume *primitive.Decimal128) error
	UpsertDailyChart(chart *treasury.Chart) error

	Start() error
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
		r.ColTreasury = db.Collection("treasury")
		r.ColToken = db.Collection("token")
		r.ColChart = db.Collection("chart")
	} else {
		return nil, err
	}

	if err := tokenIndex(r.ColToken); err != nil {
		return nil, err
	}

	if err := chartIndex(r.ColChart); err != nil {
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

	err := t.ColChart.FindOne(context.Background(), filter, opts).Decode(&latestChart)
	if err != nil {
		return nil, err
	}
	return &latestChart, nil
}
