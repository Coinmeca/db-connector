package farmdb

import (
	"context"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonmethod/farm"
	"github.com/coinmeca/go-common/commonprotocol"

	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
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

type FarmDBInterface interface {
	// query
	BsonForChart(t *farm.Chart) (bson.M, bson.M)
	BsonForFarmRecent(recent *farm.Recent) (bson.M, bson.M)
	BsonForInfo(info *farm.Farm) (bson.M, bson.M)
	BulkWriteInfo(models []mongo.WriteModel) error

	// getter
	GetAllFarmAddresses() ([]*commonprotocol.Contract, error)
	GetChartAtTime(chainId *string, address *string, time *int64) *farm.Chart
	GetFarm(chainId string, address string) *farm.Farm
	GetFarms(chainId *string) ([]*farm.Farm, error)
	GetInterest24h(nowTime *int64, last *farm.Last)
	GetInterest24hBackup(nowTime *int64, last *farm.Last)
	GetStakedWithValue(nowTime *int64, last *farm.Last)
	GetStakedWithValueBackup(nowTime *int64, last *farm.Last)
	GetStakedWithValueBackup2(nowTime *int64, last *farm.Last)
	GetStaking24h(nowTime *int64, last *farm.Last)
	GetStaking24hBackup(nowTime *int64, last *farm.Last)
	GetTotalInterest(nowTime *int64, last *farm.Last)

	// setter
	SaveFarmChart(t *farm.Chart) error
	SaveFarmInfo(info *farm.Farm) error
	SaveFarmInfoFromModel(models *[]mongo.WriteModel, info *farm.Farm)
	SaveFarmRecent(recent *farm.Recent) error

	Start() error
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

	if err := farmIndex(r.colFarm); err != nil {
		return nil, err
	}

	if err := chartIndex(r.colChart); err != nil {
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

func farmIndex(col *mongo.Collection) error {
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
