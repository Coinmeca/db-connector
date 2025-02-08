package marketdb

import (
	"context"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonprotocol"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type MarketDB struct {
	config *conf.Config

	client     *mongo.Client
	ColMarket  *mongo.Collection
	ColChart   *mongo.Collection
	ColHistory *mongo.Collection

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

type MarketDBInterface interface {
	// query
	BsonForChart(k *market.Chart, interval *int64) (bson.M, bson.M)
	BsonForChartByIntervals(k *market.Chart) *[]bson.M
	BsonForInfo(info *market.Market) (bson.M, bson.M)
	BsonForMarketChart(chart *market.Chart, interval *int64) (bson.M, bson.M)
	BsonForMarketChartVolume(chart *market.Chart, interval *int64) (bson.M, bson.M)
	BsonForMarketChartVolumesByIntervals(chart *market.Chart) *[]bson.M
	BsonForMarketLiquidity(chainId *string, address *string, liquidity *[]*market.MarketLiquidity) (bson.M, bson.A)
	BsonForMarketRecent(recent *market.Recent) (bson.M, bson.M)
	BulkWriteInfo(models []mongo.WriteModel) error

	// getter
	GetAllMarketAddresses() ([]*commonprotocol.Contract, error)
	GetAllMarkets() ([]*market.Market, error)
	GetChart(chainId *string, address *string, interval *int64) ([]*market.Chart, error)
	GetChartLast(chainId *string, address *string, interval *int64) *market.Chart
	GetHighAndLow24h(chainId *string, address *string) (*primitive.Decimal128, *primitive.Decimal128, error)
	GetLastAll(time *int64, last *market.Last)
	GetMarket(chainId *string, address *string) (*market.Market, error)
	GetMarketRoute(chainId *string, base *string, quote *string) (*market.Market, error)
	GetMarketTicker(chainId *string, address *string) map[string]string
	GetMarkets(chainId *string) ([]*market.Market, error)
	GetPrice24hAgo(chainId *string, address *string) (*primitive.Decimal128, error)
	GetVolume24h(chainId *string, address *string) (*primitive.Decimal128, *primitive.Decimal128, error)

	// setter
	SaveChart(chart *market.Chart, interval int64) error
	SaveChartByIntervals(chart *market.Chart) error
	SaveChartVolume(chart *market.Chart, interval int64) error
	SaveChartVolumesByIntervals(chart *market.Chart) error
	SaveMarketInfo(info *market.Market) error
	SaveMarketInfoFromModel(models *[]mongo.WriteModel, info *market.Market)
	SaveMarketLiquidity(chainId *string, address *string, liquidity *[]*market.MarketLiquidity) error
	SaveMarketRecent(recent *market.Recent) error
	SaveOrderbook(o market.OutputOrderbookResult) error

	Start() error
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
		r.ColMarket = db.Collection("market")
		r.ColChart = db.Collection("chart")
		r.ColHistory = db.Collection("history")
	} else {
		return nil, err
	}

	if err := marketIndex(r.ColMarket); err != nil {
		return nil, err
	}

	if err := chartIndex(r.ColChart); err != nil {
		return nil, err
	}

	if err := historyIndex(r.ColHistory); err != nil {
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
