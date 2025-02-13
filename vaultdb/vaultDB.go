package vaultdb

import (
	"context"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonmethod/vault"

	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type VaultDB struct {
	config *conf.Config

	client      *mongo.Client
	ColVault    *mongo.Collection
	ColChart    *mongo.Collection
	ColChartSub *mongo.Collection
	ColHistory  *mongo.Collection

	start chan struct{}
}

type VaultDBInterface interface {
	// query
	BsonForChart(chart *vault.Chart, interval *int64) (bson.M, bson.M)
	BsonForChartByIntervals(chart *vault.Chart) *[]bson.M
	BsonForChartPrice(chart *vault.Chart, interval *int64) (bson.M, bson.M)
	BsonForChartSub(chart *vault.ChartSub) (bson.M, bson.M)
	BsonForChartSubAtTime(time *int64, chainId string, address string) mongo.Pipeline
	BsonForChartSubsAtTime(time *int64, chainId string, addresses []string) mongo.Pipeline
	BsonForInfo(info *vault.Vault) (bson.M, bson.M)
	BsonForValue(chainId *string, address *string, value *primitive.Decimal128) (bson.M, bson.M)
	BsonForValueAtTime(time *int64, chainId string, address string) mongo.Pipeline
	BsonForValuesAtTime(time *int64, chainId string, addresses []string) mongo.Pipeline
	BsonForVaultRecent(recent *vault.Recent) (bson.M, bson.M)
	BsonForVaultWeight(recent *vault.Recent) (bson.M, bson.M)
	BulkWriteChart(models []mongo.WriteModel) error
	BulkWriteChartSub(models []mongo.WriteModel) error
	BulkWriteInfo(models []mongo.WriteModel) error

	// getter
	GetAllKeyTokenSymbols() ([]*vault.Vault, error)
	GetAllVaults() ([]*vault.Vault, error)
	GetKeyTokens(chainId *string) ([]*vault.Vault, error)
	GetKeyTokenSymbols(chainId *string) ([]*vault.Vault, error)
	GetNonKeyTokenSymbols(chainId *string) ([]*vault.Vault, error)
	GetChart(chainId *string, address *string, interval *int64) []*vault.Chart
	GetChartLast(chainId *string, address *string, interval *int64) *vault.Chart
	GetChartSub(chainId *string, address *string) ([]*vault.ChartSub, error)
	GetChartSubAtTime(chainId *string, address *string, time *int64) (chartSub *vault.ChartSub)
	GetChartSubLast(chainId *string, address *string) (chartSub *vault.ChartSub)
	GetChartSubsAtTime(time *int64, chainId *string, addresses []string) map[string]*vault.ChartSub
	GetValue(chainId *string, address *string) *primitive.Decimal128
	GetValueAtTime(time *int64, chainId *string, address *string) *primitive.Decimal128
	GetValuesAtTime(time *int64, chainId *string, addresses []string) map[string]*primitive.Decimal128
	GetVault(chainId *string, address *string) (*vault.Vault, error)
	GetVaults(chainId *string) ([]*vault.Vault, error)
	GetDeposit24h(chainId *string, address *string) (*primitive.Decimal128, error)
	GetWithdraw24h(chainId *string, address *string) (*primitive.Decimal128, error)
	GetMint24h(chainId *string, address *string) (*primitive.Decimal128, error)
	GetBurn24h(chainId *string, address *string) (*primitive.Decimal128, error)
	GetRate24hAgo(chainId *string, address *string) (*primitive.Decimal128, error)
	GetWeight24hAgo(chainId *string, address *string) (*primitive.Decimal128, error)
	GetLocked24hAgo(chainId *string, address *string) (*primitive.Decimal128, error)
	GetValueLocked24hAgo(chainId *string, address *string) (*primitive.Decimal128, error)
	GetLastAll(nowTime *int64, last *vault.Last) error

	// setter
	SaveChart(t *vault.Chart, interval int64) error
	SaveChartByIntervals(t *vault.Chart) error
	SaveChartFromModel(models *[]mongo.WriteModel, exchange *primitive.Decimal128, t *vault.Chart, interval int64)
	SaveChartSub(t *vault.ChartSub) error
	SaveChartSubFromModel(models *[]mongo.WriteModel, t *vault.ChartSub)
	SaveChartVolume(chart *vault.Chart, interval int64) error
	SaveChartVolumesByIntervals(chart *vault.Chart) error
	SaveValue(chainId *string, address *string, value *primitive.Decimal128) error
	SaveVaultInfo(info *vault.Vault) error
	SaveVaultInfoFromModel(models *[]mongo.WriteModel, info *vault.Vault)
	SaveVaultInfoWithWeight(recent *vault.Recent, info *vault.Vault) error
	SaveVaultRecent(recent *vault.Recent) error

	UpdateVaultDepositAmount(chainId string, address string, amount primitive.Decimal128) error
	UpdateVaultWithdrawAmount(chainId string, address string, amount primitive.Decimal128) error

	Start() error
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
		r.ColVault = db.Collection("vault")
		r.ColChart = db.Collection("chart")
		r.ColChartSub = db.Collection("chart_sub")
		r.ColHistory = db.Collection("history")
	} else {
		return nil, err
	}

	if err := vaultIndex(r.ColVault); err != nil {
		return nil, err
	}

	if err := chartIndex(r.ColChart); err != nil {
		return nil, err
	}

	if err := chartSubIndex(r.ColChartSub); err != nil {
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

func vaultIndex(col *mongo.Collection) error {
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

func chartSubIndex(col *mongo.Collection) error {
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

// func (v *VaultDB) SaveVaultMintAndBurnBackup(recent *vault.Recent, vault *vault.Vault) error {
// 	filter, update := v.BsonForVaultMintAndBurn(recent)
// 	option := options.FindOneAndUpdate().SetUpsert(true)
// 	err := v.ColHistory.FindOneAndUpdate(
// 		context.Background(),
// 		filter,
// 		update,
// 		option,
// 	).Decode(vault)
// 	if err != nil {
// 		commonlog.Logger.Error("SaveVaultMintAndBurn",
// 			zap.String("BsonForVaultMintAndBurn", err.Error()),
// 		)
// 		return err
// 	}
// 	return nil
// }
