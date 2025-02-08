package vault

import (
	"context"
	"db-connector/conf"

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

	if err := vaultIndex(r.colVault); err != nil {
		return nil, err
	}

	if err := chartIndex(r.colChart); err != nil {
		return nil, err
	}

	if err := chartSubIndex(r.colChartSub); err != nil {
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
// 	err := v.colHistory.FindOneAndUpdate(
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
