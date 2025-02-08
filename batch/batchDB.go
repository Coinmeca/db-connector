package batch

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

type BatchDB struct {
	config *conf.Config

	client *mongo.Client
	colJob *mongo.Collection

	start chan struct{}
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &BatchDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["batchDB"]["username"].(string),
		Password: r.config.Repositories["batchDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["batchDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["batchDB"]["db"].(string))
		r.colJob = db.Collection("job")
	} else {
		return nil, err
	}

	commonlog.Logger.Debug("load repository",
		zap.String("batchDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (b *BatchDB) Start() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		close(b.start)
		return
	}()
}

func (b *BatchDB) GetBatchTableInfos() ([]commondatabase.BatchInfo, error) {
	var batchTableInfos []commondatabase.BatchInfo
	cursor, err := b.colJob.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var batchInfo commondatabase.BatchInfo
		if err := cursor.Decode(&batchInfo); err != nil {
			return nil, err
		}
		batchTableInfos = append(batchTableInfos, batchInfo)
	}

	return batchTableInfos, nil
}
