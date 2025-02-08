package historydb

import (
	"context"
	"db-connector/conf"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type HistoryDB struct {
	config *conf.Config

	client       *mongo.Client
	colTxHistory *mongo.Collection

	start           chan struct{}
	lastProcessedId primitive.ObjectID
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &HistoryDB{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["historyDB"]["username"].(string),
		Password: r.config.Repositories["historyDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["historyDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["historyDB"]["db"].(string))
		r.colTxHistory = db.Collection("tx_history")
	} else {
		return nil, err
	}

	if err := txIndex(r.colTxHistory); err != nil {
		return nil, err
	}

	commonlog.Logger.Debug("load repository",
		zap.String("historyDB", r.config.Common.ServiceId),
	)
	return r, nil
}

func (h *HistoryDB) Start() error {
	return func() (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = v.(error)
			}
		}()
		close(h.start)
		return
	}()
}

func txIndex(col *mongo.Collection) error {
	index := mongo.IndexModel{
		Keys: bson.D{
			{Key: "hash", Value: 1},
			{Key: "blockHash", Value: 1},
		},
		Options: options.Index().SetUnique(true).SetSparse(true),
	}

	_, err := col.Indexes().CreateOne(context.Background(), index)
	return err
}

func (h *HistoryDB) SaveTransactionRecord(txDetail *commondatabase.TxData) error {
	filter := bson.M{"hash": txDetail.Hash}
	update := bson.M{
		"$set": bson.M{
			"blockHash":   txDetail.BlockHash,
			"blockNumber": txDetail.BlockNumber,
			"hash":        txDetail.Hash,
			"chainId":     txDetail.ChainId,
			"accessList":  txDetail.AccessList,
			"from":        txDetail.From,
			"gas":         txDetail.Gas,
			"gasPrice":    txDetail.GasPrice,
			"input":       txDetail.Input,
			"to":          txDetail.To,
			"cate":        txDetail.Cate,
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := h.colTxHistory.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		return err
	}
	return nil
}

func (h *HistoryDB) PollingTxs(ctx context.Context, notificationChan chan<- *commondatabase.GrpcTxData, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			query := bson.M{"_id": bson.M{"$gt": h.lastProcessedId}}
			opts := options.Find().SetSort(bson.M{"_id": 1})
			cursor, err := h.colTxHistory.Find(ctx, query, opts)
			if err != nil {
				commonlog.Logger.Error("Polling Error", zap.Error(err))
				continue
			}

			for cursor.Next(ctx) {
				var txData commondatabase.GrpcTxData
				if err := cursor.Decode(&txData); err != nil {
					commonlog.Logger.Error("Decode Error", zap.Error(err))
					continue
				}

				// Convert string ID back to ObjectID
				newLastProcessedId, err := primitive.ObjectIDFromHex(txData.Id)
				if err != nil {
					commonlog.Logger.Error("Error converting string ID to ObjectID", zap.Error(err))
					continue
				}

				notificationChan <- &txData
				h.lastProcessedId = newLastProcessedId
			}

			cursor.Close(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *HistoryDB) PollingTxsBackup(callback func(manage *commondatabase.TxData), interval time.Duration) {
	var lastProcessedId interface{} = nil

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			query := bson.M{}
			if lastProcessedId != nil {
				query["_id"] = bson.M{"$gt": lastProcessedId}
			}

			opts := options.Find().SetSort(bson.M{"_id": 1})
			cursor, err := h.colTxHistory.Find(context.Background(), query, opts)
			if err != nil {
				commonlog.Logger.Error("PollingTxsBackup",
					zap.String("PollingTxsBackup", err.Error()),
				)
				continue
			}

			for cursor.Next(context.Background()) {
				txData := commondatabase.TxData{}
				if err := cursor.Decode(&txData); err != nil {
					commonlog.Logger.Error("PollingTxsBackup",
						zap.String("PollingTxsBackup", err.Error()),
					)
					continue
				}

				callback(&txData)
				lastProcessedId = txData.Id
			}

			cursor.Close(context.Background())
		}
	}
}
