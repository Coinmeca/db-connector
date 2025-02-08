package model

import (
	"coinmeca-crawler/conf"
	"context"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type HistoryDB struct {
	conf *conf.Config

	client       *mongo.Client
	colTxHistory *mongo.Collection

	start chan struct{}
}

func NewHistoryDB(config *conf.Config) (commondatabase.IRepository, error) {
	r := &HistoryDB{
		conf:  config,
		start: make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.conf.Repositories["historyDB"]["username"].(string),
		Password: r.conf.Repositories["historyDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["historyDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	// 수정된 부분: client.Ping 호출이 성공했을 때 컬렉션 초기화
	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["historyDB"]["db"].(string))
		r.colTxHistory = db.Collection("tx_history")
	} else {
		return nil, err
	}

	//commonlog.Trace("load repository :", "HistoryDB", r.conf.Common)
	commonlog.Logger.Debug("load repository",
		zap.String("historyDB", r.conf.Common.ServiceId),
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
