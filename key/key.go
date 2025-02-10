package key

import (
	"context"
	"errors"
	"time"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type KeyManager struct {
	config *conf.Config

	client     *mongo.Client
	db         *mongo.Database
	Collection map[string]*mongo.Collection

	start   chan struct{}
	Current map[string]*commondatabase.APIKey
}

// NewKeyManager returns KeyManager as IRepository
func NewKeyManager(config *conf.Config) (*KeyManager, error) {
	r := &KeyManager{
		config: config,
		start:  make(chan struct{}),
	}

	var err error
	credential := options.Credential{
		Username: r.config.Repositories["keyDB"]["username"].(string),
		Password: r.config.Repositories["keyDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["keyDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	if err = r.client.Ping(context.Background(), nil); err == nil {
		r.db = r.client.Database(config.Repositories["keyDB"]["db"].(string))
	} else {
		return nil, err
	}

	r.Collection = make(map[string]*mongo.Collection)
	r.Current = make(map[string]*commondatabase.APIKey)

	commonlog.Logger.Debug("load repository",
		zap.String("keyDB", r.config.Common.ServiceId),
	)

	return r, nil
}

func (h *KeyManager) Start() error {
	return func() (err error) {
		defer func() {
			if k := recover(); k != nil {
				err = k.(error)
				commonlog.Logger.Error("Panic recovered in KeyManager Start", zap.Error(err))
			}
		}()
		close(h.start)
		return nil
	}()
}

func (k *KeyManager) InitKey(cate, chainId string) (*commondatabase.APIKey, error) {
	key, err := k.GetActiveKey(cate, chainId)
	if err != nil {
		key, err = k.GetNewKey(cate, chainId)
		if err != nil {
			return nil, err
		}
	}
	err = k.SetKey(cate, chainId, key.Key)
	if err != nil {
		commonlog.Logger.Error("active key doesn't set:",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
			zap.String("key", key.Key),
		)
		return nil, err
	}
	return key, nil
}

func (k *KeyManager) ExpiredKey(cate, chainId, key string) error {
	col := k.col(cate, chainId)
	if col == nil {
		commonlog.Logger.Error("ExpiredKey: not found collection",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
		)
		return errors.New("not found collection")
	}

	filter := bson.M{"chainId": chainId, "key": key}
	update := bson.M{
		"$set": bson.M{
			"expired": time.Now().Unix(),
			"active":  false,
		},
	}

	options := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	result := col.FindOneAndUpdate(context.Background(), filter, update, options)
	if err := result.Err(); err != nil {
		commonlog.Logger.Error("ExpiredKey",
			zap.String("update failed", err.Error()),
		)
		return err
	}

	return nil
}

func (k *KeyManager) SetKey(cate, chainId, key string) error {
	col := k.col(cate, chainId)
	if col == nil {
		commonlog.Logger.Error("SetKey: not found collection",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
		)
		return errors.New("not found collection")
	}

	current := &commondatabase.APIKey{}
	filter := bson.M{"chainId": chainId, "key": key}
	update := bson.M{
		"$set": bson.M{
			"start":  time.Now().Unix(),
			"active": true,
		},
	}

	options := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	err := col.FindOneAndUpdate(context.Background(), filter, update, options).Decode(current)
	if err != nil {
		commonlog.Logger.Error("SetKey",
			zap.String("update failed", key),
		)
		return err
	}

	commonlog.Logger.Debug("SetKey",
		zap.String("cate:", cate),
		zap.String("key:", current.Key),
	)

	k.Current[cate] = current

	return nil
}

func (k *KeyManager) GetActiveKey(cate, chainId string) (*commondatabase.APIKey, error) {
	col := k.col(cate, chainId)
	if col == nil {
		commonlog.Logger.Error("GetActiveKey: not found collection",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
		)
		return nil, errors.New("not found collection")
	}

	filter := bson.M{
		"chainId": chainId,
		"active":  true,
	}

	key := &commondatabase.APIKey{}
	options := options.FindOne().SetSort(bson.M{"expired": 1})
	err := col.FindOne(context.Background(), filter, options).Decode(key)
	if err != nil {
		commonlog.Logger.Error("GetActiveKey",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
			zap.String("not found", err.Error()),
		)
		return nil, err
	}

	return key, nil
}

func (k *KeyManager) GetNewKey(cate, chainId string) (*commondatabase.APIKey, error) {
	col := k.col(cate, chainId)
	if col == nil {
		commonlog.Logger.Error("GetNewKey: not found collection",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
		)
		return nil, errors.New("not found collection")
	}

	filter := bson.M{
		"chainId": chainId,
		"$or": []bson.M{
			{
				"$expr": bson.M{
					"$lte": bson.A{
						bson.M{"$add": bson.A{"$expired", bson.M{"$multiply": bson.A{"$retry", 86400}}}},
						time.Now().Unix(),
					},
				},
			},
			{
				"expired": nil,
			},
		},
	}

	key := &commondatabase.APIKey{}
	options := options.FindOne().SetSort(bson.M{"expired": 1})
	err := col.FindOne(context.Background(), filter, options).Decode(key)
	if err != nil {
		commonlog.Logger.Error("GetNewKey",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
			zap.String("not found", err.Error()),
		)
		return nil, err
	}

	return key, nil
}

func (k *KeyManager) GetNewKeys(cate, chainId string) ([]*commondatabase.APIKey, error) {
	col := k.col(cate, chainId)
	if col == nil {
		commonlog.Logger.Error("GetNewKeys: not found collection",
			zap.String("cate", cate),
			zap.String("chainId", chainId),
		)
		return nil, errors.New("not found collection")
	}

	var keys []*commondatabase.APIKey
	ctx := context.Background()
	filter := bson.M{
		"chainId": chainId,
		"$or": []bson.M{
			{
				"$expr": bson.M{
					"$lte": bson.A{
						bson.M{"$add": bson.A{"$expired", bson.M{"$multiply": bson.A{"$retry", 86400}}}},
						time.Now().Unix(),
					},
				},
			},
			{
				"expired": nil,
			},
		},
	}
	options := options.Find().SetSort(bson.M{"expired": 1})
	cursor, err := col.Find(ctx, filter, options)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		key := &commondatabase.APIKey{}
		if err := cursor.Decode(&key); err != nil {
			return nil, err
		}

		keys = append(keys, key)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return keys, nil
}

func (k *KeyManager) GetCurrentKey(cate, chainId string) *commondatabase.APIKey {
	current, ok := k.Current[cate]
	if !ok {
		init, err := k.InitKey(cate, chainId)
		if err != nil {
			return nil
		}
		current = init
	}

	return current
}

func (k *KeyManager) col(cate, chainId string) *mongo.Collection {
	if cate == "cmc" {
		cate = "cmc"
	} else if cate == "alchemy" {
		cate = "alchemy." + chainId
	} else {
		commonlog.Logger.Error("KeyDB: not found collection",
			zap.String("cate:", cate),
			zap.String("chainId", chainId),
		)
		return nil
	}

	col, ok := k.Collection[cate]
	if !ok {
		k.Collection[cate] = k.db.Collection(cate)
		col = k.Collection[cate]
	}
	return col
}
