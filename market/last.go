package modelmarket

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

func (m *MarketDB) GetLastAll(time *int64, last *market.Last) {
	ago := *time - 86400

	// 24h volume, high, low
	pipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"chainId": last.ChainId,
			"address": last.Address,
			"time":    bson.D{{"$gte", ago}},
		}}},
		{{"$group", bson.D{
			{"_id", nil},
			{"high", bson.D{{"$max", "$high"}}},
			{"low", bson.D{{"$min", "$low"}}},
			{"base", bson.D{{"$sum", "$volume.base"}}},
			{"quote", bson.D{{"$sum", "$volume.quote"}}},
		}}},
	}

	cursor, err := m.colChart.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("GetLastAll",
			zap.String("Load Error", err.Error()),
		)
	}
	defer cursor.Close(context.Background())

	var _24h struct {
		BaseVolume  primitive.Decimal128 `bson:"base"`
		QuoteVolume primitive.Decimal128 `bson:"quote"`
		High        primitive.Decimal128 `bson:"high"`
		Low         primitive.Decimal128 `bson:"low"`
		Price       primitive.Decimal128 `bson:"price"`
		// test
		Time primitive.Decimal128 `bson:"time"`
	}

	if cursor.Next(context.Background()) {
		if err := cursor.Decode(&_24h); err != nil {
			commonlog.Logger.Error("GetLastAll",
				zap.String("No Result decode data : ", ""),
			)
		}
		last.Volume.Base = _24h.BaseVolume
		last.Volume.Quote = _24h.QuoteVolume
		last.High = _24h.High
		last.Low = _24h.Low
	}

	// 24h price ago
	pipeline = mongo.Pipeline{
		{{"$match", bson.M{
			"chainId": last.ChainId,
			"address": last.Address,
			"time":    bson.M{"$gte": ago},
		}}},
		{{"$project", bson.M{
			"price": "$open",
			// NOTE: 아래 필드 추가되어야 반영됨.. 이유는 왜?
			"time": "$time",
		}}},
		// 소팅 해야하는데 필드가 없어서?
		{{"$sort", bson.M{"time": 1}}},
		{{"$limit", 1}},
	}

	cursor, err = m.colChart.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("GetLastAll",
			zap.String("Load Error 24hago ", err.Error()),
		)
	}
	defer cursor.Close(context.Background())

	if cursor.Next(context.Background()) {
		if err := cursor.Decode(&_24h); err != nil {
			commonlog.Logger.Error("GetLastAll",
				zap.String("No Result 24h ago", err.Error()),
			)
		}
		last.Price = _24h.Price
	}
}

func (e *MarketDB) GetVolume24h(chainId, address *string) (*primitive.Decimal128, *primitive.Decimal128, error) {
	ago := time.Now().UTC().Unix() - 86400

	pipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"time":    bson.D{{"$gte", ago}},
			"chainId": chainId,
			"address": strings.ToLower(*address),
		}}},
		{{"$group", bson.D{
			{"_id", nil},
			{"base", bson.D{{"$sum", "$amount"}}},
			{"quote", bson.D{{"$sum", "$quantity"}}},
		}}},
		{{"$sort", bson.M{"time": 1}}},
	}
	cursor, err := e.colHistory.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("GetVolume24h",
			zap.String("Load Error", err.Error()),
		)
		return nil, nil, err
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		commonlog.Logger.Error("GetVolume24h",
			zap.String("Cannot Find Result", err.Error()),
		)
		return nil, nil, err
	}
	if len(results) == 0 {
		commonlog.Logger.Error("GetVolume24h",
			zap.String("No Result", err.Error()),
		)
		return nil, nil, err
	}

	base := results[0]["base"].(primitive.Decimal128)
	quote := results[0]["quote"].(primitive.Decimal128)
	return &base, &quote, nil
}

func (e *MarketDB) GetHighAndLow24h(chainId, address *string) (*primitive.Decimal128, *primitive.Decimal128, error) {
	ago := time.Now().UTC().Unix() - 86400

	pipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"time":    bson.D{{"$gte", ago}},
			"chainId": chainId,
			"address": strings.ToLower(*address),
		}}},
		{{"$group", bson.D{
			{"_id", nil},
			{"high", bson.D{{"$max", "$high"}}},
			{"low", bson.D{{"$min", "$low"}}},
		}}},
	}
	cursor, err := e.colChart.Aggregate(context.Background(), pipeline)
	if err != nil {
		commonlog.Logger.Error("GetHighAndLow24h",
			zap.String("Load Error", err.Error()),
		)
		return nil, nil, err
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		commonlog.Logger.Error("GetHighAndLow24h",
			zap.String("Cannot Find Result", err.Error()),
		)
		return nil, nil, err
	}
	if len(results) == 0 {
		commonlog.Logger.Error("GetHighAndLow24h",
			zap.String("No Result", err.Error()),
		)
		return nil, nil, err
	}

	base := results[0]["high"].(primitive.Decimal128)
	quote := results[0]["low"].(primitive.Decimal128)
	return &base, &quote, nil
}

func (e *MarketDB) GetPrice24hAgo(chainId, address *string) (*primitive.Decimal128, error) {
	ago := time.Now().UTC().Unix() - 86400

	pipeline := mongo.Pipeline{
		{{"$match", bson.M{
			"chainId": chainId,
			"time":    bson.M{"$gte": ago},
			"address": strings.ToLower(*address),
		}}},
		{{"$project", bson.M{
			"price": "$open",
			"time":  "$time",
		}}},
		{{"$sort", bson.M{"time": 1}}},
		{{"$limit", 1}},
	}

	cursor, err := e.colChart.Aggregate(context.Background(), pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		log.Fatal(err)
		return nil, err
	}
	if len(results) == 0 {
		return nil, err
	}

	lastPrice := results[0]["price"].(primitive.Decimal128)
	return &lastPrice, nil
}
