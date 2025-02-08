package marketdb

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonprotocol"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (m *MarketDB) GetMarketTicker(chainId, address *string) map[string]string {
	filter := bson.M{
		"chainId": *chainId,
		"address": bson.M{"$regex": *address, "$options": "i"},
	}

	projection := bson.M{
		"base.address":  1,
		"quote.address": 1,
	}

	token := bson.M{}
	result := make(map[string]string)

	err := m.colMarket.FindOne(context.Background(), filter, options.FindOne().SetProjection(projection)).Decode(&token)
	if err != nil {
		commonlog.Logger.Debug("MarketDB",
			zap.String("GetMarketTicker", err.Error()),
		)
		return nil
	}

	result["base"] = token["base.address"].(string)
	result["quote"] = token["quote.address"].(string)

	return result
}

func (m *MarketDB) GetAllMarketAddresses() ([]*commonprotocol.Contract, error) {
	var markets []*commonprotocol.Contract
	option := options.Find().SetProjection(bson.M{
		"chainId": 1,
		"address": 1,
	})
	cursor, err := m.colMarket.Find(context.Background(), bson.M{}, option)

	if err != nil {
		return markets, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		mk := &commonprotocol.Contract{}
		if err := cursor.Decode(&mk); err == nil {
			mk.Cate = "abstract"
			mk.Name = "market"
			markets = append(markets, mk)
		}
	}

	return markets, nil
}
