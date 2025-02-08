package market

import (
	"context"
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (m *MarketDB) BulkWriteInfo(models []mongo.WriteModel) error {
	result, err := m.colMarket.BulkWrite(context.Background(), models)
	if err != nil {
		commonlog.Logger.Error("MarketDB",
			zap.String("BulkWriteInfo", err.Error()),
		)
		return err
	}
	fmt.Printf("Inserted: %v, Updated: %v, Deleted: %v\n", result.InsertedCount, result.ModifiedCount, result.DeletedCount)
	return nil
}

func (m *MarketDB) SaveMarketInfoFromModel(models *[]mongo.WriteModel, info *market.Market) {
	filter, update := m.BsonForInfo(info)
	*models = append(*models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
}

func (m *MarketDB) SaveMarketInfo(info *market.Market) error {
	filter, update := m.BsonForInfo(info)
	option := options.Update().SetUpsert(true)

	_, err := m.colMarket.UpdateOne(
		context.Background(),
		filter,
		update,
		option,
	)
	if err != nil {
		fmt.Println("error", err)
		return err
	}
	return nil
}

func (e *MarketDB) SaveOrderbook(o market.OutputOrderbookResult) error {
	asks := []market.Tick{}
	bids := []market.Tick{}

	orderbook := o.Orderbook
	zero := primitive.NewDecimal128(0, 0)

	for _, ask := range orderbook.Asks {
		price, err := commonutils.Decimal128FromBigInt(ask.Price)
		if err != nil {
			price = &zero
		}
		balance, err := commonutils.Decimal128FromBigInt(ask.Balance)
		if err != nil {
			balance = &zero
		}
		asks = append(asks, market.Tick{Price: *price, Balance: *balance})
	}
	for _, bid := range orderbook.Bids {
		price, err := commonutils.Decimal128FromBigInt(bid.Price)
		if err != nil {
			price = &zero
		}
		balance, err := commonutils.Decimal128FromBigInt(bid.Balance)
		if err != nil {
			balance = &zero
		}
		bids = append(bids, market.Tick{Price: *price, Balance: *balance})
	}

	filter := bson.M{"address": o.Address}
	update := bson.M{
		"$set": bson.M{
			"orderbook.asks": asks,
			"orderbook.bids": bids,
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := e.colMarket.UpdateOne(context.Background(), filter, update, opts)
	if err != nil {
		return err
	}
	return nil
}

func (m *MarketDB) GetMarket(chainId, address *string) (*market.Market, error) {
	mk := &market.Market{}
	if err := m.colMarket.FindOne(context.Background(), bson.M{"chainId": chainId, "address": strings.ToLower(*address)}, nil).Decode(mk); err != nil {
		commonlog.Logger.Debug("MarketDB",
			zap.String("GetMarket", err.Error()),
		)
		return nil, err
	} else {
		return mk, nil
	}
}

func (m *MarketDB) GetMarketRoute(chainId *string, base *string, quote *string) (*market.Market, error) {
	mk := &market.Market{}
	if err := m.colMarket.FindOne(context.Background(), bson.M{"chainId": chainId, "base": strings.ToLower(*base), "quote": strings.ToLower(*quote)}, nil).Decode(mk); err != nil {
		return nil, err
	} else {
		return mk, nil
	}
}

func (m *MarketDB) GetMarkets(chainId *string) ([]*market.Market, error) {
	var markets []*market.Market
	cursor, err := m.colMarket.Find(context.Background(), bson.M{"chainId": chainId})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		mk := &market.Market{}
		if err := cursor.Decode(mk); err != nil {
			return nil, err
		}
		markets = append(markets, mk)
	}
	return markets, nil
}

func (m *MarketDB) GetAllMarkets() ([]*market.Market, error) {
	var markets []*market.Market
	cursor, err := m.colMarket.Find(context.Background(), bson.M{})

	if err != nil {
		return markets, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		mk := &market.Market{}
		if err := cursor.Decode(mk); err == nil {
			markets = append(markets, mk)
		}
	}

	return markets, nil
}
