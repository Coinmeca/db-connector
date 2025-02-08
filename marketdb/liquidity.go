package marketdb

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (m *MarketDB) SaveMarketLiquidity(chainId, address *string, liquidity *[]*market.MarketLiquidity) error {
	filter, update := m.BsonForMarketLiquidity(chainId, address, liquidity)
	option := options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true)

	market := &market.Market{}
	err := m.ColMarket.FindOneAndUpdate(
		context.Background(),
		filter,
		update,
		option,
	).Decode(market)
	if err != nil {
		commonlog.Logger.Debug("MarketDB",
			zap.String("SaveMarketLiquidity", err.Error()),
		)
		return err
	}

	return nil
}
