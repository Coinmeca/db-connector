package marketdb

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/market"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (m *MarketDB) SaveMarketRecent(recent *market.Recent) error {
	filter, update := m.BsonForMarketRecent(recent)

	option := options.Update().SetUpsert(true)
	_, err := m.colHistory.UpdateOne(
		context.Background(),
		filter,
		update,
		option,
	)
	if err != nil {
		commonlog.Logger.Error("SaveMarketRecent",
			zap.String("BsonForVaultRecent", err.Error()),
		)
		return err
	}
	return nil
}
