package farmdb

import (
	"context"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/farm"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (f *FarmDB) SaveFarmRecent(recent *farm.Recent) error {
	filter, update := f.BsonForFarmRecent(recent)
	option := options.Update().SetUpsert(true)

	_, err := f.colHistory.UpdateOne(
		context.Background(),
		filter,
		update,
		option,
	)
	if err != nil {
		commonlog.Logger.Debug("FarmDB",
			zap.String("SaveFarmRecent", err.Error()),
		)
		return err
	}

	return nil
}
