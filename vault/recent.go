package modelvault

import (
	"context"
	"errors"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/vault"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (v *VaultDB) SaveVaultRecent(recent *vault.Recent) error {
	filter, update := v.BsonForVaultRecent(recent)
	option := options.Update().SetUpsert(true)

	result, err := v.colHistory.UpdateOne(
		context.Background(),
		filter,
		update,
		option,
	)
	if err != nil {
		commonlog.Logger.Error("SaveVaultRecent",
			zap.String("BsonForVaultRecent", err.Error()),
		)
		return err
	}
	if result.UpsertedCount < 1 {
		return errors.New("SaveVaultRecent Skipped")
	}
	return nil
}
