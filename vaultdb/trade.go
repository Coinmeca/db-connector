package vaultdb

import (
	"context"
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/vault"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

func (v *VaultDB) UpdateVaultDepositAmount(chainId, address string, amount primitive.Decimal128) error {
	filter := bson.M{
		"chainId": chainId,
		"address": strings.ToLower(address),
	}

	update := bson.M{
		"$inc": bson.M{
			"deposit": amount,
		},
	}

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updatedVault vault.Vault
	err := v.colVault.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&updatedVault)
	if err != nil {
		commonlog.Logger.Error("UpdateVaultDepositAmount",
			zap.String("FindOneAndUpdate", err.Error()),
		)
		return err
	}

	return nil
}

func (v *VaultDB) UpdateVaultWithdrawAmount(chainId, address string, amount primitive.Decimal128) error {
	filter := bson.M{
		"chainId": chainId,
		"address": strings.ToLower(address),
	}

	update := bson.M{
		"$inc": bson.M{
			"withdraw": amount,
		},
	}

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updatedVault vault.Vault
	err := v.colVault.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&updatedVault)
	if err != nil {
		commonlog.Logger.Error("UpdateVaultWithdrawAmount",
			zap.String("FindOneAndUpdate", err.Error()),
		)
		return err
	}

	return nil
}
