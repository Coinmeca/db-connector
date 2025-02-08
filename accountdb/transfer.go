package accountdb

import (
	"strings"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonmethod/account"
	"github.com/coinmeca/go-common/commonmethod/market"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
)

func (a *AccountDB) Transfer(chainId *string, event *market.EventTransferOrder) error {
	var zero primitive.Decimal128
	from := event.From.String()
	to := event.To.String()
	sell := strings.ToLower(event.Sell.String())

	amount, err := commonutils.Decimal128FromBigInt(event.Amount)
	if err != nil {
		commonlog.Logger.Error("TransferOrder",
			zap.String("ConvertDecimalToBigInt", err.Error()),
		)
	}

	err = a.UpdateAccountOrder(0, chainId, &from, &sell, commonutils.SubDecimal128(&zero, amount), -1)
	if err != nil {
		commonlog.Logger.Error("TransferOrder",
			zap.String("From", from),
		)
	}
	err = a.UpdateAccountOrder(0, chainId, &to, &sell, amount, 1)
	if err != nil {
		commonlog.Logger.Error("TransferOrder",
			zap.String("From", from),
		)
	}

	return nil
}

func (a *AccountDB) TransferPosition(
	tradeType account.TradeType,
	chainId *string,
	event *market.EventTransferPosition,
	value *primitive.Decimal128,
) error {
	from := strings.ToLower(event.From.String())
	to := strings.ToLower(event.To.String())

	pay := strings.ToLower(event.Pay.String())
	item := strings.ToLower(event.Item.String())

	leverage, err := commonutils.Decimal128FromBigInt(event.Leverage)
	if err != nil {
		commonlog.Logger.Error("SaveTransferPosition",
			zap.String("ConvertDecimalToBigInt", err.Error()),
		)
	}

	size, err := commonutils.Decimal128FromBigInt(event.Size)
	if err != nil {
		commonlog.Logger.Error("SaveTransferPosition",
			zap.String("ConvertDecimalToBigInt", err.Error()),
		)
	}

	transferType := account.TradeTypeLongTransfer
	if strings.EqualFold(tradeType.String(), "Long") {
		transferType = account.TradeTypeShortTransfer
	}

	err = a.UpdateAccountPosition(
		transferType,
		&account.Asset{
			Account:  from,
			Address:  pay,
			Leverage: *leverage,
			Position: []account.Position{
				{
					Address: item,
					Size:    *size,
				},
			},
		},
		&primitive.Decimal128{},
		value,
	)

	if err != nil {
		commonlog.Logger.Error("TransferPosition",
			zap.String("From:", from),
		)
		return err
	}

	err = a.UpdateAccountPosition(
		tradeType,
		&account.Asset{
			Account:  to,
			Address:  pay,
			Leverage: *leverage,
			Position: []account.Position{
				{
					Address: item,
					Size:    *size,
				},
			},
		},
		&primitive.Decimal128{},
		value,
	)

	if err != nil {
		commonlog.Logger.Error("TransferPosition",
			zap.String("To:", to),
		)
		return err
	}

	return nil
}
