package modelaccount

import (
	"math/big"
	"strings"

	"github.com/coinmeca/go-common/commonmethod/account"
	"github.com/coinmeca/go-common/commonutils"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (a *AccountDB) CalculateAccountAsset(
	tradeType bool,
	amount,
	value *primitive.Decimal128,
	asset *account.Asset,
) *account.Asset {
	_100, _ := primitive.ParseDecimal128("100000000000000000000")

	// total
	if tradeType {
		asset.Total.Buy.Amount = *commonutils.AddDecimal128(&asset.Total.Buy.Amount, amount)
		asset.Total.Buy.Value = *commonutils.AddDecimal128(&asset.Total.Buy.Value, value)
	} else {
		asset.Total.Sell.Amount = *commonutils.AddDecimal128(&asset.Total.Sell.Amount, amount)
		asset.Total.Sell.Value = *commonutils.AddDecimal128(&asset.Total.Sell.Value, value)
	}

	// total: return
	asset.Total.Return.Amount = *commonutils.SubDecimal128(&asset.Total.Sell.Amount, &asset.Total.Buy.Amount)
	asset.Total.Return.Value = *commonutils.SubDecimal128(&asset.Total.Sell.Value, &asset.Total.Buy.Value)

	// total: return rate
	asset.Total.ReturnRate.Amount = *commonutils.DivDecimal128(
		commonutils.MulDecimal128(
			&asset.Total.Return.Amount,
			&_100,
		),
		&asset.Total.Buy.Amount,
	)
	asset.Total.ReturnRate.Value = *commonutils.DivDecimal128(
		commonutils.MulDecimal128(
			&asset.Total.Return.Value,
			&_100,
		),
		&asset.Total.Buy.Value,
	)

	// count
	if tradeType {
		asset.Count.Buy += 1
		count, _ := commonutils.Decimal128FromBigInt(
			new(big.Int).Mul(
				new(big.Int).SetInt64(asset.Count.Buy),
				new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
			),
		)
		asset.Average.Buy.Amount = *commonutils.DivDecimal128(&asset.Total.Buy.Amount, count)
		asset.Average.Buy.Value = *commonutils.DivDecimal128(&asset.Total.Buy.Value, count)
	} else {
		asset.Count.Sell += 1
		count, _ := commonutils.Decimal128FromBigInt(
			new(big.Int).Mul(
				new(big.Int).SetInt64(asset.Count.Sell),
				new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
			),
		)
		asset.Average.Sell.Amount = *commonutils.DivDecimal128(&asset.Total.Sell.Amount, count)
		asset.Average.Sell.Value = *commonutils.DivDecimal128(&asset.Total.Sell.Value, count)
	}

	// avg: reutrn
	asset.Average.Return.Amount = *commonutils.SubDecimal128(&asset.Average.Sell.Amount, &asset.Average.Buy.Amount)
	asset.Average.Return.Value = *commonutils.SubDecimal128(&asset.Average.Sell.Value, &asset.Average.Buy.Value)

	// avg: return rate
	asset.Average.ReturnRate.Amount = *commonutils.DivDecimal128(
		commonutils.MulDecimal128(
			&asset.Average.Return.Amount,
			&_100,
		),
		&asset.Average.Buy.Amount,
	)
	asset.Average.ReturnRate.Value = *commonutils.DivDecimal128(
		commonutils.MulDecimal128(
			&asset.Average.Return.Value,
			&_100,
		),
		&asset.Average.Buy.Value,
	)

	return asset
}

func (a *AccountDB) CalculateAccountPosition(tradeType account.TradeType, item *string, size, leverage, margin, value *primitive.Decimal128, asset *account.Asset) {

	count := &primitive.Decimal128{}

	_100, _ := primitive.ParseDecimal128("100000000000000000000")

	if asset.Position == nil || len(asset.Position) == 0 {
		asset.Position = []account.Position{{Address: *item, Size: primitive.Decimal128{}}}
	}

	if strings.HasSuffix(tradeType.String(), "Transfer") {
		asset.Count.Long -= 1
		if tradeType == account.TradeTypeLongTransfer {
			asset.Count.Short -= 1
		}

		count, _ = commonutils.Decimal128FromBigInt(
			new(big.Int).Mul(
				new(big.Int).SetInt64(asset.Count.Long+asset.Count.Short),
				new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
			),
		)
	} else if !strings.Contains(tradeType.String(), "Margin") {
		asset.Count.Long += 1
		if tradeType == account.TradeTypeShortTransfer {
			asset.Count.Short += 1
		}

		count, _ = commonutils.Decimal128FromBigInt(
			new(big.Int).Mul(
				new(big.Int).SetInt64(asset.Count.Long+asset.Count.Short),
				new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
			),
		)
	}

	if tradeType == account.TradeTypeClose {

		asset.Leverage = *commonutils.SubDecimal128(&asset.Leverage, leverage)
		asset.Position[0].Size = *commonutils.SubDecimal128(&asset.Position[0].Size, size)

	} else {
		margin := commonutils.SubDecimal128(size, leverage)

		if strings.HasSuffix(tradeType.String(), "Transfer") {
			asset.Leverage = *commonutils.SubDecimal128(&asset.Leverage, leverage)
			asset.Position[0].Size = *commonutils.SubDecimal128(&asset.Position[0].Size, size)

			asset.Total.Invest.Amount = *commonutils.SubDecimal128(&asset.Total.Invest.Amount, margin)
			asset.Total.Invest.Value = *commonutils.SubDecimal128(&asset.Total.Invest.Amount, commonutils.MulDecimal128(margin, value))

		} else {
			asset.Leverage = *commonutils.AddDecimal128(&asset.Leverage, leverage)
			asset.Position[0].Size = *commonutils.AddDecimal128(&asset.Position[0].Size, size)

			asset.Total.Invest.Amount = *commonutils.AddDecimal128(&asset.Total.Invest.Amount, margin)
			asset.Total.Invest.Value = *commonutils.AddDecimal128(&asset.Total.Invest.Amount, commonutils.MulDecimal128(margin, value))
		}
		asset.Average.Invest.Amount = *commonutils.DivDecimal128(&asset.Total.Invest.Amount, count)
		asset.Average.Invest.Value = *commonutils.DivDecimal128(&asset.Total.Invest.Value, count)
	}

	asset.Total.Pnl.Amount = *commonutils.AddDecimal128(&asset.Total.Pnl.Amount, margin)
	asset.Total.Pnl.Value = *commonutils.AddDecimal128(&asset.Total.Pnl.Value, commonutils.MulDecimal128(margin, value))

	asset.Average.Pnl.Amount = *commonutils.DivDecimal128(&asset.Total.Pnl.Amount, count)
	asset.Average.Pnl.Value = *commonutils.DivDecimal128(&asset.Total.Pnl.Value, count)

	asset.Total.PnlRate.Amount = *commonutils.DivDecimal128(
		commonutils.MulDecimal128(
			&asset.Total.Pnl.Amount,
			&_100,
		),
		&asset.Total.Invest.Amount,
	)
	asset.Total.PnlRate.Value = *commonutils.DivDecimal128(
		commonutils.MulDecimal128(
			&asset.Total.Pnl.Value,
			&_100,
		),
		&asset.Total.Invest.Value,
	)
}
