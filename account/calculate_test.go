package modelaccount

import (
	"github.com/coinmeca/go-common/commonmethod/account"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func setup() *account.Asset {
	return &account.Asset{
		Account: "",
		ChainId: "",
		Total: account.Metric{
			Buy: account.Volume{
				Amount: primitive.NewDecimal128(0, 0),
				Value:  primitive.NewDecimal128(0, 0),
			},
		},
	}
}
