package farmdb

import (
	"context"
	"strings"

	"github.com/coinmeca/go-common/commonmethod/farm"
	"github.com/coinmeca/go-common/commonprotocol"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (f *FarmDB) GetAllFarmAddresses() ([]*commonprotocol.Contract, error) {
	var farms []*commonprotocol.Contract
	option := options.Find().SetProjection(bson.M{
		"chainId": 1,
		"address": 1,
		"main":    1,
		"name":    1,
	})
	cursor, err := f.ColFarm.Find(context.Background(), bson.M{}, option)

	if err != nil {
		return farms, err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		f := &farm.Farm{}
		if err := cursor.Decode(&f); err == nil {
			farm := &commonprotocol.Contract{
				Cate:    "abstract",
				ChainId: f.ChainId,
				Address: f.Address,
			}
			if strings.ToLower(f.Address) == strings.ToLower(f.Main) {
				farm.Name = "farm-main"
			} else {
				farm.Name = "farm-derive"
			}
			farms = append(farms, farm)
		}
	}

	return farms, nil
}
