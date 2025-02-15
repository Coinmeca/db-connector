package accountdb

import (
	"fmt"
	"strings"

	"github.com/coinmeca/go-common/commonmethod/account"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (a *AccountDB) BsonForGetAccountAssets(chainId, account string, assets *[]*string) *bson.M {
	var conditions []bson.M

	for _, asset := range *assets {
		conditions = append(conditions, bson.M{"name": bson.M{"$regex": *asset, "$options": "i"}})
	}

	filter := bson.M{
		"chainId": chainId,
		"account": account,
		"$or":     conditions,
	}

	return &filter
}

func (a *AccountDB) BsonForUpdateAccountAsset(chainId, account string, asset *account.Asset) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": chainId,
		"account": account,
		"address": bson.M{"$regex": asset.Address, "$options": "i"},
	}

	update := bson.M{
		"$set": bson.M{
			"count":   asset.Count,
			"total":   asset.Total,
			"average": asset.Average,
		},
	}

	return filter, update
}

func (a *AccountDB) BsonForUpdateAccountAssets(chainId, account string, assets []*account.Asset) *[]mongo.WriteModel {
	var models []mongo.WriteModel

	for _, asset := range assets {
		filter := bson.M{
			"chainId": chainId,
			"account": account,
			"address": bson.M{"$regex": asset.Address, "$options": "i"},
		}

		update := bson.M{
			"$setOnInsert": filter,
			"$set": bson.M{
				"count":   asset.Count,
				"total":   asset.Total,
				"average": asset.Average,
			},
		}

		models = append(models, mongo.NewUpdateManyModel().
			SetFilter(filter).
			SetUpdate(update))
	}

	return &models
}

func (t *AccountDB) BsonForUpdateAccountOrder(
	tradeType account.TradeType,
	chainId, account, asset *string,
	amount *primitive.Decimal128,
	count int64,
) (bson.M, bson.A) {
	zero := primitive.Decimal128{}
	counts := strings.ToLower(fmt.Sprintf("$count.%s", tradeType.String()))

	filter := bson.M{
		"chainId": *chainId,
		"account": *account,
		"address": *asset,
	}

	update := bson.A{
		bson.M{
			"$set": bson.M{
				counts: bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$" + counts, zero}},
								count,
							}},
							zero,
						}},
						zero,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$" + counts, zero}},
							count,
						}},
					},
				},
				"order": bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$order", zero}},
								amount,
							}},
							zero,
						}},
						zero,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$order", zero}},
							amount,
						}},
					},
				},
			},
		},
	}

	return filter, update
}

func (t *AccountDB) BsonForUpdateAccountOrderWithLeverage(
	tradeType account.TradeType,
	chainId, account, asset *string,
	amount, leverage *primitive.Decimal128,
	count int64,
) (bson.M, bson.A) {
	zero := primitive.Decimal128{}
	counts := strings.ToLower(fmt.Sprintf("$count.%s", tradeType.String()))

	filter := bson.M{
		"chainId": *chainId,
		"account": *account,
		"address": *asset,
	}

	update := bson.A{
		bson.M{
			"$set": bson.M{
				counts: bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$" + counts, zero}},
								count,
							}},
							zero,
						}},
						zero,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$" + counts, zero}},
							count,
						}},
					},
				},
				"order": bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$order", zero}},
								amount,
							}},
							zero,
						}},
						zero,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$order", zero}},
							amount,
						}},
					},
				},
				"leverage": bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$order", zero}},
								leverage,
							}},
							zero,
						}},
						zero,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$order", zero}},
							leverage,
						}},
					},
				},
			},
		},
	}

	return filter, update
}

func (t *AccountDB) BsonForUpdateAccountAssetUse(
	tradeType account.TradeType,
	chainId, account, asset *string,
	amount *primitive.Decimal128,
	count int64,
) (bson.M, bson.A) {
	zero := primitive.Decimal128{}
	counts := strings.ToLower(fmt.Sprintf("$count.%s", tradeType.String()))

	filter := bson.M{
		"chainId": *chainId,
		"account": *account,
		"address": *asset,
	}

	update := bson.A{
		bson.M{
			"$set": bson.M{
				counts: bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$" + counts, zero}},
								count,
							}},
							zero,
						}},
						zero,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$" + counts, zero}},
							count,
						}},
					},
				},
				"using": bson.M{
					"$cond": bson.A{
						bson.M{"$lt": bson.A{
							bson.M{"$add": bson.A{
								bson.M{"$ifNull": bson.A{"$using", zero}},
								amount,
							}},
							0,
						}},
						0,
						bson.M{"$add": bson.A{
							bson.M{"$ifNull": bson.A{"$using", zero}},
							amount,
						}},
					},
				},
			},
		},
	}

	return filter, update
}

func (a *AccountDB) BsonForGetAccountPosition(chainId, user, pay, item *string) (bson.M, bson.M) {
	filter := bson.M{
		"chainId":          *chainId,
		"account":          *user,
		"address":          *pay,
		"position.address": *item,
	}

	projection := bson.M{
		"position.$": 1,
	}

	return filter, projection
}

func (a *AccountDB) BsonForGetAccountPositions(chainId, user, asset *string) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": *chainId,
		"account": *user,
		"address": *asset,
	}

	projection := bson.M{
		"position.$": 1,
	}

	return filter, projection
}

func (a *AccountDB) BsonForGetAccountAssetByPosition(chainId, user, pay, item *string) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": *chainId,
		"account": *user,
		"address": *pay,
	}

	projection := bson.M{
		"chainId":  1,
		"address":  1,
		"account":  1,
		"leverage": 1,
		"count":    1,
		"position": bson.M{
			"$elemMatch": bson.M{
				"address": *item,
			},
		},
	}

	return filter, projection
}

func (a *AccountDB) BsonForInitAccountPosition(asset *account.Asset) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": asset.ChainId,
		"account": asset.Account,
		"address": asset.Address,
	}

	update := bson.M{
		"$set": bson.M{
			"count":    asset.Count,
			"leverage": asset.Leverage,
			"total":    asset.Total,
			"average":  asset.Average,
		},
		"$addToSet": bson.M{
			"position": asset.Position[0],
		},
	}

	return filter, update
}

func (a *AccountDB) BsonForUpdateAccountPosition(asset *account.Asset) (bson.M, bson.M) {
	filter := bson.M{
		"chainId":          asset.ChainId,
		"account":          asset.Account,
		"address":          asset.Address,
		"position.address": asset.Position[0].Address,
	}

	update := bson.M{
		"$set": bson.M{
			"count":      asset.Count,
			"leverage":   asset.Leverage,
			"total":      asset.Total,
			"average":    asset.Average,
			"position.$": asset.Position[0],
		},
	}

	return filter, update
}
