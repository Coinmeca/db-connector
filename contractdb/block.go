package contractdb

import (
	"context"
	"fmt"
	"math/big"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (c *ContractDB) BsonForCheckpoint(chainId *string, blockNumber *big.Int) (bson.M, bson.M) {
	filter := bson.M{
		"chainId": *chainId,
	}

	update := bson.M{
		"$setOnInsert": bson.M{
			"chainId": *chainId,
		},
		"$max": bson.M{
			"checkpoint": blockNumber.Int64(),
		},
	}

	return filter, update
}

func (c *ContractDB) SaveCheckpoint(chainId *string, blockNumber *big.Int) error {
	filter, update := c.BsonForCheckpoint(chainId, blockNumber)
	option := options.Update().SetUpsert(true)

	_, err := c.ColChain.UpdateOne(
		context.Background(),
		filter,
		update,
		option,
	)
	if err != nil {
		fmt.Println("error", err)
		return err
	}
	return nil
}

func (c *ContractDB) GetCheckpoint(chainId *string) *big.Int {
	chain := c.GetChain(chainId)
	if chain == nil || chain.Checkpoint == 0 {
		return big.NewInt(0)
	}

	return big.NewInt(int64(chain.Checkpoint))
}
