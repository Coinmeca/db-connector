﻿package contractdb

import (
	"context"
	"time"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

func (c *ContractDB) GetChains() []*commondatabase.Chain {
	cursor, err := c.ColChain.Find(context.Background(), bson.M{})
	if err != nil {
		commonlog.Logger.Error("GetChains",
			zap.String("Cannot decode", err.Error()),
		)
		return nil
	}
	defer cursor.Close(context.Background())

	var result []*commondatabase.Chain
	// Iterate through the documents
	for cursor.Next(context.Background()) {
		chain := &commondatabase.Chain{}
		err := cursor.Decode(&chain)
		if err == nil {
			result = append(result, chain)
		} else {
			commonlog.Logger.Error("GetChains",
				zap.String("Cannot decode", err.Error()),
			)
		}
	}

	// Check if any error occurred during iteration
	if err := cursor.Err(); err != nil {
		commonlog.Logger.Error("GetChains",
			zap.String("Cursor error", err.Error()),
		)
		return nil
	}

	return result
}

func (c *ContractDB) GetChain(chainId *string) *commondatabase.Chain {
	result := &commondatabase.Chain{}

	filter := bson.M{"chainId": chainId}
	if err := c.ColChain.FindOne(context.Background(), filter, nil).Decode(&result); err != nil {
		commonlog.Logger.Error("GetChain",
			zap.String("not found ", err.Error()),
		)
		return nil
	}

	return result
}

func (c *ContractDB) GetTargetChains() []string {
	now := time.Now().Unix()
	if c.chainsUpdate > (now-86400) && c.chains != nil && len(c.chains) > 0 {
		return c.chains
	}

	chains := c.conf.Chains
	if len(chains) == 0 {

		chainsInfo := c.GetChains()
		if len(chainsInfo) > 0 {
			for _, chain := range chainsInfo {
				chains = append(chains, chain.ChainId)
			}
		}
	}

	if len(chains) == 0 {
		return nil
	}

	c.chains = chains
	c.chainsUpdate = now
	return chains
}
