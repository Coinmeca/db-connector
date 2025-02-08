package contract

import (
	"context"
	"fmt"
	"math/big"

	"github.com/coinmeca/go-common/commonlog"
	"github.com/ethereum/go-ethereum"
	"go.uber.org/zap"
)

func (c *ContractDB) ContractCall(ctx context.Context, chainId string, msg ethereum.CallMsg, blockNumber *big.Int) []byte {
	ethRepo := c.GetEthRepo(chainId)
	callResult, err := ethRepo.GetEthClient().CallContract(ctx, msg, blockNumber)

	if err != nil {
		fmt.Println("callResult", callResult)
		fmt.Println("callResultErr", err)

		keys, err := c.key.GetNewKeys("alchemy", chainId)
		if err != nil {
			commonlog.Logger.Error("ContractVaultGetAll",
				zap.String("GetNewKeys", err.Error()),
			)
			return nil
		}

		for _, new := range keys {
			ethRepo = c.GetEthRepoByKey(chainId, new)
			callResult, err = ethRepo.GetEthClient().CallContract(ctx, msg, blockNumber)
			if err == nil {
				c.key.SetKey("alchemy", chainId, new.Key)
				break
			}
		}

		return callResult
	}

	return callResult
}

func (c *ContractDB) Call(chainId string, result interface{}, method string, args ...interface{}) error {
	ethRepo := c.GetEthRepo(chainId)
	err := ethRepo.Call(&result, method, args...)

	if err != nil {
		fmt.Println("callResultErr", err)

		keys, err := c.key.GetNewKeys("alchemy", chainId)
		if err != nil {
			commonlog.Logger.Error("Call",
				zap.String("GetNewKeys", err.Error()),
			)
			return err
		}

		for _, new := range keys {
			ethRepo = c.GetEthRepoByKey(chainId, new)
			err = ethRepo.Call(result, method, args...)

			if err == nil {
				c.key.SetKey("alchemy", chainId, new.Key)
				break
			}
		}

		return err
	}

	return err
}
