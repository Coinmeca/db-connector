package modelcontract

import (
	"coinmeca-batch/conf"
	"coinmeca-batch/key"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonprotocol"
	commonrepository "github.com/coinmeca/go-common/commonrepository"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type ContractDB struct {
	conf *conf.Config

	client      *mongo.Client
	colContract *mongo.Collection
	colChain    *mongo.Collection

	key          *key.KeyManager
	ethRepo      map[string]*commonrepository.EthRepository
	chains       []string
	chainsUpdate int64
	start        chan struct{}
}

func NewDB(config *conf.Config, key *key.KeyManager) (commondatabase.IRepository, error) {
	r := &ContractDB{
		conf:    config,
		key:     key,
		start:   make(chan struct{}),
		ethRepo: make(map[string]*commonrepository.EthRepository),
		chains:  make([]string, 0),
	}

	var err error
	credential := options.Credential{
		Username: config.Repositories["contractDB"]["username"].(string),
		Password: config.Repositories["contractDB"]["pass"].(string),
	}

	clientOptions := options.Client().ApplyURI(config.Repositories["contractDB"]["datasource"].(string)).SetAuth(credential)
	if r.client, err = mongo.Connect(context.Background(), clientOptions); err != nil {
		return nil, err
	}

	// 수정된 부분: client.Ping 호출이 성공했을 때 컬렉션 초기화
	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["contractDB"]["db"].(string))
		r.colContract = db.Collection("contract")
		r.colChain = db.Collection("chain")
	} else {
		return nil, err
	}

	//commonlog.Trace("load repository :", "ContractDB", r.conf.Common)
	commonlog.Logger.Debug("load repository",
		zap.String("contractDB", r.conf.Common.ServiceId),
	)
	return r, nil
}

func (c *ContractDB) Start() error {
	return func() (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = v.(error)
			}
		}()
		close(c.start)
		return
	}()
}

func convertAbiType(inputData *[]map[string]interface{}) (*abi.ABI, error) {
	if abiBytes, err := json.Marshal(inputData); err != nil {
		return nil, err
	} else if result, err := abi.JSON(strings.NewReader(string(abiBytes))); err != nil {
		return nil, err
	} else {
		return &result, nil
	}
}

func (c *ContractDB) ConnectKeyManager(key *key.KeyManager) {
	c.key = key
}

func (c *ContractDB) GetContracts() ([]*commonprotocol.Contract, error) {
	result := make([]*commonprotocol.Contract, 0)
	if cursor, err := c.colContract.Find(context.Background(), bson.M{}); err != nil {
		return nil, err
	} else {
		defer cursor.Close(context.Background())
		for cursor.Next(context.TODO()) {
			temp := &commondatabase.Contract{}
			if err := cursor.Decode(temp); err != nil {
				return nil, err
			}
			abiData, err := convertAbiType(temp.Abi)
			if err != nil {
				return nil, err
			}

			result = append(result, &commonprotocol.Contract{
				Id:        temp.Id,
				ChainId:   temp.ChainId,
				ServiceId: temp.ServiceId,
				Cate:      temp.Cate,
				Name:      temp.Name,
				Address:   temp.Address,
				Abi:       abiData,
			})
		}
		return result, nil
	}

}

func (c *ContractDB) GetContractsByCate(cate string) ([]*commonprotocol.Contract, error) {
	result := make([]*commonprotocol.Contract, 0)
	filter := bson.M{
		"cate": cate,
	}
	if cursor, err := c.colContract.Find(context.Background(), filter); err != nil {
		return nil, err
	} else {
		defer cursor.Close(context.Background())
		for cursor.Next(context.TODO()) {
			temp := &commondatabase.Contract{}
			if err := cursor.Decode(temp); err != nil {
				return nil, err
			}
			abiData, err := convertAbiType(temp.Abi)
			if err != nil {
				return nil, err
			}

			result = append(result, &commonprotocol.Contract{
				Id:        temp.Id,
				ChainId:   temp.ChainId,
				ServiceId: temp.ServiceId,
				Cate:      temp.Cate,
				Name:      temp.Name,
				Address:   temp.Address,
				Abi:       abiData,
			})
		}
		return result, nil
	}
}

func (c *ContractDB) GetContract(name string) (*commonprotocol.Contract, error) {
	temp := &commondatabase.Contract{}
	err := c.colContract.FindOne(context.Background(), bson.M{
		"name": name,
	}).Decode(temp)
	if err != nil {
		return nil, err
	}

	abiData, err := convertAbiType(temp.Abi)
	if err != nil {
		return nil, err
	}

	result := &commonprotocol.Contract{
		Id:        temp.Id,
		ChainId:   temp.ChainId,
		ServiceId: temp.ServiceId,
		Cate:      temp.Cate,
		Name:      temp.Name,
		Address:   temp.Address,
		Abi:       abiData,
	}

	return result, nil
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

func (c *ContractDB) GetChains() []*commondatabase.Chain {
	cursor, err := c.colChain.Find(context.Background(), bson.M{})
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

func (c *ContractDB) GetEthRepo(chainId string) *commonrepository.EthRepository {
	ethRepo, ok := c.ethRepo[chainId]
	if !ok {
		current := c.key.GetCurrentKey("alchemy", chainId)
		ethRepo = commonrepository.NewEthRepository(current.Url + current.Key)
		c.ethRepo[chainId] = ethRepo
	}
	return ethRepo
}

func (c *ContractDB) GetEthRepoByKey(chainId string, key *commondatabase.APIKey) *commonrepository.EthRepository {
	ethRepo := commonrepository.NewEthRepository(key.Url + key.Key)
	c.ethRepo[chainId] = ethRepo
	return ethRepo
}

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
