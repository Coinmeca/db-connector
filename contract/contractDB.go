package contract

import (
	"context"
	"db-connector/conf"
	"db-connector/key"
	"fmt"
	"math/big"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonlog"
	"github.com/coinmeca/go-common/commonprotocol"
	commonrepository "github.com/coinmeca/go-common/commonrepository"
	"github.com/ethereum/go-ethereum"
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

type ContractDBInterface interface {
	ConnectKeyManager(key *key.KeyManager)

	Call(chainId string, result interface{}, method string, args ...interface{}) error
	ContractCall(ctx context.Context, chainId string, msg ethereum.CallMsg, blockNumber *big.Int) []byte

	// getter
	GetChains() []*commondatabase.Chain
	GetContract(name string) (*commonprotocol.Contract, error)
	GetContracts() ([]*commonprotocol.Contract, error)
	GetContractsByCate(cate string) ([]*commonprotocol.Contract, error)
	GetEthRepo(chainId string) *commonrepository.EthRepository
	GetEthRepoByKey(chainId string, key *commondatabase.APIKey) *commonrepository.EthRepository
	GetTargetChains() []string

	Start() error
}

func NewDB(config *conf.Config) (commondatabase.IRepository, error) {
	key, err := key.NewKeyManager(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize key manager: %v", err))
	}

	r := &ContractDB{
		conf:    config,
		key:     key,
		start:   make(chan struct{}),
		ethRepo: make(map[string]*commonrepository.EthRepository),
		chains:  make([]string, 0),
	}

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

func (c *ContractDB) ConnectKeyManager(key *key.KeyManager) {
	c.key = key
}
