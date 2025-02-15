package contractdb

import (
	"context"
	"math/big"

	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/db-connector/key"
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

	client       *mongo.Client
	ColContract  *mongo.Collection
	ColChain     *mongo.Collection
	ColChainInfo *mongo.Collection

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

func NewDB(config *conf.Config, key *key.KeyManager) (commondatabase.IRepository, error) {
	var err error

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

	if err = r.client.Ping(context.Background(), nil); err == nil {
		db := r.client.Database(config.Repositories["contractDB"]["db"].(string))
		r.ColContract = db.Collection("contract")
		r.ColChain = db.Collection("chain")
	} else {
		return nil, err
	}

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
