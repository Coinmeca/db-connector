package modelcontract

import (
	"github.com/coinmeca/go-common/commondatabase"
	commonrepository "github.com/coinmeca/go-common/commonrepository"
)

func (c *ContractDB) GetEthRepo(chainId string) *commonrepository.EthRepository {
	ethRepo, ok := c.ethRepo[chainId]
	if !ok {
		current := c.key.GetCurrentKey("alchemy", chainId)
		c.ethRepo[chainId] = commonrepository.NewEthRepository(current.Url + current.Key)
		ethRepo = c.ethRepo[chainId]
	}
	return ethRepo
}

func (c *ContractDB) GetEthRepoByKey(chainId string, key *commondatabase.APIKey) *commonrepository.EthRepository {
	ethRepo := commonrepository.NewEthRepository(key.Url + key.Key)
	c.ethRepo[chainId] = ethRepo
	return ethRepo
}
