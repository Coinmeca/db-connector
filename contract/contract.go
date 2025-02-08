package contract

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/coinmeca/go-common/commondatabase"
	"github.com/coinmeca/go-common/commonprotocol"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"go.mongodb.org/mongo-driver/bson"
)

func convertAbiType(inputData *[]map[string]interface{}) (*abi.ABI, error) {
	if abiBytes, err := json.Marshal(inputData); err != nil {
		return nil, err
	} else if result, err := abi.JSON(strings.NewReader(string(abiBytes))); err != nil {
		return nil, err
	} else {
		return &result, nil
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
