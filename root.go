package db

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/coinmeca/db-connector/accountdb"
	"github.com/coinmeca/db-connector/batch"
	"github.com/coinmeca/db-connector/conf"
	"github.com/coinmeca/db-connector/contractdb"
	"github.com/coinmeca/db-connector/farmdb"
	"github.com/coinmeca/db-connector/historydb"
	"github.com/coinmeca/db-connector/key"
	"github.com/coinmeca/db-connector/marketdb"
	"github.com/coinmeca/db-connector/treasurydb"
	"github.com/coinmeca/db-connector/vaultdb"

	"github.com/coinmeca/go-common/commondatabase"
)

type RepositoryConstructor func(config *conf.Config, root *Repositories) (commondatabase.IRepository, error)

type Repositories struct {
	lock  sync.RWMutex
	conf  *conf.Config
	elems map[reflect.Type]reflect.Value
}

type IRepository interface {
	Start() error
}

func NewRepositories(c *conf.Config) (*Repositories, error) {
	r := &Repositories{
		conf:  c,
		elems: make(map[reflect.Type]reflect.Value),
	}

	if err := r.initializeRepositories(); err != nil {
		return nil, err
	}

	if err := r.startAll(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Repositories) initializeRepositories() error {
	key, err := key.NewKeyManager(r.conf)
	if err != nil {
		return fmt.Errorf("Key Manager initialization failed: %v", err)
	}
	r.register(key)

	rep, err := contractdb.NewDB(r.conf, key)
	if err != nil {
		return err
	}
	r.register(rep)

	repoInitializers := []func(*conf.Config) (commondatabase.IRepository, error){
		batch.NewDB,
		historydb.NewDB,
		vaultdb.NewDB,
		marketdb.NewDB,
		farmdb.NewDB,
		treasurydb.NewDB,
		accountdb.NewDB,
	}

	for _, initializer := range repoInitializers {
		rep, err := initializer(r.conf)
		if err != nil {
			return err
		}
		r.register(rep)
	}

	return nil
}

func (r *Repositories) startAll() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, elem := range r.elems {
		if err := elem.Interface().(commondatabase.IRepository).Start(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repositories) register(rep commondatabase.IRepository) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	repoType := reflect.TypeOf(rep)
	if _, exists := r.elems[repoType]; exists {
		return fmt.Errorf("duplicate repository instance: %v", repoType)
	}
	r.elems[repoType] = reflect.ValueOf(rep)
	return nil
}

func (r *Repositories) Register(constructor RepositoryConstructor, config *conf.Config) error {
	if p, err := constructor(config, r); err != nil {
		return err
	} else if r != nil {
		r.lock.Lock()
		defer r.lock.Unlock()

		if _, ok := r.elems[reflect.TypeOf(p)]; ok == true {
			return fmt.Errorf("duplicated instance of %v", reflect.TypeOf(p))
		} else {
			r.elems[reflect.TypeOf(p)] = reflect.ValueOf(p)
		}
	}
	return nil
}

func (r *Repositories) Get(rs ...interface{}) error {
	r.lock.RLock()
	defer r.lock.RUnlock()

	notFounds := make([]reflect.Type, 0)
	for _, v := range rs {
		elem := reflect.ValueOf(v).Elem()
		if e, ok := r.elems[elem.Type()]; ok == true {
			elem.Set(e)
		} else {
			notFounds = append(notFounds, elem.Type())
		}
	}

	if len(notFounds) > 0 {
		err := fmt.Errorf("unknown repository ")
		for _, e := range notFounds {
			err = fmt.Errorf("%v, %v ", err.Error(), e)
		}
		return err
	}

	return nil
}

func NewRepositoriesBackup(c *conf.Config) (*Repositories, error) {
	r := &Repositories{
		conf:  c,
		elems: make(map[reflect.Type]reflect.Value),
	}

	for _, c := range []struct {
		constructor RepositoryConstructor
		config      *conf.Config
	}{
		//{NewTraderDB, c},
		//{NewBatchDB, c},

		//{contract.NewDB, c},
		//{vault.NewDB, c},
		//{market.NewDB, c},
		//{farm.NewDB, c},
	} {
		if err := r.Register(c.constructor, c.config); err != nil {
			return nil, err
		}
	}

	if err := func() error {
		r.lock.Lock()
		defer r.lock.Unlock()

		for _, elem := range r.elems {
			if err := elem.Interface().(commondatabase.IRepository).Start(); err != nil {
				return err
			}
		}
		return nil
	}(); err != nil {
		return nil, err
	}

	return r, nil
}
