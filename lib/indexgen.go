package lib

import (
	"database/sql"
	pg "github.com/SWRMLabs/ss-dw-indexgen/postgres"
	logger "github.com/ipfs/go-log/v2"
	"sync"
	"time"
)

var log = logger.Logger("indexgen/lib")

type IndexGenerator struct {
	lk    sync.Mutex
	pgUrl string
	db    *sql.DB
}

func NewIndexGenerator(pgUrl string) (*IndexGenerator, error) {
	id := &IndexGenerator{
		pgUrl: pgUrl,
	}
	_, err := id.open()
	if err != nil {
		return nil, err
	}
	return id, nil
}

func (i *IndexGenerator) open() (*sql.DB, error) {
	i.lk.Lock()
	defer i.lk.Unlock()

	var err error
	// If DB was opened, check if the connection is still usable
	if i.db != nil {
		err = i.db.Ping()
	}
	// If DB was not opened or its not usable, create a new connection
	if i.db == nil || err != nil {
		if err != nil {
			log.Error("Unable to ping closing previous DB", err.Error())
			i.db.Close()
		}
		log.Info("Opening new DB")
		i.db, err = sql.Open("postgres", i.pgUrl)
		if err != nil {
			return nil, err
		}
		i.db.SetMaxOpenConns(5)
		i.db.SetMaxIdleConns(5)
		i.db.SetConnMaxLifetime(5*time.Minute)
	} else {
		log.Info("Using existing DB")
	}
	return i.db, nil
}

func (i *IndexGenerator) McGenerate(
	key string,
	ip string,
	customerId string) (int64, error) {
	db, err := i.open()
	if err != nil {
		return 0, err
	}
	return pg.MclientIndexGen(db, key, ip, customerId)
}

func (i *IndexGenerator) Generate(
	projectid string,
	key string,
	ip string,
	hashvalue string,
) (*pg.Out, error) {
	dbp, err := i.open()
	if err != nil {
		return nil, err
	}
	return pg.GenerateIndex(dbp, projectid, key, ip, hashvalue)
}

func (i *IndexGenerator) Close() error {
	i.lk.Lock()
	defer i.lk.Unlock()

	if i.db != nil {
		return i.db.Close()
	}
	log.Debug("Closing DB")
	return nil
}
