package lib

import (
	"database/sql"
	pg "github.com/StreamSpace/ss-dw-indexgen/postgres"
	"sync"
)

type indexGenerator struct {
	lk    sync.Mutex
	pgUrl string
	db    *sql.DB
}

func NewIndexGenerator(pgUrl string) (*indexGenerator, error) {
	id := &indexGenerator{
		pgUrl: pgUrl,
	}
	_, err := id.open()
	if err != nil {
		return nil, err
	}
	return id, nil
}

func (i *indexGenerator) open() (*sql.DB, error) {
	i.lk.Lock()
	defer i.lk.Unlock()

	var err error
	// If DB was opened, check if the connection is still usable
	if i.db != nil {
		err = i.db.Ping()
	}
	// If DB was not opened or its not usable, create a new connection
	if i.db == nil || err != nil {
		i.db, err = sql.Open("postgres", i.pgUrl)
		if err != nil {
			return nil, err
		}
	}
	return i.db, nil
}

func (i *indexGenerator) Generate(
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

func (i *indexGenerator) Close() error {
	i.lk.Lock()
	defer i.lk.Unlock()

	if i.db != nil {
		return i.db.Close()
	}
	return nil
}
