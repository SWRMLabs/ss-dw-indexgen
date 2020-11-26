package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	logger "github.com/ipfs/go-log/v2"
	_ "github.com/lib/pq"
)

type insertdata struct {
	Project string
	Key     string
	Ip      string
	Hash    string
}

type Out struct {
	Downloadindex int64  `json:"downloadindex"`
	Project       string `json:"project-id"`
	Key           string `json:"public-key"`
	Ip            string `json:"ip"`
	Hash          string `json:"hash"`
	Timestamp     string `json:"timestamp"`
}

type mClientInsertdata struct {
	Pubkey     string
	Ip         string
	CustomerId string
}

var log = logger.Logger("sql/postgres")

func newConfig(projectid string, key string, ip string, hashvalue string) *insertdata {
	return &insertdata{
		Project: projectid,
		Key:     key,
		Ip:      ip,
		Hash:    hashvalue,
	}
}

func mCNewConfig(key string, ip string, customerId string) *mClientInsertdata {
	return &mClientInsertdata{
		Pubkey:     key,
		Ip:         ip,
		CustomerId: customerId,
	}
}

func Open() (*sql.DB, func(), error) {
	jsonfile, err := os.Open("url-store.json")
	if err != nil {
		log.Errorf("Unable to open json file %s", err.Error())
		return nil, nil, err
	}
	url, err := ioutil.ReadAll(jsonfile)
	if err != nil {
		log.Errorf("Unable to read data from json file %s", err.Error())
		return nil, nil, err
	}
	var str map[string]string
	err = json.Unmarshal(url, &str)
	if err != nil {
		log.Errorf("Failed to Unmarshal url %s", err.Error())
		return nil, nil, err
	}
	db, err := sql.Open("postgres", str["url"])
	if err != nil {
		log.Errorf("Unable to connect %s", err.Error())
		return nil, nil, err
	}
	return db, func() {
		db.Close()
	}, nil
}

func GenerateIndex(
	db *sql.DB,
	projectid string,
	key string,
	ip string,
	hashvalue string,
) (*Out, error) {

	insertdata := newConfig(projectid, key, ip, hashvalue)
	timestamp := time.Now().Unix()
	bcn, err := getBCN(timestamp, db)
	if err != nil {
		log.Error("Unable to get bcn %s", err.Error())
		return nil, err
	}
	err = createTable(db, bcn)
	if err != nil {
		log.Errorf("Unable to create table %s", err.Error())
		return nil, err
	}
	jsonData, err := insertion(db, bcn, insertdata)
	if err != nil {
		log.Errorf("Unable to insert data %s", err.Error())
		return nil, err
	}
	return jsonData, nil
}

func MclientIndexGen(
	db *sql.DB,
	pubKey string,
	ip string,
	customerId string,
) (int64, error) {
	if customerId == "" {
		return 0, errors.New("Customer id is found empty")
	}
	inData := mCNewConfig(pubKey, ip, customerId)
	err := mcCreateTable(db)
	if err != nil {
		log.Errorf("Failed to create medium client %s", err.Error())
		return 0, err
	}
	newIndex, err := mClientInsertion(db, inData)
	if err != nil {
		log.Errorf("Failed medium-clinet insert data into table %s", err.Error())
		return 0, err
	}
	return newIndex, nil
}

func mcCreateTable(db *sql.DB) error {
	tablename := "midClients"
	query := fmt.Sprintf(`create table if not exists %s
	(mcindex serial,
	 publickey varchar(200),
	 ip varchar(45),
	 customerId varchar(45)
	 )`, tablename)
	_, err := db.Query(query)
	if err != nil {
		log.Errorf("Unbale to create table %s", err.Error())
		return err
	}
	return nil
}

func mClientInsertion(db *sql.DB, data *mClientInsertdata) (int64, error) {
	tablename := "midClients"
	query := fmt.Sprintf(`
		insert into %s (
		publicKey,ip,
		customerId)
		VALUES($1,$2,$3)
		returning mcindex`, tablename)
	var id int64
	err := db.QueryRow(query, data.Pubkey, data.Ip, data.CustomerId).Scan(&id)
	if err != nil {
		log.Errorf("Failed to insert data into table %s", err.Error())
		return 0, err
	}
	return id, nil
}

func getBCN(timestamp int64, db *sql.DB) (int64, error) {
	rows, err := db.Query(`select bcn from timebcndcnmapping where starttime<=$1 and endtime>=$1`, timestamp)
	if err != nil {
		log.Errorf("Select query unable to excute %s", err.Error())
		return 0, err
	}
	defer rows.Close()
	var bcn int64
	for rows.Next() {
		err := rows.Scan(&bcn)
		if err != nil {
			log.Errorf("Failed to scan BCN %s", err.Error())
			return 0, err
		}
	}
	return bcn, nil
}

func createTable(db *sql.DB, bcn int64) error {
	tablename := fmt.Sprintf("downloads_requests_%#v", bcn)
	query := fmt.Sprintf(`create table if not exists %s
	(downloadindex serial,
	 projectid varchar(50),
	 publickey varchar(200),
	 ip varchar(45),
	 hash varchar(200),
	 timestamp timestamp default current_timestamp)`, tablename)
	r, err := db.Query(query)
	if err != nil {
		log.Errorf("Unbale to create table %s", err.Error())
		return err
	}
	r.Close()
	return nil
}

func insertion(db *sql.DB, bcn int64, insertdata *insertdata) (*Out, error) {
	tablename := fmt.Sprintf("downloads_requests_%#v", bcn)
	query := fmt.Sprintf(`insert into %s (projectId,publicKey,ip,hash)VALUES($1,$2,$3,$4) returning downloadindex`, tablename)
	var id string
	err := db.QueryRow(query, insertdata.Project, insertdata.Key, insertdata.Ip, insertdata.Hash).Scan(&id)
	if err != nil {
		log.Errorf("Unable to excute insert query %s", err.Error())
		return nil, err
	}
	dataretrive := fmt.Sprintf(`select * from %s where downloadindex = %s`, tablename, id)
	rows, err := db.Query(dataretrive)
	if err != nil {
		log.Error("Data retrival from select is failed %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	result := &Out{}
	for rows.Next() {
		err := rows.Scan(&result.Downloadindex, &result.Project, &result.Key, &result.Ip, &result.Hash, &result.Timestamp)
		if err != nil {
			log.Errorf("Unable to get resultant tuple from database %s", err.Error())
			return nil, err
		}
	}
	return result, nil
}
