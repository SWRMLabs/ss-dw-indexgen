package postgres

import (
	"database/sql"
	"encoding/json"
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

type extract struct {
	Downloadindex int64  `json:"downloadindex"`
	Project       string `json:"project-id"`
	Key           string `json:"public-key"`
	Ip            string `json:"ip"`
	Hash          string `json:"hash"`
	Timestamp     string `json:"timestamp"`
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

func GenerateIndex(projectid string, key string, ip string, hashvalue string) error {
	jsonfile, err := os.Open("url-store.json")
	if err != nil {
		log.Errorf("Unable to open json file %s", err.Error())
		return err
	}
	url, err := ioutil.ReadAll(jsonfile)
	if err != nil {
		log.Errorf("Unable to read data from json file %s", err.Error())
		return err
	}
	var str map[string]string
	err = json.Unmarshal(url, &str)
	if err != nil {
		log.Errorf("Failed to Unmarshal url %s", err.Error())
		return err
	}
	db, err := sql.Open("postgres", str["url"])
	if err != nil {
		log.Errorf("Unable to connect %s", err.Error())
		return err
	}
	defer db.Close()

	insertdata := newConfig(projectid, key, ip, hashvalue)
	timestamp := time.Now().Unix()
	bcn, err := getBCN(timestamp, db)
	if err != nil {
		log.Error("Unable to get bcn %s", err.Error())
		return err
	}
	err = createTable(db, bcn)
	if err != nil {
		log.Errorf("Unable to create table %s", err.Error())
		return err
	}
	jsondata, err := insertion(db, bcn, insertdata)
	if err != nil {
		log.Errorf("Unable to insert data %s", err.Error())
		return err
	}
	fmt.Println(jsondata)
	return nil
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
			log.Errorf("Scan of bcn is failed %s",err.Error())
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
	_, err := db.Query(query)
	if err != nil {
		log.Errorf("Unbale to create table %s", err.Error())
		return err
	}
	return nil
}

func insertion(db *sql.DB, bcn int64, insertdata *insertdata) (string, error) {
	tablename := fmt.Sprintf("downloads_requests_%#v", bcn)
	query := fmt.Sprintf(`insert into %s (projectId,publicKey,ip,hash)VALUES(%s,%s,%s,%s) returning downloadindex`,
		tablename, insertdata.Project, insertdata.Key, insertdata.Ip, insertdata.Hash)
	var id string
	err := db.QueryRow(query).Scan(&id)
	if err != nil {
		log.Errorf("Unable to excute insert query %s", err.Error())
		return "", err
	}

	dataretrive := fmt.Sprintf(`select * from %s where downloadindex = %s`, tablename, id)
	rows, err := db.Query(dataretrive)
	if err != nil {
		log.Error("Data retrival from select is failed %s", err.Error())
		return "", err
	}
	defer rows.Close()
	var result extract
	var js []byte
	for rows.Next() {
		err := rows.Scan(&result.Downloadindex, &result.Project, &result.Key, &result.Ip, &result.Hash, &result.Timestamp)
		if err != nil {
			log.Errorf("Unable to get resultant tuple from database %s", err.Error())
			return "", nil
		}
		js, err = json.MarshalIndent(result, " ", "\t")
		if err != nil {
			log.Errorf("Unable to indent marshal %s", err.Error())
			return "", err
		}
	}
	return string(js), nil
}
