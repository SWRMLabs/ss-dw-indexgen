package main

import (
	"encoding/json"
	"flag"
	"fmt"

	pg "github.com/SWRMLabs/ss-dw-indexgen/postgres"
	logger "github.com/ipfs/go-log/v2"
)

var log = logger.Logger("sql/database")

func main() {
	logger.SetLogLevel("sql/postgres", "debug")
	projectid := flag.String("id", "", "project if of database")
	publickey := flag.String("key", "", "key to access database")
	ip := flag.String("ip", "", "ip address")
	hashvalue := flag.String("hash", "", "hashed values")
	flag.Parse()
	if *projectid == "" || *publickey == "" || *ip == "" || *hashvalue == "" {
		log.Errorf("Flags must not be empty")
	} else {
		db, closeFn, err := pg.Open()
		if err != nil {
			log.Error("Failed to open database connection", err)
			return
		}
		defer closeFn()
		result, err := pg.GenerateIndex(db, *projectid, *publickey, *ip, *hashvalue)
		if err != nil {
			log.Error("Message", err)
			return
		}
		js, err := json.MarshalIndent(result, " ", "\t")
		if err != nil {
			log.Errorf("Unable to indent marshal %s", err.Error())
			return
		}
		fmt.Println(string(js))
	}
}
