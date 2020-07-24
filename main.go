package main

import (
	"flag"

	pg "github.com/StreamSpace/New-Postgres/postgres"
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
		err := pg.GenerateIndex(*projectid, *publickey, *ip, *hashvalue)
		if err != nil {
			log.Error("Message",err)
		}
	}
}
