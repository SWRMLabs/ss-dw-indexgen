package main

import (
	"flag"
	pg "github.com/StreamSpace/New-Postgres/postgres"
	logger "github.com/ipfs/go-log/v2"
)

func main() {
	logger.SetLogLevel("sql/postgres", "debug")
	projectid := flag.String("id", "", "project if of database")
	publickey := flag.String("key", "", "key to access database")
	ip := flag.String("ip", "", "ip address")
	hashvalue := flag.String("hash", "", "hashed values")
	flag.Parse()
	pg.NewPostgres(*projectid, *publickey, *ip, *hashvalue)

}
