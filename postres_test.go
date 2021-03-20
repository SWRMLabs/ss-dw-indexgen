package main

import (
	"github.com/SWRMLabs/ss-dw-indexgen/lib"
	"testing"
)

type input struct {
	ip   string
	key  string
	csId string
}

func setUp() *input {
	return &input{
		ip:   "abcde",
		key:  "dnsjvnjn3ngj3nj",
		csId: "nsdjnvjnvnjkv",
	}
}
func TestMain(t *testing.T) {
	in := setUp()
	pgGen, err := lib.NewIndexGenerator("postgres://qa:qa@123@34.70.130.132/postgres-qa")
	if err != nil {
		t.Fatal(err)
	}
	newIndex, err := pgGen.McGenrate(in.key, in.ip, in.csId)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("We got index is %d", newIndex)
}
