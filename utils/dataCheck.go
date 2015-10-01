package utils

import (
	// "bytes"
	// "fmt"
	"log"
	"opencoredata.org/ocdJanus/connect"
	// "opencoredata.org/ocdJanus/mongo"
	// "os"
	// "text/template"
)

func DataCheck(qry string) bool {
	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rowscheck, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}
	len := 0
	for rowscheck.Next() {
		len = len + 1
	}

	if len > 0 {
		return true
	} else {
		return false
	}

}
