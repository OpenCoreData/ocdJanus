package utils

import (
	"database/sql"
	"log"
)

func DataCheck(qry string, conn *sql.DB) bool {
	// conn, err := connect.GetJanusCon()
	// if err != nil {
	// 	panic(err)
	// }
	// defer conn.Close()

	rowscheck, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}
	len := 0
	for rowscheck.Next() {
		len = len + 1
	}

	// conn.Close()

	if len > 0 {
		return true
	} else {
		return false
	}

}
