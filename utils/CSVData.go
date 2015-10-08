package utils

import (
	"bytes"
	// "database/sql"
	"encoding/csv"
	"opencoredata.org/ocdJanus/connect"

	// "fmt"
	// "io"
	"log"
	// "opencoredata.org/ocdJanus/mongo"
)

func CSVData(qry string) []byte {
	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	cols, err := rows.Columns()

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	b := &bytes.Buffer{}
	writer := csv.NewWriter(b)
	writer.Comma = '\t'

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			log.Println("Failed to scan row", err)
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}
		writer.Write(result)
	}
	writer.Flush()

	conn.Close()

	return b.Bytes()
}
