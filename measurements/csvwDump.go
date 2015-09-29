package measurements

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"opencoredata.org/ocdJanus/mongo"
)

// func UploadCSVToMongo(database string, collection string, URI string, data string) string {
func mongoCSVW(rows *sql.Rows, URI string, filename string) error {
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
			return nil
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

	mongo.UploadCSVToMongo("test", "jsonld", URI, filename, b.Bytes())

	return nil
}

// http://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
func dumpCSVW(rows *sql.Rows, out io.Writer) error {
	cols, err := rows.Columns()

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	writer := csv.NewWriter(out)
	writer.Comma = '\t'

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			fmt.Println("Failed to scan row", err)
			return nil
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "\\N"
			} else {
				result[i] = string(raw)
			}
		}

		// fmt.Printf("%#v\n", result)
		writer.Write(result)
	}
	writer.Flush()

	return nil
}
