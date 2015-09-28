package measurements

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"io"
	"log"
	"strconv"
)

type CVSW struct {
	Tables []Table `json:"tables"`
}

type Table struct {
	URL string                 `json:"url"`
	Row []CVSWJanusChemCarbRow `json:"row"`
}

type CVSWJanusChemCarbRow struct {
	URL       string                      `json:"url"`
	Rownum    int                         `json:"rownum"`
	Describes []CVSWJanusChemCarbRowItems `json:"describes"`
}

type CVSWJanusChemCarbRowItems struct {
	Leg            int         `json:"Leg"`
	Site           int         `json:"Site"`
	Hole           string      `json:"Hole"`
	Core           int         `json:"Core"`
	Core_type      string      `json:"Core_type"`
	Section_number int         `json:"Section_number"`
	Section_type   string      `json:"Section_type"`
	Top_cm         NullFloat64 `json:"Top_cm"`
	Bot_cm         NullFloat64 `json:"Bot_cm"`
	Depth_mbsf     NullFloat64 `json:"Depth_mbsf"`
	Inor_c_wt_pct  NullFloat64 `json:"Inor_c_wt_pct"`
	Caco3_wt_pct   NullFloat64 `json:"Caco3_wt_pct"`
	Tot_c_wt_pct   NullFloat64 `json:"Tot_c_wt_pct"`
	Org_c_wt_pct   NullFloat64 `json:"Org_c_wt_pct"`
	Nit_wt_pct     NullFloat64 `json:"Nit_wt_pct"`
	Sul_wt_pct     NullFloat64 `json:"Sul_wt_pct"`
	H_wt_pct       NullFloat64 `json:"H_wt_pct"`
}

type NullFloat64 struct {
	sql.NullFloat64
}

// func (nf NullFloat64) MarshalText() []byte {
// 	if nf.Valid {
// 		log.Printf("Hello/n")
// 		nfv := nf.Float64
// 		return []byte(strconv.FormatFloat(nfv, 'f', -1, 64))
// 	} else {
// 		return []byte("null")
// 	}
// }

func (nf NullFloat64) MarshalText() ([]byte, error) {
	if nf.Valid {
		nfv := nf.Float64
		return []byte(strconv.FormatFloat(nfv, 'f', -1, 64)), nil
	} else {
		return []byte("null"), nil
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// webCSVBuilder
// needs URI
func dumpJSON(rows *sql.Rows, out io.Writer) error {
	allResults := []CVSWJanusChemCarbRow{}
	i := 1
	for rows.Next() {
		d := []CVSWJanusChemCarbRowItems{}
		var t CVSWJanusChemCarbRowItems
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("http://example.org/countries.csv#row=%v", i)
		aRow := CVSWJanusChemCarbRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := Table{"http://example.org/countries.csv", allResults}
	tableSet := []Table{}
	tableSet = append(tableSet, theTable)
	final := CVSW{tableSet}

	res2B, _ := json.MarshalIndent(final, "", " ")

	_, err := out.Write(res2B)
	check(err)

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
