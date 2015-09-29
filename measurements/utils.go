package measurements

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"io"
	"log"
	// "opencoredata.org/ocdJanus/metadata"
	"strconv"
)

type CVSW struct {
	Tables []Table `json:"tables"`
}

type Table struct {
	URL string     `json:"url"`
	Row []JanusRow `json:"row"`
}

type JanusRow struct {
	URL       string          `json:"url"`
	Rownum    int             `json:"rownum"`
	Describes []JanusRowItems `json:"describes"`
}

// make name generic
type JanusRowItems struct {
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

// webCSVBuilder   needs URI
// call as a passed function
func dumpJSON(rows *sql.Rows, out io.Writer) error {
	allResults := []JanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusRowItems{}
		var t JanusRowItems
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("http://example.org/countries.csv#row=%v", i)
		aRow := JanusRow{rowURL, i, d}
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

	// make metadata
	// metastruct := &JanusRowItems{}
	// log.Print(metadata.CSVMetadata(metastruct))
	// log.Print(metadata.SchemaOrgDataset(metastruct))

	return nil
}
