package ngmeasurements

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/utils"
)

type JanusChemCarbcVSW struct {
	Tables []JanusChemCarbtable `json:"tables"`
}

type JanusChemCarbtable struct {
	URL string                  `json:"url"`
	Row []JanusChemCarbjanusRow `json:"row"`
}

type JanusChemCarbjanusRow struct {
	URL       string          `json:"url"`
	Rownum    int             `json:"rownum"`
	Describes []JanusChemCarb `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusChemCarb struct {
	Leg            int64           `json:"Leg"`
	Site           int64           `json:"Site"`
	Hole           string          `json:"Hole"`
	Core           int64           `json:"Core"`
	Core_type      string          `json:"Core_type"`
	Section_number int64           `json:"Section_number"`
	Section_type   string          `json:"Section_type"`
	Top_cm         float64         `json:"Top_cm"`
	Bot_cm         float64         `json:"Bot_cm"`
	Depth_mbsf     float64         `json:"Depth_mbsf"`
	Inor_c_wt_pct  sql.NullFloat64 `json:"Inor_C_wt_pct"`
	Caco3_wt_pct   sql.NullFloat64 `json:"CacO3_wt_pct"`
	Tot_c_wt_pct   sql.NullFloat64 `json:"Tot_C_wt_pct"`
	Org_c_wt_pct   sql.NullFloat64 `json:"Org_C_wt_pct"`
	Nit_wt_pct     sql.NullFloat64 `json:"Nit_wt_pct"`
	Sul_wt_pct     sql.NullFloat64 `json:"Sul_wt_pct"`
	H_wt_pct       sql.NullFloat64 `json:"H_wt_pct"`
}

func JanusChemCarbModel() *JanusChemCarb {
	return &JanusChemCarb{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusChemCarbFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusChemCarbjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusChemCarb{}
		var t JanusChemCarb
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusChemCarbjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusChemCarbtable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusChemCarbtable{}
	tableSet = append(tableSet, theTable)
	final := JanusChemCarbcVSW{tableSet}

	// Optional. Switch the session to a Strong behavior.
	session.SetMode(mgo.Strong, true)
	c := session.DB(database).C(collection)

	err = c.Insert(&final)
	if err != nil {
		log.Printf("Janus func Error %v with %s\n", err, filename)
	}

	log.Printf("File: %s  written", filename)

	jm, _ := json.MarshalIndent(final, "", " ")
	_ = utils.WriteFile(filename, jm)

	// session.Close()
	return nil
}
