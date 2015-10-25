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

type JanusCoreSummarycVSW struct {
	Tables []JanusCoreSummarytable `json:"tables"`
}

type JanusCoreSummarytable struct {
	URL string                     `json:"url"`
	Row []JanusCoreSummaryjanusRow `json:"row"`
}

type JanusCoreSummaryjanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusCoreSummary `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusCoreSummary struct {
	Leg               int64           `json:"Leg"`
	Site              int64           `json:"Site"`
	Hole              string          `json:"Hole"`
	Core              int64           `json:"Core"`
	Core_type         string          `json:"Core_type"`
	Top_depth_mbsf    sql.NullFloat64 `json:"Top_depth_mbsf"`
	Length_cored      sql.NullFloat64 `json:"Length_cored"`
	Length_recovered  sql.NullFloat64 `json:"Length_recovered"`
	Percent_recovered sql.NullFloat64 `json:"Percent_recovered"`
	Curated_length    sql.NullFloat64 `json:"Curated_length"`
	Ship_date_time    sql.NullString  `json:"Ship_date_time"`
	Core_comment      sql.NullString  `json:"Core_comment"`
}

func JanusCoreSummaryModel() *JanusCoreSummary {
	return &JanusCoreSummary{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusCoreSummaryFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusCoreSummaryjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusCoreSummary{}
		var t JanusCoreSummary
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusCoreSummaryjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusCoreSummarytable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusCoreSummarytable{}
	tableSet = append(tableSet, theTable)
	final := JanusCoreSummarycVSW{tableSet}

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
