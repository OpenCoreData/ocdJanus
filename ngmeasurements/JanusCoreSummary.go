package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
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
func JanusCoreSummaryFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

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

	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(database).C(collection)

	err = c.Insert(&final)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("File: %s  written", filename)

	session.Close()
	return nil
}
