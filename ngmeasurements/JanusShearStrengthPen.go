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

type JanusShearStrengthPencVSW struct {
	Tables []JanusShearStrengthPentable `json:"tables"`
}

type JanusShearStrengthPentable struct {
	URL string                          `json:"url"`
	Row []JanusShearStrengthPenjanusRow `json:"row"`
}

type JanusShearStrengthPenjanusRow struct {
	URL       string                  `json:"url"`
	Rownum    int                     `json:"rownum"`
	Describes []JanusShearStrengthPen `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusShearStrengthPen struct {
	Leg              int64           `json:"Leg"`
	Site             int64           `json:"Site"`
	Hole             string          `json:"Hole"`
	Core             int64           `json:"Core"`
	Core_type        string          `json:"Core_type"`
	Section_number   int64           `json:"Section_number"`
	Section_type     string          `json:"Section_type"`
	Top_cm           float64         `json:"Top_cm"`
	Bot_cm           float64         `json:"Bot_cm"`
	Depth_mbsf       float64         `json:"Depth_mbsf"`
	Section_id       int64           `json:"Section_id"`
	Strength_reading float64         `json:"Strength_reading"`
	Run_timestamp    sql.NullString  `json:"Run_timestamp"`
	Measurement_no   int64           `json:"Measurement_no"`
	Sys_id           string          `json:"Sys_id"`
	Direction        sql.NullString  `json:"Direction"`
	Core_temperature sql.NullFloat64 `json:"Core_temperature"`
	Adapter_used     sql.NullString  `json:"Adapter_used"`
	Section_comment  sql.NullString  `json:"Section_comment"`
	Sample_comment   sql.NullString  `json:"Sample_comment"`
}

func JanusShearStrengthPenModel() *JanusShearStrengthPen {
	return &JanusShearStrengthPen{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusShearStrengthPenFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusShearStrengthPenjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusShearStrengthPen{}
		var t JanusShearStrengthPen
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusShearStrengthPenjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusShearStrengthPentable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusShearStrengthPentable{}
	tableSet = append(tableSet, theTable)
	final := JanusShearStrengthPencVSW{tableSet}

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
