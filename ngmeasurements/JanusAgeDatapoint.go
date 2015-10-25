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

type JanusAgeDatapointcVSW struct {
	Tables []JanusAgeDatapointtable `json:"tables"`
}

type JanusAgeDatapointtable struct {
	URL string                      `json:"url"`
	Row []JanusAgeDatapointjanusRow `json:"row"`
}

type JanusAgeDatapointjanusRow struct {
	URL       string              `json:"url"`
	Rownum    int                 `json:"rownum"`
	Describes []JanusAgeDatapoint `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusAgeDatapoint struct {
	Leg                   int64           `json:"Leg"`
	Site                  int64           `json:"Site"`
	Hole                  string          `json:"Hole"`
	Age_model_type        string          `json:"Age_model_type"`
	Depth_mbsf            float64         `json:"Depth_mbsf"`
	Age_ma                sql.NullFloat64 `json:"Age_ma"`
	Control_point_comment sql.NullString  `json:"Control_point_comment"`
}

func JanusAgeDatapointModel() *JanusAgeDatapoint {
	return &JanusAgeDatapoint{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusAgeDatapointFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusAgeDatapointjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusAgeDatapoint{}
		var t JanusAgeDatapoint
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusAgeDatapointjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusAgeDatapointtable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusAgeDatapointtable{}
	tableSet = append(tableSet, theTable)
	final := JanusAgeDatapointcVSW{tableSet}

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
