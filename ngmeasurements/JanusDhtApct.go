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

type JanusDhtApctcVSW struct {
	Tables []JanusDhtApcttable `json:"tables"`
}

type JanusDhtApcttable struct {
	URL string                 `json:"url"`
	Row []JanusDhtApctjanusRow `json:"row"`
}

type JanusDhtApctjanusRow struct {
	URL       string         `json:"url"`
	Rownum    int            `json:"rownum"`
	Describes []JanusDhtApct `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusDhtApct struct {
	Leg                  int64           `json:"Leg"`
	Site                 int64           `json:"Site"`
	Hole                 string          `json:"Hole"`
	Core                 int64           `json:"Core"`
	Core_type            string          `json:"Core_type"`
	Top_depth_mbsf       float64         `json:"Top_depth_mbsf"`
	Bot_depth_mbsf       float64         `json:"Bot_depth_mbsf"`
	Run_number           int64           `json:"Run_number"`
	Dht_comment          sql.NullFloat64 `json:"Dht_comment"`
	Best_fit_temperature sql.NullFloat64 `json:"Best_fit_temperature"`
	Best_fit_error       sql.NullFloat64 `json:"Best_fit_error"`
	Mudline_temperature  sql.NullFloat64 `json:"Mudline_temperature"`
	Tool_name            string          `json:"Tool_name"`
	Run_comment          sql.NullString  `json:"Run_comment"`
}

func JanusDhtApctModel() *JanusDhtApct {
	return &JanusDhtApct{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusDhtApctFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusDhtApctjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusDhtApct{}
		var t JanusDhtApct
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusDhtApctjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusDhtApcttable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusDhtApcttable{}
	tableSet = append(tableSet, theTable)
	final := JanusDhtApctcVSW{tableSet}

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
