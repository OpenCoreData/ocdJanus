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

type JanusThermalConductivitycVSW struct {
	Tables []JanusThermalConductivitytable `json:"tables"`
}

type JanusThermalConductivitytable struct {
	URL string                             `json:"url"`
	Row []JanusThermalConductivityjanusRow `json:"row"`
}

type JanusThermalConductivityjanusRow struct {
	URL       string                     `json:"url"`
	Rownum    int                        `json:"rownum"`
	Describes []JanusThermalConductivity `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusThermalConductivity struct {
	Leg              int64          `json:"Leg"`
	Site             int64          `json:"Site"`
	Hole             string         `json:"Hole"`
	Core             int64          `json:"Core"`
	Core_type        string         `json:"Core_type"`
	Section_number   int64          `json:"Section_number"`
	Section_type     string         `json:"Section_type"`
	Top_cm           float64        `json:"Top_cm"`
	Bot_cm           float64        `json:"Bot_cm"`
	Depth_mbsf       float64        `json:"Depth_mbsf"`
	Probe_type       string         `json:"Probe_type"`
	Thermcon_value   float64        `json:"Thermcon_value"`
	System_id        sql.NullInt64  `json:"System_id"`
	Probe_number     int64          `json:"Probe_number"`
	Thermcon_comment sql.NullString `json:"Thermcon_comment"`
}

func JanusThermalConductivityModel() *JanusThermalConductivity {
	return &JanusThermalConductivity{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusThermalConductivityFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusThermalConductivityjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusThermalConductivity{}
		var t JanusThermalConductivity
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusThermalConductivityjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusThermalConductivitytable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusThermalConductivitytable{}
	tableSet = append(tableSet, theTable)
	final := JanusThermalConductivitycVSW{tableSet}

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
