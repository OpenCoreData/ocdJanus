package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
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
	Probe_id         int64          `json:"Probe_id"`
	Thermcon_comment sql.NullString `json:"Thermcon_comment"`
}

func JanusThermalConductivityModel() *JanusThermalConductivity {
	return &JanusThermalConductivity{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusThermalConductivityFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

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

	// session, err := mgo.Dial("127.0.0.1")
	// if err != nil {
	// 	panic(err)
	// }
	// defer  session.Close()

	// Optional. Switch the session to a Strong behavior.
	session.SetMode(mgo.Strong, true)
	c := session.DB(database).C(collection)

	err = c.Insert(&final)
	if err != nil {
		log.Fatalf("Error %v with %v\n", err, final)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
