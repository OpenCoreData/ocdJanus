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

type JanusNcrSectioncVSW struct {
	Tables []JanusNcrSectiontable `json:"tables"`
}

type JanusNcrSectiontable struct {
	URL string                    `json:"url"`
	Row []JanusNcrSectionjanusRow `json:"row"`
}

type JanusNcrSectionjanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusNcrSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusNcrSection struct {
	Leg                    int64           `json:"Leg"`
	Site                   int64           `json:"Site"`
	Hole                   string          `json:"Hole"`
	Core                   int64           `json:"Core"`
	Core_type              string          `json:"Core_type"`
	Section_number         int64           `json:"Section_number"`
	Section_type           string          `json:"Section_type"`
	Top_cm                 float64         `json:"Top_cm"`
	Depth_mbsf             float64         `json:"Depth_mbsf"`
	Section_id             int64           `json:"Section_id"`
	Voltage                float64         `json:"Voltage"`
	Resistivity            sql.NullFloat64 `json:"Resistivity"`
	Run_number             int64           `json:"Run_number"`
	Run_timestamp          string          `json:"Run_timestamp"`
	Core_status            string          `json:"Core_status"`
	Liner_status           string          `json:"Liner_status"`
	Core_temperature       float64         `json:"Core_temperature"`
	Core_diameter          float64         `json:"Core_diameter"`
	Requested_daq_interval float64         `json:"Requested_daq_interval"`
	Requested_daq_period   float64         `json:"Requested_daq_period"`
	Actual_daq_period      float64         `json:"Actual_daq_period"`
}

func JanusNcrSectionModel() *JanusNcrSection {
	return &JanusNcrSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusNcrSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusNcrSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusNcrSection{}
		var t JanusNcrSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusNcrSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusNcrSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusNcrSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusNcrSectioncVSW{tableSet}

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
