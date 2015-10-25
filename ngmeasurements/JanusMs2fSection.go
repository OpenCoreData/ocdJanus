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

type JanusMs2fSectioncVSW struct {
	Tables []JanusMs2fSectiontable `json:"tables"`
}

type JanusMs2fSectiontable struct {
	URL string                     `json:"url"`
	Row []JanusMs2fSectionjanusRow `json:"row"`
}

type JanusMs2fSectionjanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusMs2fSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusMs2fSection struct {
	Leg                         int64          `json:"Leg"`
	Site                        int64          `json:"Site"`
	Hole                        string         `json:"Hole"`
	Core                        int64          `json:"Core"`
	Core_type                   string         `json:"Core_type"`
	Section_number              int64          `json:"Section_number"`
	Section_type                string         `json:"Section_type"`
	Top_cm                      float64        `json:"Top_cm"`
	Depth_mbsf                  float64        `json:"Depth_mbsf"`
	Section_id                  int64          `json:"Section_id"`
	Magnetic_susceptibility     float64        `json:"Magnetic_susceptibility"`
	Corrected_susceptibility    sql.NullString `json:"Corrected_susceptibility"`
	Run_number                  string         `json:"Run_number"`
	Run_timestamp               string         `json:"Run_timestamp"`
	Avg_magnetic_susceptibility float64        `json:"Avg_magnetic_susceptibility"`
	Core_temperature            float64        `json:"Core_temperature"`
	Probe_temperature           float64        `json:"Probe_temperature"`
	Drift_corr_susceptibility   float64        `json:"Drift_corr_susceptibility"`
	Actual_daq_period           float64        `json:"Actual_daq_period"`
	Offset                      float64        `json:"Offset"`
}

func JanusMs2fSectionModel() *JanusMs2fSection {
	return &JanusMs2fSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusMs2fSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusMs2fSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusMs2fSection{}
		var t JanusMs2fSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusMs2fSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusMs2fSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusMs2fSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusMs2fSectioncVSW{tableSet}

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
