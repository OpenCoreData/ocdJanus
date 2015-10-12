package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusGraSectioncVSW struct {
	Tables []JanusGraSectiontable `json:"tables"`
}

type JanusGraSectiontable struct {
	URL string                    `json:"url"`
	Row []JanusGraSectionjanusRow `json:"row"`
}

type JanusGraSectionjanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusGraSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusGraSection struct {
	Leg                        int64           `json:"Leg"`
	Site                       int64           `json:"Site"`
	Hole                       string          `json:"Hole"`
	Core                       int64           `json:"Core"`
	Core_type                  string          `json:"Core_type"`
	Section_number             int64           `json:"Section_number"`
	Section_type               string          `json:"Section_type"`
	Top_cm                     float64         `json:"Top_cm"`
	Depth_mbsf                 float64         `json:"Depth_mbsf"`
	Section_id                 int64           `json:"Section_id"`
	Density_g_cc               sql.NullFloat64 `json:"Density_g_cc"`
	Run_number                 string          `json:"Run_number"`
	Run_timestamp              string          `json:"Run_timestamp"`
	Core_status                string          `json:"Core_status"`
	Liner_status               string          `json:"Liner_status"`
	Requested_daq_interval_cm  sql.NullFloat64 `json:"Requested_daq_interval_cm"`
	Requested_daq_interval_sec sql.NullFloat64 `json:"Requested_daq_interval_sec"`
	Actual_daq_period_sec      float64         `json:"Actual_daq_period_sec"`
	Measured_counts            int64           `json:"Measured_counts"`
	Core_diameter              float64         `json:"Core_diameter"`
	Calibration_timestamp      string          `json:"Calibration_timestamp"`
	Calibration_intercept      float64         `json:"Calibration_intercept"`
	Calibration_slope          float64         `json:"Calibration_slope"`
}

func JanusGraSectionModel() *JanusGraSection {
	return &JanusGraSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusGraSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusGraSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusGraSection{}
		var t JanusGraSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusGraSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusGraSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusGraSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusGraSectioncVSW{tableSet}

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
		log.Printf("Janus func Error %v with %s\n", err, final, filename)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
