package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusNgrSectioncVSW struct {
	Tables []JanusNgrSectiontable `json:"tables"`
}

type JanusNgrSectiontable struct {
	URL string                    `json:"url"`
	Row []JanusNgrSectionjanusRow `json:"row"`
}

type JanusNgrSectionjanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusNgrSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusNgrSection struct {
	Leg                         int64           `json:"Leg"`
	Site                        int64           `json:"Site"`
	Hole                        string          `json:"Hole"`
	Core                        int64           `json:"Core"`
	Core_type                   string          `json:"Core_type"`
	Section_number              int64           `json:"Section_number"`
	Section_type                string          `json:"Section_type"`
	Top_cm                      float64         `json:"Top_cm"`
	Depth_mbsf                  float64         `json:"Depth_mbsf"`
	Section_id                  int64           `json:"Section_id"`
	Corrected_total_counts_sec  sql.NullFloat64 `json:"Corrected_total_counts_sec"`
	Run_number                  string          `json:"Run_number"`
	Run_timestamp               sql.NullString  `json:"Run_timestamp"`
	Core_status                 sql.NullString  `json:"Core_status"`
	Liner_status                sql.NullString  `json:"Liner_status"`
	Requested_daq_interval_cm   sql.NullFloat64 `json:"Requested_daq_interval_cm"`
	Requested_daq_interval_sec  sql.NullFloat64 `json:"Requested_daq_interval_sec"`
	Actual_daq_period_sec       sql.NullFloat64 `json:"Actual_daq_period_sec"`
	Core_diameter               sql.NullFloat64 `json:"Core_diameter"`
	Total_counts_sec            float64         `json:"Total_counts_sec"`
	Calibration_timestamp       sql.NullString  `json:"Calibration_timestamp"`
	Calibration_intercept       sql.NullFloat64 `json:"Calibration_intercept"`
	Calibration_slope           sql.NullFloat64 `json:"Calibration_slope"`
	Calibration_mse             sql.NullFloat64 `json:"Calibration_mse"`
	Background_total_counts_sec sql.NullFloat64 `json:"Background_total_counts_sec"`
}

func JanusNgrSectionModel() *JanusNgrSection {
	return &JanusNgrSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusNgrSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusNgrSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusNgrSection{}
		var t JanusNgrSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusNgrSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusNgrSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusNgrSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusNgrSectioncVSW{tableSet}

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
		log.Printf("Janus func Error %v with %s\n", err, filename)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
