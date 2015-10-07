package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
)

type JanusPws2SectioncVSW struct {
	Tables []JanusPws2Sectiontable `json:"tables"`
}

type JanusPws2Sectiontable struct {
	URL string                     `json:"url"`
	Row []JanusPws2SectionjanusRow `json:"row"`
}

type JanusPws2SectionjanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusPws2Section `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPws2Section struct {
	Leg                      int64           `json:"Leg"`
	Site                     int64           `json:"Site"`
	Hole                     string          `json:"Hole"`
	Core                     int64           `json:"Core"`
	Core_type                string          `json:"Core_type"`
	Section_number           int64           `json:"Section_number"`
	Section_type             string          `json:"Section_type"`
	Top_cm                   float64         `json:"Top_cm"`
	Bot_cm                   float64         `json:"Bot_cm"`
	Depth_mbsf               float64         `json:"Depth_mbsf"`
	Section_id               int64           `json:"Section_id"`
	Direction                sql.NullString  `json:"Direction"`
	Velocity_m_s             float64         `json:"Velocity_m_s"`
	Run_number               int64           `json:"Run_number"`
	Run_timestamp            sql.NullString  `json:"Run_timestamp"`
	Core_temperature_c       sql.NullFloat64 `json:"Core_temperature_c"`
	Raw_data_collected       sql.NullString  `json:"Raw_data_collected"`
	Measurement_no           int64           `json:"Measurement_no"`
	Transducer_separation_mm sql.NullFloat64 `json:"Transducer_separation_mm"`
	Measured_time_us         sql.NullFloat64 `json:"Measured_time_us"`
	Calibration_timestamp    sql.NullString  `json:"Calibration_timestamp"`
	Calibration_delay        sql.NullFloat64 `json:"Calibration_delay"`
}

func JanusPws2SectionModel() *JanusPws2Section {
	return &JanusPws2Section{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPws2SectionFunc(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPws2SectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPws2Section{}
		var t JanusPws2Section
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPws2SectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPws2Sectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPws2Sectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusPws2SectioncVSW{tableSet}

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

	return nil
}
