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

type JanusPws1SectioncVSW struct {
	Tables []JanusPws1Sectiontable `json:"tables"`
}

type JanusPws1Sectiontable struct {
	URL string                     `json:"url"`
	Row []JanusPws1SectionjanusRow `json:"row"`
}

type JanusPws1SectionjanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusPws1Section `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPws1Section struct {
	Leg                   int64           `json:"Leg"`
	Site                  int64           `json:"Site"`
	Hole                  string          `json:"Hole"`
	Core                  int64           `json:"Core"`
	Core_type             string          `json:"Core_type"`
	Section_number        int64           `json:"Section_number"`
	Section_type          string          `json:"Section_type"`
	Top_cm                float64         `json:"Top_cm"`
	Bot_cm                float64         `json:"Bot_cm"`
	Depth_mbsf            float64         `json:"Depth_mbsf"`
	Section_id            int64           `json:"Section_id"`
	Direction             sql.NullString  `json:"Direction"`
	Velocity              float64         `json:"Velocity"`
	Run_number            int64           `json:"Run_number"`
	Run_timestamp         sql.NullString  `json:"Run_timestamp"`
	Core_temperature      sql.NullFloat64 `json:"Core_temperature"`
	Raw_data_collected    sql.NullString  `json:"Raw_data_collected"`
	Measurement_no        int64           `json:"Measurement_no"`
	Transducer_separation sql.NullFloat64 `json:"Transducer_separation"`
	Measured_time         sql.NullFloat64 `json:"Measured_time"`
	Calibration_timestamp sql.NullString  `json:"Calibration_timestamp"`
	Calib_delay           sql.NullFloat64 `json:"Calib_delay"`
}

func JanusPws1SectionModel() *JanusPws1Section {
	return &JanusPws1Section{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPws1SectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPws1SectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPws1Section{}
		var t JanusPws1Section
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPws1SectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPws1Sectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPws1Sectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusPws1SectioncVSW{tableSet}

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
