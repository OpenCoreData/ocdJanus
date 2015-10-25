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

type JanusPws3SectioncVSW struct {
	Tables []JanusPws3Sectiontable `json:"tables"`
}

type JanusPws3Sectiontable struct {
	URL string                     `json:"url"`
	Row []JanusPws3SectionjanusRow `json:"row"`
}

type JanusPws3SectionjanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusPws3Section `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPws3Section struct {
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
	Velocity              sql.NullFloat64 `json:"Velocity"`
	Run_number            int64           `json:"Run_number"`
	Run_timestamp         sql.NullString  `json:"Run_timestamp"`
	Core_temperature      sql.NullFloat64 `json:"Core_temperature"`
	Liner_correction      sql.NullString  `json:"Liner_correction"`
	Raw_data_collected    sql.NullString  `json:"Raw_data_collected"`
	Standard_name         sql.NullString  `json:"Standard_name"`
	Standard_set_name     sql.NullString  `json:"Standard_set_name"`
	Expected_velocity     sql.NullFloat64 `json:"Expected_velocity"`
	Measurement_no        sql.NullInt64   `json:"Measurement_no"`
	Meas_separation_mean  sql.NullFloat64 `json:"Meas_separation_mean"`
	Meas_time_mean        sql.NullFloat64 `json:"Meas_time_mean"`
	Contact_pressure      sql.NullFloat64 `json:"Contact_pressure"`
	Liner_thickness       sql.NullFloat64 `json:"Liner_thickness"`
	Calibration_timestamp sql.NullString  `json:"Calibration_timestamp"`
	Calib_separation_m0   sql.NullFloat64 `json:"Calibration_separation_m0"`
	Calib_separation_m1   sql.NullFloat64 `json:"Calibration_separation_m1"`
	Calib_separation_mse  sql.NullFloat64 `json:"Calibration_separation_mse"`
	Calib_time_m0         sql.NullFloat64 `json:"Calibration_time_m0"`
	Calib_time_m1         sql.NullFloat64 `json:"Calibration_time_m1"`
	Calib_time_mse        sql.NullFloat64 `json:"Calibration_time_mse"`
}

func JanusPws3SectionModel() *JanusPws3Section {
	return &JanusPws3Section{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPws3SectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPws3SectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPws3Section{}
		var t JanusPws3Section
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPws3SectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPws3Sectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPws3Sectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusPws3SectioncVSW{tableSet}

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
