package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
)

type JanusPwlSectioncVSW struct {
	Tables []JanusPwlSectiontable `json:"tables"`
}

type JanusPwlSectiontable struct {
	URL string                    `json:"url"`
	Row []JanusPwlSectionjanusRow `json:"row"`
}

type JanusPwlSectionjanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusPwlSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPwlSection struct {
	Leg                       int64           `json:"Leg"`
	Site                      int64           `json:"Site"`
	Hole                      string          `json:"Hole"`
	Core                      int64           `json:"Core"`
	Core_type                 string          `json:"Core_type"`
	Section_number            int64           `json:"Section_number"`
	Section_type              string          `json:"Section_type"`
	Top_cm                    float64         `json:"Top_cm"`
	Depth_mbsf                float64         `json:"Depth_mbsf"`
	Section_id                int64           `json:"Section_id"`
	Velocity_m_s              sql.NullFloat64 `json:"Velocity_m_s"`
	Run_number                string          `json:"Run_number"`
	Run_timestamp             sql.NullString  `json:"Run_timestamp"`
	Core_status               sql.NullString  `json:"Core_status"`
	Liner_status              sql.NullString  `json:"Liner_status"`
	Liner_correction          sql.NullString  `json:"Liner_correction"`
	Requested_interval_cm     sql.NullFloat64 `json:"Requested_interval_cm"`
	Requested_samples         sql.NullInt64   `json:"Requested_samples"`
	Acoustic_signal_threshold sql.NullFloat64 `json:"Acoustic_signal_threshold"`
	Core_temperature_c        sql.NullFloat64 `json:"Core_temperature_c"`
	Mean_separation_mm        sql.NullFloat64 `json:"Mean_separation_mm"`
	Stddev_separation         sql.NullInt64   `json:"Stddev_separation"`
	Mean_transit_time         sql.NullFloat64 `json:"Mean_transit_time"`
	Stddev_transit_time       sql.NullFloat64 `json:"Stddev_transit_time"`
	Mean_acoustic_signal      sql.NullFloat64 `json:"Mean_acoustic_signal"`
	Attempted_daqs            sql.NullInt64   `json:"Attempted_daqs"`
	Valid_daqs                sql.NullInt64   `json:"Valid_daqs"`
	Liner_thickness           sql.NullFloat64 `json:"Liner_thickness"`
	Standard_name             sql.NullString  `json:"Standard_name"`
	Standard_set_name         sql.NullString  `json:"Standard_set_name"`
	Expected_velocity_m_s     sql.NullFloat64 `json:"Expected_velocity_m_s"`
	Calib_timestamp           sql.NullString  `json:"Calibration_timestamp"`
	Calib_separation_m0       sql.NullFloat64 `json:"Calibration_separation_m0"`
	Calib_separation_m1       sql.NullFloat64 `json:"Calibration_separation_m1"`
	Calib_separation_mse      sql.NullFloat64 `json:"Calibration_separation_mse"`
	Calib_time_m0             sql.NullFloat64 `json:"Calibration_time_m0"`
	Calib_time_m1             sql.NullFloat64 `json:"Calibration_time_m1"`
	Calib_time_mse            sql.NullFloat64 `json:"Calibration_time_mse"`
}

func JanusPwlSectionModel() *JanusPwlSection {
	return &JanusPwlSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPwlSectionFunc(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPwlSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPwlSection{}
		var t JanusPwlSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPwlSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPwlSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPwlSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusPwlSectioncVSW{tableSet}

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

	conn.Close()
	return nil
}
