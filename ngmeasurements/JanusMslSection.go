package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
)

type cVSW struct {
	Tables []table `json:"tables"`
}

type table struct {
	URL string     `json:"url"`
	Row []janusRow `json:"row"`
}

type janusRow struct {
	URL       string           `json:"url"`
	Rownum    int              `json:"rownum"`
	Describes []JanusMslSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusMslSection struct {
    Leg                            int64                `json:"Leg"`
    Site                           int64                `json:"Site"`
    Hole                           string               `json:"Hole"`
    Core                           int64                `json:"Core"`
    Core_type                      string               `json:"Core_type"`
    Section_number                 int64                `json:"Section_number"`
    Section_type                   string               `json:"Section_type"`
    Top_cm                         float64              `json:"Top_cm"`
    Depth_mbsf                     float64              `json:"Depth_mbsf"`
    Section_id                     int64                `json:"Section_id"`
    Magnetic_susceptibility        float64              `json:"Magnetic_susceptibility"`
    Corrected_susceptibility       float64              `json:"Corrected_susceptibility"`
    Run_number                     string               `json:"Run_number"`
    Run_timestamp                  string               `json:"Run_timestamp"`
    Core_status                    sql.NullString       `json:"Core_status"`
    Liner_status                   sql.NullString       `json:"Liner_status"`
    Requested_daq_interval         sql.NullFloat64      `json:"Requested_daq_interval"`
    Requested_daqs                 sql.NullInt64        `json:"Requested_daqs"`
    Bkgd_susceptibility            sql.NullFloat64      `json:"Bkgd_susceptibility"`
    Bkgd_elapsed_zero_time         sql.NullInt64        `json:"Bkgd_elapsed_zero_time"`
    Core_temperature               sql.NullFloat64      `json:"Core_temperature"`
    Loop_temperature               sql.NullFloat64      `json:"Loop_temperature"`
    Sample_elapsed_zero_time       sql.NullInt64        `json:"Sample_elapsed_zero_time"`
    Actual_daq_period              sql.NullFloat64      `json:"Actual_daq_period"`
    Core_diameter                  sql.NullFloat64      `json:"Core_diameter"`

}

func JanusMslSectionModel() *JanusMslSection {
	return &JanusMslSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusMslSectionFunc(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []janusRow{}
	i := 1
	for rows.Next() {
		d := []JanusMslSection{}
		var t JanusMslSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := janusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := table{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []table{}
	tableSet = append(tableSet, theTable)
	final := cVSW{tableSet}

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