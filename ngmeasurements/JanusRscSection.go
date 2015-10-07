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
	Describes []JanusRscSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusRscSection struct {
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
    Run_number                     int64                `json:"Run_number"`
    Run_timestamp                  string               `json:"Run_timestamp"`
    Number_measured                int64                `json:"Number_measured"`
    Calibration_timestamp          string               `json:"Calibration_timestamp"`
    L_star                         float64              `json:"L_star"`
    A_star                         float64              `json:"A_star"`
    B_star                         float64              `json:"B_star"`
    Height                         float64              `json:"Height"`
    Height_assumed                 int64                `json:"Height_assumed"`
    Munsell_hvc                    string               `json:"Munsell_hvc"`
    Tristimulus_x                  float64              `json:"Tristimulus_x"`
    Tristimulus_y                  float64              `json:"Tristimulus_y"`
    Tristimulus_z                  float64              `json:"Tristimulus_z"`
    First_channel                  int64                `json:"First_channel"`
    Last_channel                   int64                `json:"Last_channel"`
    Channel_increment              int64                `json:"Channel_increment"`
    Spectral_values                string               `json:"Spectral_values"`

}

func JanusRscSectionModel() *JanusRscSection {
	return &JanusRscSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusRscSectionFunc(qry string, uri string, filename string, database string, collection string) error {

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
		d := []JanusRscSection{}
		var t JanusRscSection
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