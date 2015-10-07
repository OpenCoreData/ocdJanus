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
	Describes []JanusMsclSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusMsclSection struct {
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
    Normalized_susceptibility      float64              `json:"Normalized_susceptibility"`
    Magnetic_susceptibility        float64              `json:"Magnetic_susceptibility"`
    Run_number                     string               `json:"Run_number"`
    Run_timestamp                  string               `json:"Run_timestamp"`
    Integration_time_s             float64              `json:"Integration_time_s"`
    Number_cycles                  int64                `json:"Number_cycles"`
    System_id                      int64                `json:"System_id"`
    Lims_component_id              int64                `json:"Lims_component_id"`
    Freq_normalization_factor      float64              `json:"Freq_normalization_factor"`

}

func JanusMsclSectionModel() *JanusMsclSection {
	return &JanusMsclSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusMsclSectionFunc(qry string, uri string, filename string, database string, collection string) error {

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
		d := []JanusMsclSection{}
		var t JanusMsclSection
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