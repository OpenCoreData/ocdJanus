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

type JanusMsclSectioncVSW struct {
	Tables []JanusMsclSectiontable `json:"tables"`
}

type JanusMsclSectiontable struct {
	URL string                     `json:"url"`
	Row []JanusMsclSectionjanusRow `json:"row"`
}

type JanusMsclSectionjanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusMsclSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusMsclSection struct {
	Leg                       int64   `json:"Leg"`
	Site                      int64   `json:"Site"`
	Hole                      string  `json:"Hole"`
	Core                      int64   `json:"Core"`
	Core_type                 string  `json:"Core_type"`
	Section_number            int64   `json:"Section_number"`
	Section_type              string  `json:"Section_type"`
	Top_cm                    float64 `json:"Top_cm"`
	Depth_mbsf                float64 `json:"Depth_mbsf"`
	Section_id                int64   `json:"Section_id"`
	Normalized_susceptibility float64 `json:"Normalized_susceptibility"`
	Magnetic_susceptibility   float64 `json:"Magnetic_susceptibility"`
	Run_number                string  `json:"Run_number"`
	Run_timestamp             string  `json:"Run_timestamp"`
	Integration_time          float64 `json:"Integration_time"`
	Number_cycles             int64   `json:"Number_cycles"`
	System_id                 int64   `json:"System_id"`
	Lims_component_id         int64   `json:"Lims_component_id"`
	Freq_normalization_factor float64 `json:"Freq_normalization_factor"`
}

func JanusMsclSectionModel() *JanusMsclSection {
	return &JanusMsclSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusMsclSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusMsclSectionjanusRow{}
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
		aRow := JanusMsclSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusMsclSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusMsclSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusMsclSectioncVSW{tableSet}

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
