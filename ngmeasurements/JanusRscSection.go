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

type JanusRscSectioncVSW struct {
	Tables []JanusRscSectiontable `json:"tables"`
}

type JanusRscSectiontable struct {
	URL string                    `json:"url"`
	Row []JanusRscSectionjanusRow `json:"row"`
}

type JanusRscSectionjanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusRscSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusRscSection struct {
	Leg                   int64   `json:"Leg"`
	Site                  int64   `json:"Site"`
	Hole                  string  `json:"Hole"`
	Core                  int64   `json:"Core"`
	Core_type             string  `json:"Core_type"`
	Section_number        int64   `json:"Section_number"`
	Section_type          string  `json:"Section_type"`
	Top_cm                float64 `json:"Top_cm"`
	Depth_mbsf            float64 `json:"Depth_mbsf"`
	Section_id            int64   `json:"Section_id"`
	Run_number            int64   `json:"Run_number"`
	Run_timestamp         string  `json:"Run_timestamp"`
	Number_measured       int64   `json:"Number_measured"`
	Calibration_timestamp string  `json:"Calibration_timestamp"`
	L_star                float64 `json:"L_star"`
	A_star                float64 `json:"A_star"`
	B_star                float64 `json:"B_star"`
	Height                float64 `json:"Height"`
	Height_assumed        int64   `json:"Height_assumed"`
	Munsell_hvc           string  `json:"Munsell_hvc"`
	Tristimulus_x         float64 `json:"Tristimulus_x"`
	Tristimulus_y         float64 `json:"Tristimulus_y"`
	Tristimulus_z         float64 `json:"Tristimulus_z"`
	First_channel         int64   `json:"First_channel"`
	Last_channel          int64   `json:"Last_channel"`
	Channel_increment     int64   `json:"Channel_increment"`
	Spectral_values       string  `json:"Spectral_values"`
}

func JanusRscSectionModel() *JanusRscSection {
	return &JanusRscSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusRscSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusRscSectionjanusRow{}
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
		aRow := JanusRscSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusRscSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusRscSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusRscSectioncVSW{tableSet}

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
