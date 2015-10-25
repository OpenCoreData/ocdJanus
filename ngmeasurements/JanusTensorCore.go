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

type JanusTensorCorecVSW struct {
	Tables []JanusTensorCoretable `json:"tables"`
}

type JanusTensorCoretable struct {
	URL string                    `json:"url"`
	Row []JanusTensorCorejanusRow `json:"row"`
}

type JanusTensorCorejanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusTensorCore `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusTensorCore struct {
	Leg              int64           `json:"Leg"`
	Site             int64           `json:"Site"`
	Hole             string          `json:"Hole"`
	Core             int64           `json:"Core"`
	Core_type        string          `json:"Core_type"`
	Top_depth_mbsf   float64         `json:"Top_depth_mbsf"`
	Hole_azimuth     float64         `json:"Hole_azimuth"`
	Hole_inclination float64         `json:"Hole_inclination"`
	Angle_motf       float64         `json:"Angle_motf"`
	Angle_mtf        float64         `json:"Angle_mtf"`
	Site_variation   sql.NullFloat64 `json:"Site_variation"`
}

func JanusTensorCoreModel() *JanusTensorCore {
	return &JanusTensorCore{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusTensorCoreFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusTensorCorejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusTensorCore{}
		var t JanusTensorCore
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusTensorCorejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusTensorCoretable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusTensorCoretable{}
	tableSet = append(tableSet, theTable)
	final := JanusTensorCorecVSW{tableSet}

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
