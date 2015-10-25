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

type JanusAgeProfilecVSW struct {
	Tables []JanusAgeProfiletable `json:"tables"`
}

type JanusAgeProfiletable struct {
	URL string                    `json:"url"`
	Row []JanusAgeProfilejanusRow `json:"row"`
}

type JanusAgeProfilejanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusAgeProfile `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusAgeProfile struct {
	Leg                    int64           `json:"Leg"`
	Site                   int64           `json:"Site"`
	Hole                   string          `json:"Hole"`
	Datum_fossil_group     int64           `json:"Datum_fossil_group"`
	Fossil_group_name      string          `json:"Fossil_group_name"`
	Datum_depth            float64         `json:"Datum_depth"`
	Datum_depth_error      sql.NullFloat64 `json:"Datum_depth_error"`
	Datum_age              float64         `json:"Datum_age"`
	Datum_age_error        sql.NullFloat64 `json:"Datum_age_error"`
	Datum_id               int64           `json:"Datum_id"`
	Datum_type             string          `json:"Datum_type"`
	Datum_description      sql.NullString  `json:"Datum_description"`
	Taxon_id               int64           `json:"Taxon_id"`
	Genus_subgenus         string          `json:"Genus_subgenus"`
	Species_subspecies     string          `json:"Species_subspecies"`
	Datum_sample_id        int64           `json:"Datum_sample_id"`
	Datum_location         string          `json:"Datum_location"`
	Datum_post_cruise_flag string          `json:"Datum_post_cruise_flag"`
	Timescale_author_year  string          `json:"Timescale_author_year"`
	Datum_comment          sql.NullString  `json:"Datum_comment"`
}

func JanusAgeProfileModel() *JanusAgeProfile {
	return &JanusAgeProfile{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusAgeProfileFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusAgeProfilejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusAgeProfile{}
		var t JanusAgeProfile
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusAgeProfilejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusAgeProfiletable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusAgeProfiletable{}
	tableSet = append(tableSet, theTable)
	final := JanusAgeProfilecVSW{tableSet}

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
