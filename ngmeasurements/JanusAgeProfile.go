package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
)

type JanusAgeProfilecVSW struct {
	Tables []JanusAgeProfiletable `json:"tables"`
}

type JanusAgeProfiletable struct {
	URL string     `json:"url"`
	Row []JanusAgeProfilejanusRow `json:"row"`
}

type JanusAgeProfilejanusRow struct {
	URL       string           `json:"url"`
	Rownum    int              `json:"rownum"`
	Describes []JanusAgeProfile `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusAgeProfile struct {
    Leg                            int64                `json:"Leg"`
    Site                           int64                `json:"Site"`
    Hole                           string               `json:"Hole"`
    Datum_fossil_group             int64                `json:"Datum_fossil_group"`
    Fossil_group_name              string               `json:"Fossil_group_name"`
    Datum_depth_top                float64              `json:"Datum_depth_top"`
    Datum_depth_base               sql.NullFloat64      `json:"Datum_depth_base"`
    Datum_age_young                float64              `json:"Datum_age_young"`
    Datum_age_old                  sql.NullFloat64      `json:"Datum_age_old"`
    Datum_id                       int64                `json:"Datum_id"`
    Datum_type                     string               `json:"Datum_type"`
    Datum_description              sql.NullString       `json:"Datum_description"`
    Genus_subgenus                 string               `json:"Genus_subgenus"`
    Species_subspecies             string               `json:"Species_subspecies"`
    Depth_map_type                 sql.NullString       `json:"Depth_map_type"`
    Mcd_flag                       sql.NullString       `json:"Mcd_flag"`
    Compression_flag               sql.NullString       `json:"Compression_flag"`

}

func JanusAgeProfileModel() *JanusAgeProfile {
	return &JanusAgeProfile{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusAgeProfileFunc(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

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