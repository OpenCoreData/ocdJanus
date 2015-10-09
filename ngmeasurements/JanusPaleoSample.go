package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusPaleoSamplecVSW struct {
	Tables []JanusPaleoSampletable `json:"tables"`
}

type JanusPaleoSampletable struct {
	URL string                     `json:"url"`
	Row []JanusPaleoSamplejanusRow `json:"row"`
}

type JanusPaleoSamplejanusRow struct {
	URL       string             `json:"url"`
	Rownum    int                `json:"rownum"`
	Describes []JanusPaleoSample `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPaleoSample struct {
	Leg                     int64          `json:"Leg"`
	Site                    int64          `json:"Site"`
	Hole                    string         `json:"Hole"`
	Core                    int64          `json:"Core"`
	Core_type               string         `json:"Core_type"`
	Section_number          int64          `json:"Section_number"`
	Section_type            string         `json:"Section_type"`
	Top_cm                  float64        `json:"Top_cm"`
	Bot_cm                  float64        `json:"Bot_cm"`
	Depth_mbsf              float64        `json:"Depth_mbsf"`
	Sample_id               int64          `json:"Sample_id"`
	Location                string         `json:"Location"`
	Fossil_group            int64          `json:"Fossil_group"`
	Fossil_group_name       string         `json:"Fossil_group_name"`
	Geologic_age_name_old   sql.NullString `json:"Geologic_age_name_old"`
	Geologic_age_name_young sql.NullString `json:"Geologic_age_name_young"`
	Zone_abbrev_bottom      sql.NullString `json:"Zone_abbrev_bottom"`
	Zone_abbrev_top         sql.NullString `json:"Zone_abbrev_top"`
	Group_abundance_name    sql.NullString `json:"Group_abundance_name"`
	Preservation_name       sql.NullString `json:"Preservation_name"`
	Scientist_id            int64          `json:"Scientist_id"`
	Last_name               string         `json:"Last_name"`
	First_name              sql.NullString `json:"First_name"`
	Paleo_sample_comment    sql.NullString `json:"Paleo_sample_comment"`
}

func JanusPaleoSampleModel() *JanusPaleoSample {
	return &JanusPaleoSample{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPaleoSampleFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPaleoSamplejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPaleoSample{}
		var t JanusPaleoSample
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPaleoSamplejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPaleoSampletable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPaleoSampletable{}
	tableSet = append(tableSet, theTable)
	final := JanusPaleoSamplecVSW{tableSet}

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

	session.Close()
	return nil
}
