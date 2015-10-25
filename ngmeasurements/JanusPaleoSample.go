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
	Leg                  int64          `json:"Leg"`
	Site                 int64          `json:"Site"`
	Hole                 string         `json:"Hole"`
	Core                 int64          `json:"Core"`
	Core_type            string         `json:"Core_type"`
	Section_number       int64          `json:"Section_number"`
	Section_type         string         `json:"Section_type"`
	Top_cm               float64        `json:"Top_cm"`
	Bot_cm               float64        `json:"Bot_cm"`
	Depth_mbsf           float64        `json:"Depth_mbsf"`
	Sample_id            int64          `json:"Sample_id"`
	Location             string         `json:"Location"`
	Fossil_group         int64          `json:"Fossil_group"`
	Fossil_group_name    string         `json:"Fossil_group_name"`
	Geologic_age_old     sql.NullString `json:"Geologic_age_old"`
	Geologic_age_young   sql.NullString `json:"Geologic_age_young"`
	Zone_old             sql.NullString `json:"Zone_old"`
	Zone_young           sql.NullString `json:"Zone_young"`
	Group_abundance_name sql.NullString `json:"Group_abundance_name"`
	Preservation_name    sql.NullString `json:"Preservation_name"`
	Scientist_id         int64          `json:"Scientist_id"`
	Scientist_last_name  string         `json:"Scientist_last_name"`
	Scientist_first_name sql.NullString `json:"Scientist_first_name"`
	Paleo_sample_comment sql.NullString `json:"Paleo_sample_comment"`
}

func JanusPaleoSampleModel() *JanusPaleoSample {
	return &JanusPaleoSample{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPaleoSampleFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

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
