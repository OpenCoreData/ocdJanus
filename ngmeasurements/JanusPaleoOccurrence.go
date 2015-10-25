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

type JanusPaleoOccurrencecVSW struct {
	Tables []JanusPaleoOccurrencetable `json:"tables"`
}

type JanusPaleoOccurrencetable struct {
	URL string                         `json:"url"`
	Row []JanusPaleoOccurrencejanusRow `json:"row"`
}

type JanusPaleoOccurrencejanusRow struct {
	URL       string                 `json:"url"`
	Rownum    int                    `json:"rownum"`
	Describes []JanusPaleoOccurrence `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPaleoOccurrence struct {
	Leg                       int64           `json:"Leg"`
	Site                      int64           `json:"Site"`
	Hole                      string          `json:"Hole"`
	Core                      int64           `json:"Core"`
	Core_type                 string          `json:"Core_type"`
	Section_number            int64           `json:"Section_number"`
	Section_type              string          `json:"Section_type"`
	Top_cm                    float64         `json:"Top_cm"`
	Bot_cm                    float64         `json:"Bot_cm"`
	Depth_mbsf                float64         `json:"Depth_mbsf"`
	Fossil_group              int64           `json:"Fossil_group"`
	Fossil_group_name         string          `json:"Fossil_group_name"`
	Taxon_id                  int64           `json:"Taxon_id"`
	Genus_subgenus            string          `json:"Genus_subgenus"`
	Species_subspecies        string          `json:"Species_subspecies"`
	Contaminated_reworked     sql.NullString  `json:"Contaminated_reworked"`
	Taxon_relative_abundance  sql.NullString  `json:"Taxon_relative_abundance"`
	Taxon_numeric_abundance   sql.NullInt64   `json:"Taxon_numeric_abundance"`
	Taxon_percent_abundance   sql.NullFloat64 `json:"Taxon_percent_abundance"`
	Presence_absence_flag     sql.NullString  `json:"Presence_absence_flag"`
	Sample_geologic_age_old   sql.NullString  `json:"Sample_geologic_age_old"`
	Sample_geologic_age_young sql.NullString  `json:"Sample_geologic_age_young"`
	Sample_zone_old           sql.NullString  `json:"Sample_zone_old"`
	Sample_zone_young         sql.NullString  `json:"Sample_zone_young"`
	Sample_group_abundance    sql.NullString  `json:"Sample_group_abundance"`
	Sample_preservation       sql.NullString  `json:"Sample_preservation"`
	Sample_id                 int64           `json:"Sample_id"`
	Location                  string          `json:"Location"`
	Post_cruise_flag          string          `json:"Post_cruise_flag"`
	Scientist_last_name       sql.NullString  `json:"Scientist_last_name"`
	Scientist_first_name      sql.NullString  `json:"Scientist_first_name"`
	Taxon_comment             sql.NullString  `json:"Taxon_comment"`
}

func JanusPaleoOccurrenceModel() *JanusPaleoOccurrence {
	return &JanusPaleoOccurrence{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPaleoOccurrenceFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPaleoOccurrencejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPaleoOccurrence{}
		var t JanusPaleoOccurrence
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPaleoOccurrencejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPaleoOccurrencetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPaleoOccurrencetable{}
	tableSet = append(tableSet, theTable)
	final := JanusPaleoOccurrencecVSW{tableSet}

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
