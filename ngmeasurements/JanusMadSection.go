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

type JanusMadSectioncVSW struct {
	Tables []JanusMadSectiontable `json:"tables"`
}

type JanusMadSectiontable struct {
	URL string                    `json:"url"`
	Row []JanusMadSectionjanusRow `json:"row"`
}

type JanusMadSectionjanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusMadSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusMadSection struct {
	Leg                         int64           `json:"Leg"`
	Site                        int64           `json:"Site"`
	Hole                        string          `json:"Hole"`
	Core                        int64           `json:"Core"`
	Core_type                   string          `json:"Core_type"`
	Section_number              int64           `json:"Section_number"`
	Section_type                string          `json:"Section_type"`
	Top_cm                      float64         `json:"Top_cm"`
	Bot_cm                      float64         `json:"Bot_cm"`
	Depth_mbsf                  float64         `json:"Depth_mbsf"`
	Section_id                  int64           `json:"Section_id"`
	Calc_water_content_wet      sql.NullFloat64 `json:"Calc_water_content_wet"`
	Calc_water_content_dry      sql.NullFloat64 `json:"Calc_water_content_dry"`
	Calc_bulk_density           sql.NullFloat64 `json:"Calc_bulk_density"`
	Calc_dry_density            sql.NullFloat64 `json:"Calc_dry_density"`
	Calc_grain_density          sql.NullFloat64 `json:"Calc_grain_density"`
	Calc_porosity               sql.NullFloat64 `json:"Calc_porosity"`
	Calc_void_ratio             sql.NullFloat64 `json:"Calc_void_ratio"`
	Calc_method                 string          `json:"Calc_method"`
	Method                      string          `json:"Method"`
	Mad_comment                 sql.NullString  `json:"Mad_comment"`
	Beaker_date_time            sql.NullString  `json:"Beaker_date_time"`
	Sample_water_content_bulk   sql.NullFloat64 `json:"Sample_water_content_bulk"`
	Sample_water_content_solids sql.NullFloat64 `json:"Sample_water_content_solids"`
	Sample_bulk_density         sql.NullFloat64 `json:"Sample_bulk_density"`
	Sample_dry_density          sql.NullFloat64 `json:"Sample_dry_density"`
	Sample_grain_density        sql.NullFloat64 `json:"Sample_grain_density"`
	Sample_porosity             sql.NullFloat64 `json:"Sample_porosity"`
	Sample_void_ratio           sql.NullFloat64 `json:"Sample_void_ratio"`
}

func JanusMadSectionModel() *JanusMadSection {
	return &JanusMadSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusMadSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusMadSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusMadSection{}
		var t JanusMadSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusMadSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusMadSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusMadSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusMadSectioncVSW{tableSet}

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
