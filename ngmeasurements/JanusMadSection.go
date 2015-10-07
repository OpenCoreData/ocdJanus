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
	Describes []JanusMadSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusMadSection struct {
    Leg                            int64                `json:"Leg"`
    Site                           int64                `json:"Site"`
    Hole                           string               `json:"Hole"`
    Core                           int64                `json:"Core"`
    Core_type                      string               `json:"Core_type"`
    Section_number                 int64                `json:"Section_number"`
    Section_type                   string               `json:"Section_type"`
    Top_cm                         float64              `json:"Top_cm"`
    Bot_cm                         float64              `json:"Bot_cm"`
    Depth_mbsf                     float64              `json:"Depth_mbsf"`
    Sam_section_id                 int64                `json:"Sam_section_id"`
    Calc_water_content_wet_pct     sql.NullFloat64      `json:"Calc_water_content_wet_pct"`
    Calc_water_content_dry_pct     sql.NullFloat64      `json:"Calc_water_content_dry_pct"`
    Calc_bulk_density_g_cc         sql.NullFloat64      `json:"Calc_bulk_density_g_cc"`
    Calc_dry_density_g_cc          sql.NullFloat64      `json:"Calc_dry_density_g_cc"`
    Calc_grain_density_g_cc        sql.NullFloat64      `json:"Calc_grain_density_g_cc"`
    Calc_porosity_pct              sql.NullFloat64      `json:"Calc_porosity_pct"`
    Calc_void_ratio                sql.NullFloat64      `json:"Calc_void_ratio"`
    Calc_method                    string               `json:"Calc_method"`
    Method                         string               `json:"Method"`
    Comments                       sql.NullString       `json:"Comments"`
    Beaker_date_time               sql.NullString       `json:"Beaker_date_time"`
    Sample_water_content_bulk      sql.NullFloat64      `json:"Sample_water_content_bulk"`
    Sample_water_content_solids    sql.NullFloat64      `json:"Sample_water_content_solids"`
    Sample_bulk_density            sql.NullFloat64      `json:"Sample_bulk_density"`
    Sample_dry_density             sql.NullFloat64      `json:"Sample_dry_density"`
    Sample_grain_density           sql.NullFloat64      `json:"Sample_grain_density"`
    Sample_porosity                sql.NullFloat64      `json:"Sample_porosity"`
    Sample_void_ratio              sql.NullFloat64      `json:"Sample_void_ratio"`

}

func JanusMadSectionModel() *JanusMadSection {
	return &JanusMadSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusMadSectionFunc(qry string, uri string, filename string, database string, collection string) error {

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
		d := []JanusMadSection{}
		var t JanusMadSection
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