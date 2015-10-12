package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusSedThinSectionSamplecVSW struct {
	Tables []JanusSedThinSectionSampletable `json:"tables"`
}

type JanusSedThinSectionSampletable struct {
	URL string                              `json:"url"`
	Row []JanusSedThinSectionSamplejanusRow `json:"row"`
}

type JanusSedThinSectionSamplejanusRow struct {
	URL       string                      `json:"url"`
	Rownum    int                         `json:"rownum"`
	Describes []JanusSedThinSectionSample `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusSedThinSectionSample struct {
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
	Location             string         `json:"Location"`
	Sample_id            int64          `json:"Sample_id"`
	Component_type_code  string         `json:"Component_type_code"`
	Component_name_code  int64          `json:"Component_name_code"`
	Sts_component_name   string         `json:"Sts_component_name"`
	Abundance_code       sql.NullString `json:"Abundance_code"`
	Thin_section_comment sql.NullString `json:"Thin_section_comment"`
}

func JanusSedThinSectionSampleModel() *JanusSedThinSectionSample {
	return &JanusSedThinSectionSample{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusSedThinSectionSampleFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusSedThinSectionSamplejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusSedThinSectionSample{}
		var t JanusSedThinSectionSample
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusSedThinSectionSamplejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusSedThinSectionSampletable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusSedThinSectionSampletable{}
	tableSet = append(tableSet, theTable)
	final := JanusSedThinSectionSamplecVSW{tableSet}

	// session, err := mgo.Dial("127.0.0.1")
	// if err != nil {
	// 	panic(err)
	// }
	// defer  session.Close()

	// Optional. Switch the session to a Strong behavior.
	session.SetMode(mgo.Strong, true)
	c := session.DB(database).C(collection)

	err = c.Insert(&final)
	if err != nil {
		log.Printff("Error %v with %v\n", err, final)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
