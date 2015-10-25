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

type JanusSmearSlidecVSW struct {
	Tables []JanusSmearSlidetable `json:"tables"`
}

type JanusSmearSlidetable struct {
	URL string                    `json:"url"`
	Row []JanusSmearSlidejanusRow `json:"row"`
}

type JanusSmearSlidejanusRow struct {
	URL       string            `json:"url"`
	Rownum    int               `json:"rownum"`
	Describes []JanusSmearSlide `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusSmearSlide struct {
	Leg                 int64          `json:"Leg"`
	Site                int64          `json:"Site"`
	Hole                string         `json:"Hole"`
	Core                int64          `json:"Core"`
	Core_type           string         `json:"Core_type"`
	Section_number      int64          `json:"Section_number"`
	Section_type        string         `json:"Section_type"`
	Top_cm              float64        `json:"Top_cm"`
	Bot_cm              float64        `json:"Bot_cm"`
	Depth_mbsf          float64        `json:"Depth_mbsf"`
	Section_id          int64          `json:"Section_id"`
	Location            string         `json:"Location"`
	Sample_id           int64          `json:"Sample_id"`
	Lithology_code      string         `json:"Lithology_code"`
	Percent_sand        sql.NullString `json:"Percent_sand"`
	Percent_silt        sql.NullString `json:"Percent_silt"`
	Percent_clay        sql.NullString `json:"Percent_clay"`
	Component_type_code string         `json:"Component_type_code"`
	Component_name_code int64          `json:"Component_name_code"`
	Component_name      string         `json:"Component_name"`
	Abundance_code      sql.NullString `json:"Abundance_code"`
	Component_percent   sql.NullString `json:"Component_percent"`
}

func JanusSmearSlideModel() *JanusSmearSlide {
	return &JanusSmearSlide{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusSmearSlideFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusSmearSlidejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusSmearSlide{}
		var t JanusSmearSlide
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusSmearSlidejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusSmearSlidetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusSmearSlidetable{}
	tableSet = append(tableSet, theTable)
	final := JanusSmearSlidecVSW{tableSet}

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
