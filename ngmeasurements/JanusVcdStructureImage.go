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

type JanusVcdStructureImagecVSW struct {
	Tables []JanusVcdStructureImagetable `json:"tables"`
}

type JanusVcdStructureImagetable struct {
	URL string                           `json:"url"`
	Row []JanusVcdStructureImagejanusRow `json:"row"`
}

type JanusVcdStructureImagejanusRow struct {
	URL       string                   `json:"url"`
	Rownum    int                      `json:"rownum"`
	Describes []JanusVcdStructureImage `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusVcdStructureImage struct {
	Leg            int64   `json:"Leg"`
	Site           int64   `json:"Site"`
	Hole           string  `json:"Hole"`
	Core           int64   `json:"Core"`
	Core_type      string  `json:"Core_type"`
	Section_number int64   `json:"Section_number"`
	Section_type   string  `json:"Section_type"`
	Top_cm         float64 `json:"Top_cm"`
	Depth_mbsf     float64 `json:"Depth_mbsf"`
	Page_id        int64   `json:"Page_id"`
	Url            string  `json:"Url"`
}

func JanusVcdStructureImageModel() *JanusVcdStructureImage {
	return &JanusVcdStructureImage{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusVcdStructureImageFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusVcdStructureImagejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusVcdStructureImage{}
		var t JanusVcdStructureImage
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusVcdStructureImagejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusVcdStructureImagetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusVcdStructureImagetable{}
	tableSet = append(tableSet, theTable)
	final := JanusVcdStructureImagecVSW{tableSet}

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
