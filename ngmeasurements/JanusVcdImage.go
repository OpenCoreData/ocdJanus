package ngmeasurements

import (
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
)

type JanusVcdImagecVSW struct {
	Tables []JanusVcdImagetable `json:"tables"`
}

type JanusVcdImagetable struct {
	URL string                  `json:"url"`
	Row []JanusVcdImagejanusRow `json:"row"`
}

type JanusVcdImagejanusRow struct {
	URL       string          `json:"url"`
	Rownum    int             `json:"rownum"`
	Describes []JanusVcdImage `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusVcdImage struct {
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

func JanusVcdImageModel() *JanusVcdImage {
	return &JanusVcdImage{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusVcdImageFunc(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusVcdImagejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusVcdImage{}
		var t JanusVcdImage
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusVcdImagejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusVcdImagetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusVcdImagetable{}
	tableSet = append(tableSet, theTable)
	final := JanusVcdImagecVSW{tableSet}

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

	conn.Close()
	return nil
}
