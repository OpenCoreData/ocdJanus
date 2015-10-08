package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
)

type JanusPrimeDataImagecVSW struct {
	Tables []JanusPrimeDataImagetable `json:"tables"`
}

type JanusPrimeDataImagetable struct {
	URL string                        `json:"url"`
	Row []JanusPrimeDataImagejanusRow `json:"row"`
}

type JanusPrimeDataImagejanusRow struct {
	URL       string                `json:"url"`
	Rownum    int                   `json:"rownum"`
	Describes []JanusPrimeDataImage `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusPrimeDataImage struct {
	Leg               int64          `json:"Leg"`
	Site              int64          `json:"Site"`
	Hole              string         `json:"Hole"`
	Core              int64          `json:"Core"`
	Core_type         string         `json:"Core_type"`
	Section_number    int64          `json:"Section_number"`
	Section_type      string         `json:"Section_type"`
	Top_cm            float64        `json:"Top_cm"`
	Depth_mbsf        float64        `json:"Depth_mbsf"`
	Data_type_abbr    string         `json:"Data_type_abbr"`
	Fossil_group_name sql.NullString `json:"Fossil_group_name"`
	Page_id           int64          `json:"Page_id"`
	Url               string         `json:"Url"`
}

func JanusPrimeDataImageModel() *JanusPrimeDataImage {
	return &JanusPrimeDataImage{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusPrimeDataImageFunc(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusPrimeDataImagejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusPrimeDataImage{}
		var t JanusPrimeDataImage
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusPrimeDataImagejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusPrimeDataImagetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusPrimeDataImagetable{}
	tableSet = append(tableSet, theTable)
	final := JanusPrimeDataImagecVSW{tableSet}

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
