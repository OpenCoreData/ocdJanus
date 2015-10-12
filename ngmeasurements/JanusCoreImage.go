package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusCoreImagecVSW struct {
	Tables []JanusCoreImagetable `json:"tables"`
}

type JanusCoreImagetable struct {
	URL string                   `json:"url"`
	Row []JanusCoreImagejanusRow `json:"row"`
}

type JanusCoreImagejanusRow struct {
	URL       string           `json:"url"`
	Rownum    int              `json:"rownum"`
	Describes []JanusCoreImage `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusCoreImage struct {
	Leg              int64           `json:"Leg"`
	Site             int64           `json:"Site"`
	Hole             string          `json:"Hole"`
	Core             int64           `json:"Core"`
	Core_type        string          `json:"Core_type"`
	Section_number   sql.NullInt64   `json:"Section_number"`
	Section_type     sql.NullString  `json:"Section_type"`
	Top_depth_mbsf   sql.NullFloat64 `json:"Top_depth_mbsf"`
	Core_image_type  sql.NullString  `json:"Core_image_type"`
	Image_format     sql.NullString  `json:"Image_format"`
	Image_resolution sql.NullInt64   `json:"Image_resolution"`
	Url              sql.NullString  `json:"Url"`
}

func JanusCoreImageModel() *JanusCoreImage {
	return &JanusCoreImage{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusCoreImageFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusCoreImagejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusCoreImage{}
		var t JanusCoreImage
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusCoreImagejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusCoreImagetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusCoreImagetable{}
	tableSet = append(tableSet, theTable)
	final := JanusCoreImagecVSW{tableSet}

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
		log.Printf("Error %v with %v\n", err, final)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
