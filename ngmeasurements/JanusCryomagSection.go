package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusCryomagSectioncVSW struct {
	Tables []JanusCryomagSectiontable `json:"tables"`
}

type JanusCryomagSectiontable struct {
	URL string                        `json:"url"`
	Row []JanusCryomagSectionjanusRow `json:"row"`
}

type JanusCryomagSectionjanusRow struct {
	URL       string                `json:"url"`
	Rownum    int                   `json:"rownum"`
	Describes []JanusCryomagSection `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusCryomagSection struct {
	Leg              int64           `json:"Leg"`
	Site             int64           `json:"Site"`
	Hole             string          `json:"Hole"`
	Core             int64           `json:"Core"`
	Core_type        string          `json:"Core_type"`
	Section_number   int64           `json:"Section_number"`
	Section_type     string          `json:"Section_type"`
	Top_cm           float64         `json:"Top_cm"`
	Depth_mbsf       float64         `json:"Depth_mbsf"`
	Section_id       int64           `json:"Section_id"`
	Treatment_type   string          `json:"Treatment_type"`
	Treatment_demag  sql.NullFloat64 `json:"Treatment_demag"`
	Treatment_bias   sql.NullFloat64 `json:"Treatment_bias"`
	Demag_type       string          `json:"Demag_type"`
	Demag_level      float64         `json:"Demag_level"`
	Declination      float64         `json:"Declination"`
	Inclination      float64         `json:"Inclination"`
	Intensity        float64         `json:"Intensity"`
	Hole_inclination sql.NullFloat64 `json:"Hole_inclination"`
	Intensity_x      float64         `json:"Intensity_x"`
	Intensity_y      float64         `json:"Intensity_y"`
	Intensity_z      float64         `json:"Intensity_z"`
	Run_number       int64           `json:"Run_number"`
	Run_comment      sql.NullString  `json:"Run_comment"`
}

func JanusCryomagSectionModel() *JanusCryomagSection {
	return &JanusCryomagSection{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusCryomagSectionFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusCryomagSectionjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusCryomagSection{}
		var t JanusCryomagSection
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusCryomagSectionjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusCryomagSectiontable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusCryomagSectiontable{}
	tableSet = append(tableSet, theTable)
	final := JanusCryomagSectioncVSW{tableSet}

	// session, err := mgo.Dial("127.0.0.1")
	// if err != nil {
	// 	panic(err)
	// }
	// defer  session.Close()

	// Optional. Switch the session to a Strong behavior.
	session.SetMode(mgo.Strong, true)
	c := session.DB(database).C(collection)

	bsondata, bsonerr := bson.Marshal(final)
	if bsonerr != nil {
		log.Fatalf("Error %v with %v\n", err, final)
	}

	// err = c.Insert(&final)
	err = c.Insert(bsondata)
	if err != nil {
		log.Fatalf("Error %v with %v\n", err, final)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
