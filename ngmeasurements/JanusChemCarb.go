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
	Describes []JanusChemCarb `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusChemCarb struct {
    Leg                            int64                `json:"Leg"`
    Site                           int64                `json:"Site"`
    Hole                           string               `json:"Hole"`
    Core                           int64                `json:"Core"`
    Core_type                      string               `json:"Core_type"`
    Section_number                 int64                `json:"Section_number"`
    Section_type                   sql.NullString       `json:"Section_type"`
    Top_cm                         sql.NullFloat64      `json:"Top_cm"`
    Bot_cm                         sql.NullFloat64      `json:"Bot_cm"`
    Depth_mbsf                     sql.NullFloat64      `json:"Depth_mbsf"`
    Inor_c_wt_pct                  sql.NullFloat64      `json:"Inor_c_wt_pct"`
    Caco3_wt_pct                   sql.NullFloat64      `json:"Caco3_wt_pct"`
    Tot_c_wt_pct                   sql.NullFloat64      `json:"Tot_c_wt_pct"`
    Org_c_wt_pct                   sql.NullFloat64      `json:"Org_c_wt_pct"`
    Nit_wt_pct                     sql.NullFloat64      `json:"Nit_wt_pct"`
    Sul_wt_pct                     sql.NullFloat64      `json:"Sul_wt_pct"`
    H_wt_pct                       sql.NullFloat64      `json:"H_wt_pct"`

}

func JanusChemCarbModel() *JanusChemCarb {
	return &JanusChemCarb{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusChemCarbFunc(qry string, uri string, filename string, database string, collection string) error {

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
		d := []JanusChemCarb{}
		var t JanusChemCarb
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