package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
	"strconv"
)

type CVSW struct {
	Tables []Table `json:"tables"`
}

type Table struct {
	URL string     `json:"url"`
	Row []JanusRow `json:"row"`
}

type JanusRow struct {
	URL       string          `json:"url"`
	Rownum    int             `json:"rownum"`
	Describes []JanusRowItems `json:"describes"`
}

// make name generic
type JanusRowItems struct {
	Leg               int64           `json:"Leg"`
	Site              int64           `json:"Site"`
	Hole              string          `json:"Hole"`
	Core              int64           `json:"Core"`
	Core_type         string          `json:"Core_type"`
	Top_depth_mbsf    sql.NullFloat64 `json:"Top_depth_mbsf"`
	Length_cored      sql.NullFloat64 `json:"Length_cored"`
	Length_recovered  sql.NullFloat64 `json:"Length_recovered"`
	Percent_recovered sql.NullFloat64 `json:"Percent_recovered"`
	Curated_length    sql.NullFloat64 `json:"Curated_length"`
	Ship_date_time    sql.NullString  `json:"Ship_date_time"`
	Core_comment      sql.NullString  `json:"Core_comment"`
}

type NullFloat64 struct {
	sql.NullFloat64
}

func (nf NullFloat64) MarshalText() ([]byte, error) {
	if nf.Valid {
		nfv := nf.Float64
		return []byte(strconv.FormatFloat(nfv, 'f', -1, 64)), nil
	} else {
		return []byte("null"), nil
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// func JSONData(qry string, uri string, filename string) []byte {
func CoreSummaryJSONData(qry string, uri string, filename string, database string, collection string) error {

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusRowItems{}
		var t JanusRowItems
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := Table{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []Table{}
	tableSet = append(tableSet, theTable)
	final := CVSW{tableSet}

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
