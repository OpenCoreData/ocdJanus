package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
)

type JanusDhtApctcVSW struct {
	Tables []JanusDhtApcttable `json:"tables"`
}

type JanusDhtApcttable struct {
	URL string                 `json:"url"`
	Row []JanusDhtApctjanusRow `json:"row"`
}

type JanusDhtApctjanusRow struct {
	URL       string         `json:"url"`
	Rownum    int            `json:"rownum"`
	Describes []JanusDhtApct `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusDhtApct struct {
	Leg            int64           `json:"Leg"`
	Site           int64           `json:"Site"`
	Hole           string          `json:"Hole"`
	Core           int64           `json:"Core"`
	Core_type      string          `json:"Core_type"`
	Top_depth_mbsf float64         `json:"Top_depth_mbsf"`
	Bot_depth_mbsf float64         `json:"Bot_depth_mbsf"`
	Run_number     int64           `json:"Run_number"`
	Depth_comment  sql.NullFloat64 `json:"Depth_comment"`
	Temperature_c  sql.NullFloat64 `json:"Temperature_c"`
	Error_c        sql.NullFloat64 `json:"Error_c"`
	Mudline_c      sql.NullFloat64 `json:"Mudline_c"`
	Tool_name      string          `json:"Tool_name"`
	Run_comment    sql.NullString  `json:"Run_comment"`
}

func JanusDhtApctModel() *JanusDhtApct {
	return &JanusDhtApct{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusDhtApctFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusDhtApctjanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusDhtApct{}
		var t JanusDhtApct
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusDhtApctjanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusDhtApcttable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusDhtApcttable{}
	tableSet = append(tableSet, theTable)
	final := JanusDhtApctcVSW{tableSet}

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
		log.Printf("Janus func Error %v with %s\n", err, final, filename)
	}

	log.Printf("File: %s  written", filename)

	// session.Close()
	return nil
}
