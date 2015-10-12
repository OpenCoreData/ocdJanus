package ngmeasurements

import (
	"database/sql"
	"fmt"
	"github.com/kisielk/sqlstruct"
	"gopkg.in/mgo.v2"
	"log"
	// "opencoredata.org/ocdJanus/connect"
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
	Describes []JanusTestStuff `json:"describes"`
}

// make name generic
type JanusTestStuff struct {
	Leg                   int64           `json:"Leg"`
	Site                  int64           `json:"Site"`
	Hole                  string          `json:"Hole"`
	Age_model_type        string          `json:"Age_model_type"`
	Depth_mbsf            float64         `json:"Depth_mbsf"`
	Age_ma                sql.NullFloat64 `json:"Age_ma"`
	Control_point_comment sql.NullString  `json:"Control_point_comment"`
}

func AgeDataPointModel() *JanusTestStuff {
	return &JanusTestStuff{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func AgeDataPoint(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	// conn, err := connect.GetJanusCon()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer conn.Close()

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []janusRow{}
	i := 1
	for rows.Next() {
		d := []JanusTestStuff{}
		var t JanusTestStuff
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
