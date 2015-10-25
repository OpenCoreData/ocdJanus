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

type JanusSamplecVSW struct {
	Tables []JanusSampletable `json:"tables"`
}

type JanusSampletable struct {
	URL string                `json:"url"`
	Row []JanusSamplejanusRow `json:"row"`
}

type JanusSamplejanusRow struct {
	URL       string        `json:"url"`
	Rownum    int           `json:"rownum"`
	Describes []JanusSample `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusSample struct {
	Leg                    int64           `json:"Leg"`
	Site                   int64           `json:"Site"`
	Hole                   string          `json:"Hole"`
	Core                   int64           `json:"Core"`
	Core_type              string          `json:"Core_type"`
	Section_number         int64           `json:"Section_number"`
	Section_type           string          `json:"Section_type"`
	Top_cm                 float64         `json:"Top_cm"`
	Bot_cm                 float64         `json:"Bot_cm"`
	Depth_mbsf             float64         `json:"Depth_mbsf"`
	Request_number         sql.NullString  `json:"Request_number"`
	Sample_volume          sql.NullFloat64 `json:"Sample_volume"`
	Piece_sub_piece        sql.NullString  `json:"Piece_sub_piece"`
	Sample_comment         sql.NullString  `json:"Sample_comment"`
	Sample_archive_working sql.NullString  `json:"Sample_archive_working"`
	Sample_lab_code        string          `json:"Sample_lab_code"`
	Catwalk_sample         sql.NullString  `json:"Catwalk_sample"`
	Sample_id              int64           `json:"Sample_id"`
	Location               string          `json:"Location"`
	Sample_repository      sql.NullString  `json:"Sample_repository"`
	Sample_timestamp       string          `json:"Sample_timestamp"`
}

func JanusSampleModel() *JanusSample {
	return &JanusSample{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusSampleFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusSamplejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusSample{}
		var t JanusSample
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusSamplejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusSampletable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusSampletable{}
	tableSet = append(tableSet, theTable)
	final := JanusSamplecVSW{tableSet}

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
