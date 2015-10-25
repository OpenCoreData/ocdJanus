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

type JanusThinSectionImagecVSW struct {
	Tables []JanusThinSectionImagetable `json:"tables"`
}

type JanusThinSectionImagetable struct {
	URL string                          `json:"url"`
	Row []JanusThinSectionImagejanusRow `json:"row"`
}

type JanusThinSectionImagejanusRow struct {
	URL       string                  `json:"url"`
	Rownum    int                     `json:"rownum"`
	Describes []JanusThinSectionImage `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusThinSectionImage struct {
	Leg                int64          `json:"Leg"`
	Site               int64          `json:"Site"`
	Hole               string         `json:"Hole"`
	Core               int64          `json:"Core"`
	Core_type          string         `json:"Core_type"`
	Section_number     int64          `json:"Section_number"`
	Section_type       string         `json:"Section_type"`
	Top_cm             float64        `json:"Top_cm"`
	Bot_cm             float64        `json:"Bot_cm"`
	Depth_mbsf         float64        `json:"Depth_mbsf"`
	Section_id         int64          `json:"Section_id"`
	Piece              sql.NullInt64  `json:"Piece"`
	Sub_piece          sql.NullString `json:"Sub_piece"`
	Sample_id          int64          `json:"Sample_id"`
	Location           string         `json:"Location"`
	Slide_number       int64          `json:"Slide_number"`
	Ts_comment         sql.NullString `json:"Ts_comment"`
	Ts_sample_code_lab sql.NullString `json:"Ts_sample_code_lab"`
	Url                sql.NullString `json:"Url"`
	File_name          sql.NullString `json:"File_name"`
	Image_date         sql.NullString `json:"Image_date"`
	Light_abbr         sql.NullString `json:"Light_abbr"`
	Magnification      sql.NullString `json:"Magnification"`
	Feature            sql.NullString `json:"Feature"`
	Scientist_initials sql.NullString `json:"Scientist_initials"`
	Image_format       sql.NullString `json:"Image_format"`
	Image_resolution   sql.NullInt64  `json:"Image_resolution"`
	Micro_image_id     sql.NullInt64  `json:"Micro_image_id"`
}

func JanusThinSectionImageModel() *JanusThinSectionImage {
	return &JanusThinSectionImage{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusThinSectionImageFunc(qry string, uri string, filename string, database string, collection string, conn *sql.DB, session *mgo.Session) error {

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
	}

	allResults := []JanusThinSectionImagejanusRow{}
	i := 1
	for rows.Next() {
		d := []JanusThinSectionImage{}
		var t JanusThinSectionImage
		err := sqlstruct.Scan(&t, rows)
		if err != nil {
			log.Print(err)
		}
		d = append(d, t)
		rowURL := fmt.Sprintf("%s/%s#row=%v", uri, filename, i)
		aRow := JanusThinSectionImagejanusRow{rowURL, i, d}
		allResults = append(allResults, aRow)
		i = i + 1
	}

	theTable := JanusThinSectionImagetable{fmt.Sprintf("%s/%s", uri, filename), allResults}
	tableSet := []JanusThinSectionImagetable{}
	tableSet = append(tableSet, theTable)
	final := JanusThinSectionImagecVSW{tableSet}

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
