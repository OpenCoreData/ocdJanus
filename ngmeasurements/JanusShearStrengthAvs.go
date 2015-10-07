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
	Describes []JanusShearStrengthAvs `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusShearStrengthAvs struct {
    Leg                            int64                `json:"Leg"`
    Site                           int64                `json:"Site"`
    Hole                           string               `json:"Hole"`
    Core                           int64                `json:"Core"`
    Core_type                      string               `json:"Core_type"`
    Section_number                 int64                `json:"Section_number"`
    Section_type                   string               `json:"Section_type"`
    Top_cm                         float64              `json:"Top_cm"`
    Bot_cm                         float64              `json:"Bot_cm"`
    Depth_mbsf                     float64              `json:"Depth_mbsf"`
    Section_id                     int64                `json:"Section_id"`
    Shear_strength                 sql.NullFloat64      `json:"Shear_strength"`
    Max_torque_angle               sql.NullFloat64      `json:"Max_torque_angle"`
    Residual_strength              sql.NullFloat64      `json:"Residual_strength"`
    Residual_torque_angle          sql.NullFloat64      `json:"Residual_torque_angle"`
    Run_number                     int64                `json:"Run_number"`
    Run_timestamp                  sql.NullString       `json:"Run_timestamp"`
    Direction                      sql.NullString       `json:"Direction"`
    Rotation_rate                  sql.NullFloat64      `json:"Rotation_rate"`
    Vane_id                        sql.NullString       `json:"Vane_id"`
    Spring_id                      sql.NullString       `json:"Spring_id"`
    Raw_data_collected             sql.NullString       `json:"Raw_data_collected"`
    Raw_torque_angle               sql.NullFloat64      `json:"Raw_torque_angle"`
    Raw_strain_angle               sql.NullFloat64      `json:"Raw_strain_angle"`
    Section_comment                sql.NullString       `json:"Section_comment"`
    Spring_comment                 sql.NullString       `json:"Spring_comment"`
    Vane_comment                   sql.NullString       `json:"Vane_comment"`

}

func JanusShearStrengthAvsModel() *JanusShearStrengthAvs {
	return &JanusShearStrengthAvs{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusShearStrengthAvsFunc(qry string, uri string, filename string, database string, collection string) error {

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
		d := []JanusShearStrengthAvs{}
		var t JanusShearStrengthAvs
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