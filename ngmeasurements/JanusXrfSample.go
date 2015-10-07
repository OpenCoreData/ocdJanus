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
	Describes []JanusXrfSample `json:"describes"`
}

// make name generic  How to load the body of struct
type JanusXrfSample struct {
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
    Sample_id                      int64                `json:"Sample_id"`
    Xrf_run_identifier             string               `json:"Xrf_run_identifier"`
    Xrf_replicate                  string               `json:"Xrf_replicate"`
    Xrf_analysis_type              string               `json:"Xrf_analysis_type"`
    Bead_loi                       sql.NullFloat64      `json:"Bead_loi"`
    Sample_type                    string               `json:"Sample_type"`
    Xrf_comment                    sql.NullString       `json:"Xrf_comment"`
    Sio2_wt_pct                    sql.NullFloat64      `json:"SiO2_wt_pct"`
    Tio2_wt_pct                    sql.NullFloat64      `json:"TiO2_wt_pct"`
    Al2o3_wt_pct                   sql.NullFloat64      `json:"Al2O3_wt_pct"`
    Fe2o3_wt_pct                   sql.NullFloat64      `json:"Fe2O3*_wt_pct"`
    Mno_wt_pct                     sql.NullFloat64      `json:"MnO_wt_pct"`
    Mgo_wt_pct                     sql.NullFloat64      `json:"MgO_wt_pct"`
    Cao_wt_pct                     sql.NullFloat64      `json:"CaO_wt_pct"`
    Na2o_wt_pct                    sql.NullFloat64      `json:"Na2O_wt_pct"`
    K2o_wt_pct                     sql.NullFloat64      `json:"K2O_wt_pct"`
    P2o5_wt_pct                    sql.NullFloat64      `json:"P2O5_wt_pct"`
    Nb_ppm                         sql.NullFloat64      `json:"Nb_ppm"`
    Zr_ppm                         sql.NullFloat64      `json:"Zr_ppm"`
    Y_ppm                          sql.NullFloat64      `json:"Y_ppm"`
    S_ppm                          sql.NullFloat64      `json:"S_ppm"`
    Sr_ppm                         sql.NullFloat64      `json:"Sr_ppm"`
    Rb_ppm                         sql.NullFloat64      `json:"Rb_ppm"`
    Sc_ppm                         sql.NullFloat64      `json:"Sc_ppm"`
    Mo_ppm                         sql.NullFloat64      `json:"Mo_ppm"`
    Be_ppm                         sql.NullFloat64      `json:"Be_ppm"`
    Th_ppm                         sql.NullFloat64      `json:"Th_ppm"`
    Co_ppm                         sql.NullFloat64      `json:"Co_ppm"`
    Gd_ppm                         sql.NullFloat64      `json:"Gd_ppm"`
    Dy_ppm                         sql.NullFloat64      `json:"Dy_ppm"`
    Er_ppm                         sql.NullFloat64      `json:"Er_ppm"`
    Yb_ppm                         sql.NullFloat64      `json:"Yb_ppm"`
    Hf_ppm                         sql.NullFloat64      `json:"Hf_ppm"`
    Pb_ppm                         sql.NullFloat64      `json:"Pb_ppm"`
    Ga_ppm                         sql.NullFloat64      `json:"Ga_ppm"`
    Zn_ppm                         sql.NullFloat64      `json:"Zn_ppm"`
    Cu_ppm                         sql.NullFloat64      `json:"Cu_ppm"`
    Ni_ppm                         sql.NullFloat64      `json:"Ni_ppm"`
    Cr_ppm                         sql.NullFloat64      `json:"Cr_ppm"`
    V_ppm                          sql.NullFloat64      `json:"V_ppm"`
    Ce_ppm                         sql.NullFloat64      `json:"Ce_ppm"`
    Ba_ppm                         sql.NullFloat64      `json:"Ba_ppm"`
    Cs_ppm                         sql.NullFloat64      `json:"Cs_ppm"`
    La_ppm                         sql.NullFloat64      `json:"La_ppm"`
    Nd_ppm                         sql.NullFloat64      `json:"Nd_ppm"`
    Sm_ppm                         sql.NullFloat64      `json:"Sm_ppm"`

}

func JanusXrfSampleModel() *JanusXrfSample {
	return &JanusXrfSample{}
}

// func JSONData(qry string, uri string, filename string) []byte {
func JanusXrfSampleFunc(qry string, uri string, filename string, database string, collection string) error {

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
		d := []JanusXrfSample{}
		var t JanusXrfSample
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