package measurements

import (
	// "bytes"
	"log"
	"opencoredata.org/ocdJanus/connect"
	"os"
	// "text/template"
)

type LSH struct {
	Leg  string
	Site string
	Hole string
}

func Measurements(leg string, site string, hole string) {
	log.Printf("Measurements for %s_%s%s", leg, site, hole)

	conn, err := connect.GetJanusCon()
	defer conn.Close()

	// pull query based on looping through an array, so we can do each query
	// for the given L_SH
	//qryv2 := "SELECT * FROM ocd_chem_carb WHERE leg = 207 and site = 1259 and hole = 'C'"

	// const qrytmp = "SELECT * FROM ocd_chem_carb WHERE leg = {{.Leg}} and site = {{.Site}} and hole = '{{.Hole}}'"

	// // create the SPARQL call from a template
	// lshset := LSH{leg, site, hole}
	// var buff = bytes.NewBufferString("")
	// t, err := template.New("sql template").Parse(qrytmp)
	// if err != nil {
	// 	log.Printf("janus sql template creation failed: %s", err)
	// }
	// err = t.Execute(buff, lshset) //  instead of os.Stdout create a function to call.
	// // GET URI
	// // todo get the URI for this LSH + measurement combination.  It will be used
	// // for the files (csvw and json) and the metadata (csvw-metadata and schema.org)

	// // todo.  use same logic as used to read in query to set a uniqe file name
	// // for a measurements
	// qry := string(buff.Bytes())

	qry := "SELECT * FROM ocd_chem_carb WHERE leg = 207 and site = 1259 and hole = 'C'"

	log.Printf("Qry string %s", qry)

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
		return
	}
	fcsvw, _ := os.Create("./chemcarb.csv")
	defer fcsvw.Close()
	dumpCSVW(rows, fcsvw)

	rows2, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
		return
	}
	fjson, _ := os.Create("./chemcarb.json")
	defer fjson.Close()
	dumpJSON(rows2, fjson)

}
