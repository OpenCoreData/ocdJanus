package ngmeasurements

import (
	"bytes"
	// "fmt"
	"github.com/kisielk/sqlstruct"
	"log"
	"opencoredata.org/ocdJanus/connect"
	"opencoredata.org/ocdJanus/metadata"
	"opencoredata.org/ocdJanus/mongo"
	"opencoredata.org/ocdJanus/queries"
	"opencoredata.org/ocdJanus/utils"
	// "os"
	"database/sql"
	"strconv"
	"text/template"
)

type LSH struct {
	Leg               string
	Site              string
	Hole              string
	Latitude_degrees  float64
	Longitude_degrees float64
}

type JanusCoreSummary struct {
	Leg               int64           `json:"Leg"`
	Site              int64           `json:"Site"`
	Hole              string          `json:"Hole"`
	Core              int64           `json:"Core"`
	Core_type         string          `json:"Core_type"`
	Top_depth_mbsf    sql.NullInt64   `json:"Top_depth_mbsf"`
	Length_cored      sql.NullFloat64 `json:"Length_cored"`
	Length_recovered  sql.NullFloat64 `json:"Length_recovered"`
	Percent_recovered sql.NullFloat64 `json:"Percent_recovered"`
	Curated_length    sql.NullFloat64 `json:"Curated_length"`
	Ship_date_time    sql.NullString  `json:"Ship_date_time"`
	Core_comment      sql.NullString  `json:"Core_comment"`
}

func CoreSummary() {
	measurement := "core_summary"

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	qry := queries.Sql_lsh
	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
		return
	}

	for rows.Next() {
		var lsh LSH
		err := sqlstruct.Scan(&lsh, rows)
		if err != nil {
			log.Print(err)
		}

		const qrytmp = "SELECT * FROM ocd_core_summary WHERE leg = {{.Leg}} and site = {{.Site}} and hole = '{{.Hole}}'"
		var buff = bytes.NewBufferString("")
		t, err := template.New("sql template").Parse(qrytmp)
		if err != nil {
			log.Printf("janus sql template creation failed: %s", err)
		}
		err = t.Execute(buff, lsh) //  instead of os.Stdout create a function to call.
		qry := string(buff.Bytes())

		log.Printf("Event: %s %s_%s%s  %s\n", measurement, lsh.Leg, lsh.Site, lsh.Hole, qry)

		if utils.DataCheck(qry) {
			uri := mongo.AuthorURI(lsh.Leg, lsh.Site, lsh.Hole, measurement)
			csvfilename := utils.MakeName("csv", lsh.Leg, lsh.Site, lsh.Hole, measurement)
			csvdata := utils.CSVData(qry) // is this b.Bytes()
			mongo.UploadCSVToMongo("test", "csv", uri, csvfilename, csvdata)

			jsonfilename := utils.MakeName("json", lsh.Leg, lsh.Site, lsh.Hole, measurement)
			err := CoreSummaryJSONData(qry, uri, jsonfilename, "test", "jsonld")
			if err != nil {
				log.Printf("janus sql template creation failed: %s", err)
			}

			// // make metadata
			metastruct := &JanusCoreSummary{}
			csvwmeta := metadata.CSVMetadata(metastruct, measurement, csvfilename, uri, qry)
			mongo.UploadCSVW("test", "csvwmeta", uri, csvwmeta)
			schemameta := metadata.SchemaOrgDataset(metastruct, strconv.FormatFloat(lsh.Latitude_degrees, 'f', 2, 64), strconv.FormatFloat(lsh.Longitude_degrees, 'f', 2, 64), measurement, csvfilename, uri, qry)
			mongo.UploadSchemaOrg("test", "schemaorg", uri, schemameta)

		}
	}
}
