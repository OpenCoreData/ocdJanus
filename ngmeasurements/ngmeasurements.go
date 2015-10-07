package ngmeasurements

import (
	"bytes"
	// "fmt"
	"github.com/kisielk/sqlstruct"
	// "gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/connect"
	"opencoredata.org/ocdJanus/metadata"
	"opencoredata.org/ocdJanus/mongo"
	"opencoredata.org/ocdJanus/queries"
	"opencoredata.org/ocdJanus/utils"
	"strconv"
	"text/template"
)

type LSH struct {
	Leg               string
	Site              string
	Hole              string
	Latitude_degrees  float64
	Longitude_degrees float64
	Measurement       string
}

func MasterLoop() {
	//measurement := "core_summary"

	// array of measurements
	// measurements := []string{"age_datapoint", "age_profile", "chem_carb", "core_image", "core_summary",
	// 	"dht_apct", "gra_section", "icp_sample", "mad_section", "ms2f_section", "mscl_section", "msl_section",
	// 	"ngr_section", "paleo_image", "paleo_sample", "prime_data_image", "pwl_section", "pws1_section",
	// 	"pws2_section", "pws3_section", "sample", "sed_thin_section_sample", "shear_strength_tor",
	// 	"smear_slide", "tensor_core", "thermal_conductivity", "thin_section_image", "vcd_hard_rock_image",
	// 	"vcd_image", "vcd_structure_image", "xrd_image", "xrf_sample"}
	// queryString := []string{"SELECT * FROM ocd_age_datapoint", "SELECT * FROM ocd_age_profile",
	// 	"SELECT * FROM ocd_chem_carb", "SELECT * FROM ocd_core_image", "SELECT * FROM ocd_core_summary",
	// 	"SELECT * FROM ocd_dht_apct", "SELECT * FROM ocd_gra_section", "SELECT * FROM ocd_icp_sample",
	// 	"SELECT * FROM ocd_mad_section", "SELECT * FROM ocd_ms2f_section", "SELECT * FROM ocd_mscl_section",
	// 	"SELECT * FROM ocd_msl_section", "SELECT * FROM ocd_ngr_section", "SELECT * FROM ocd_paleo_image",
	// 	"SELECT * FROM ocd_paleo_sample", "SELECT * FROM ocd_prime_data_image", "SELECT * FROM ocd_pwl_section",
	// 	"SELECT * FROM ocd_pws1_section", "SELECT * FROM ocd_pws2_section", "SELECT * FROM ocd_pws3_section",
	// 	"SELECT * FROM ocd_sample", "SELECT * FROM ocd_sed_thin_section_sample",
	// 	"SELECT * FROM ocd_shear_strength_tor", "SELECT * FROM ocd_smear_slide", "SELECT * FROM ocd_tensor_core",
	// 	"SELECT * FROM ocd_thermal_conductivity", "SELECT * FROM ocd_thin_section_image",
	// 	"SELECT * FROM ocd_vcd_hard_rock_image", "SELECT * FROM ocd_vcd_image",
	// 	"SELECT * FROM ocd_vcd_structure_image", "SELECT * FROM ocd_xrd_image",
	// 	"SELECT * FROM ocd_xrf_sample"}

	measurements := []string{"age_datapoint", "core_summary"}
	queryString := []string{"SELECT * FROM ocd_age_datapoint", "SELECT * FROM ocd_core_summary"}

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for index, each := range queryString {

		lshqry := queries.Sql_lsh
		lshrows, err := conn.Query(lshqry)
		if err != nil {
			log.Printf(`Error with "%s": %s`, lshqry, err)
			return
		}

		for lshrows.Next() {
			var lsh LSH
			err := sqlstruct.Scan(&lsh, lshrows)
			lsh.Measurement = each
			if err != nil {
				log.Print(err)
			}

			const qrytmp = "{{.Measurement}} WHERE leg = {{.Leg}} and site = {{.Site}} and hole = '{{.Hole}}'"
			var buff = bytes.NewBufferString("")
			t, err := template.New("sql template").Parse(qrytmp)
			if err != nil {
				log.Printf("janus sql template creation failed: %s", err)
			}
			err = t.Execute(buff, lsh)
			qry := string(buff.Bytes())

			if utils.DataCheck(qry) {

				log.Printf("Event: %s %s_%s%s  %s\n", measurements[index], lsh.Leg, lsh.Site, lsh.Hole, qry)

				uri := mongo.AuthorURI(lsh.Leg, lsh.Site, lsh.Hole, measurements[index])
				csvfilename := utils.MakeName("csv", lsh.Leg, lsh.Site, lsh.Hole, measurements[index])
				csvdata := utils.CSVData(qry)
				mongo.UploadCSVToMongo("test", "csv", uri, csvfilename, csvdata)

				jsonfilename := utils.MakeName("json", lsh.Leg, lsh.Site, lsh.Hole, measurements[index])
				// version 1
				err := callToMakeJSON(measurements[index], qry, uri, jsonfilename, "test", "jsonld")
				if err != nil {
					log.Printf("janus sql template creation failed: %s", err)
				}

				metastruct := newModels("core_summary")
				csvwmeta := metadata.CSVMetadata(metastruct, measurements[index], csvfilename, uri, qry)
				mongo.UploadCSVW("test", "csvwmeta", uri, csvwmeta)
				schemameta := metadata.SchemaOrgDataset(metastruct, strconv.FormatFloat(lsh.Latitude_degrees, 'f', 2, 64), strconv.FormatFloat(lsh.Longitude_degrees, 'f', 2, 64), measurements[index], csvfilename, uri, qry)
				mongo.UploadSchemaOrg("test", "schemaorg", uri, schemameta)

			} else {
				log.Printf("EMPTY Event: %s %s_%s%s  %s\n", measurements[index], lsh.Leg, lsh.Site, lsh.Hole, qry)
			}
		}
	}
}

// will need a case for each measurements[index]
func newModels(c string) interface{} {
	switch c {
	case "core_summary":
		return CoreSummaryModel()
	case "age_datapoint":
		return AgeDataPointModel()
	}
	return nil
}

func callToMakeJSON(c string, qry string, uri string, filename string, database string, collection string) error {
	switch c {
	case "core_summary":
		err := CoreSummary(qry, uri, filename, "test", "jsonld")
		return err
	case "age_datapoint":
		err := AgeDataPoint(qry, uri, filename, "test", "jsonld")
		return err
	}
	return nil
}
