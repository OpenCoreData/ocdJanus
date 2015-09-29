package measurements

import (
	"bytes"
	"fmt"
	"log"
	"opencoredata.org/ocdJanus/connect"
	"opencoredata.org/ocdJanus/mongo"
	"os"
	"text/template"
)

type LSH struct {
	Leg         string
	Site        string
	Hole        string
	Measurement string
}

func Measurements(leg string, site string, hole string) {
	log.Printf("Measurements for %s_%s%s", leg, site, hole)

	// array of measurements
	measurements := []string{"age_datapoint", "age_profile", "chem_carb", "core_image", "core_summary",
		"dht_apct", "gra_section", "icp_sample", "mad_section", "ms2f_section", "mscl_section", "msl_section",
		"ngr_section", "paleo_image", "paleo_sample", "prime_data_image", "pwl_section", "pws1_section",
		"pws2_section", "pws3_section", "sample", "sed_thin_section_sample", "shear_strength_tor",
		"smear_slide", "tensor_core", "thermal_conductivity", "thin_section_image", "vcd_hard_rock_image",
		"vcd_image", "vcd_structure_image", "xrd_image", "xrf_sample"}
	queryString := []string{"SELECT * FROM ocd_age_datapoint", "SELECT * FROM ocd_age_profile",
		"SELECT * FROM ocd_chem_carb", "SELECT * FROM ocd_core_image", "SELECT * FROM ocd_core_summary",
		"SELECT * FROM ocd_dht_apct", "SELECT * FROM ocd_gra_section", "SELECT * FROM ocd_icp_sample",
		"SELECT * FROM ocd_mad_section", "SELECT * FROM ocd_ms2f_section", "SELECT * FROM ocd_mscl_section",
		"SELECT * FROM ocd_msl_section", "SELECT * FROM ocd_ngr_section", "SELECT * FROM ocd_paleo_image",
		"SELECT * FROM ocd_paleo_sample", "SELECT * FROM ocd_prime_data_image", "SELECT * FROM ocd_pwl_section",
		"SELECT * FROM ocd_pws1_section", "SELECT * FROM ocd_pws2_section", "SELECT * FROM ocd_pws3_section",
		"SELECT * FROM ocd_sample", "SELECT * FROM ocd_sed_thin_section_sample",
		"SELECT * FROM ocd_shear_strength_tor", "SELECT * FROM ocd_smear_slide", "SELECT * FROM ocd_tensor_core",
		"SELECT * FROM ocd_thermal_conductivity", "SELECT * FROM ocd_thin_section_image",
		"SELECT * FROM ocd_vcd_hard_rock_image", "SELECT * FROM ocd_vcd_image",
		"SELECT * FROM ocd_vcd_structure_image", "SELECT * FROM ocd_xrd_image",
		"SELECT * FROM ocd_xrf_sample"}

	conn, err := connect.GetJanusCon()
	defer conn.Close()

	if err != nil {
		panic(err)
	}

	for index, each := range queryString {
		const qrytmp = "{{.Measurement}} WHERE leg = {{.Leg}} and site = {{.Site}} and hole = '{{.Hole}}'"

		// create the SQL call from a template
		lshset := LSH{leg, site, hole, each}
		var buff = bytes.NewBufferString("")
		t, err := template.New("sql template").Parse(qrytmp)
		if err != nil {
			log.Printf("janus sql template creation failed: %s", err)
		}
		err = t.Execute(buff, lshset) //  instead of os.Stdout create a function to call.
		qry := string(buff.Bytes())

		// log.Printf("Qry string %s", qry)

		// This is a dump check..  this whole function needs to be redone
		rowscheck, err := conn.Query(qry)
		if err != nil {
			log.Printf(`Error with "%s": %s`, qry, err)
			return
		}
		len := 0
		for rowscheck.Next() {
			len = len + 1
		}

		if len > 0 {
			// get a URI to work with

			// TODO...    make a case switch on * to remove it from the URL
			// if it there.  Just leave the URL name with a blank

			uri := mongo.AuthorURI(leg, site, hole, measurements[index])
			log.Printf("Using URI %s", uri)

			rows, err := conn.Query(qry)
			if err != nil {
				log.Printf(`Error with "%s": %s`, qry, err)
				return
			}

			// TODO...    make a case switch on * to remove it from the filename
			// if it there.  Just leave the file name with a blank

			csvwfilename := fmt.Sprintf("%s_%s%s_%s.csv", leg, site, hole, measurements[index])
			fcsvw, _ := os.Create("./output/" + csvwfilename)
			defer fcsvw.Close()
			// dumpCSVW(rows, fcsvw)  // can only use one of these...  for stupid row issue...
			mongoCSVW(rows, uri, csvwfilename)

			// this double call is stupid..  remove this code and make this section correct
			rows2, err := conn.Query(qry)
			if err != nil {
				log.Printf(`Error with "%s": %s`, qry, err)
				return
			}
			jsonfilename := fmt.Sprintf("%s_%s%s_%s.json", leg, site, hole, measurements[index])
			fjson, _ := os.Create("./output/" + jsonfilename)
			defer fjson.Close()
			dumpJSON(rows2, fjson)
		} else {
			log.Printf("No dataset found for %s %s %s %s", leg, site, hole, each)
		}
	}
}
