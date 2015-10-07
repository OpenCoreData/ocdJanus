package ngmeasurements

import (
	"bytes"
	// "fmt"
	// "github.com/kisielk/sqlstruct"
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

	measurements := []string{"JanusAgeDatapoint", "JanusAgeProfile", "JanusChemCarb", "JanusCoreImage",
		"JanusCoreSummary", "JanusCryomagSection", "JanusDhtApct", "JanusGraSection",
		"JanusIcpSample", "JanusMadSection", "JanusMs2fSection", "JanusMsclSection",
		"JanusMslSection", "JanusNcrSection", "JanusNgrSection", "JanusPaleoImage",
		"JanusPaleoOccurrence", "JanusPaleoSample", "JanusPrimeDataImage", "JanusPwlSection",
		"JanusPws1Section", "JanusPws2Section", "JanusPws3Section", "JanusRscSection",
		"JanusSample", "JanusSedThinSectionSample", "JanusShearStrengthAvs",
		"JanusShearStrengthPen", "JanusShearStrengthTor", "JanusSmearSlide",
		"JanusTensorCore", "JanusThermalConductivity", "JanusThinSectionImage",
		"JanusVcdHardRockImage", "JanusVcdImage", "JanusVcdStructureImage",
		"JanusXrdImage", "JanusXrfSample"}

	queryString := []string{"SELECT * FROM ocd_age_datapoint", "SELECT * FROM ocd_age_profile", "SELECT * FROM ocd_chem_carb",
		"SELECT * FROM ocd_core_image", "SELECT * FROM ocd_core_summary", "SELECT * FROM ocd_cryomag_section",
		"SELECT * FROM ocd_dht_apct", "SELECT * FROM ocd_gra_section", "SELECT * FROM ocd_icp_sample",
		"SELECT * FROM ocd_mad_section", "SELECT * FROM ocd_ms2f_section", "SELECT * FROM ocd_mscl_section",
		"SELECT * FROM ocd_msl_section", "SELECT * FROM ocd_ncr_section", "SELECT * FROM ocd_ngr_section",
		"SELECT * FROM ocd_paleo_image", "SELECT * FROM ocd_paleo_occurrence", "SELECT * FROM ocd_paleo_sample",
		"SELECT * FROM ocd_prime_data_image", "SELECT * FROM ocd_pwl_section", "SELECT * FROM ocd_pws1_section",
		"SELECT * FROM ocd_pws2_section", "SELECT * FROM ocd_pws3_section", "SELECT * FROM ocd_rsc_section",
		"SELECT * FROM ocd_sample", "SELECT * FROM ocd_sed_thin_section_sample", "SELECT * FROM ocd_shear_strength_avs",
		"SELECT * FROM ocd_shear_strength_pen", "SELECT * FROM ocd_shear_strength_tor",
		"SELECT * FROM ocd_smear_slide", "SELECT * FROM ocd_tensor_core", "SELECT * FROM ocd_thermal_conductivity",
		"SELECT * FROM ocd_thin_section_image", "SELECT * FROM ocd_vcd_hard_rock_image",
		"SELECT * FROM ocd_vcd_image", "SELECT * FROM ocd_vcd_structure_image", "SELECT * FROM ocd_xrd_image",
		"SELECT * FROM ocd_xrf_sample"}

	// measurements := []string{"age_datapoint", "core_summary"}
	// queryString := []string{"SELECT * FROM ocd_age_datapoint", "SELECT * FROM ocd_core_summary"}

	conn, err := connect.GetJanusCon()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for index, each := range queryString {

		lshqry := queries.Sql_lsh5
		lshrows, err := conn.Query(lshqry)
		if err != nil {
			log.Printf(`Error with "%s": %s`, lshqry, err)
			return
		}

		for lshrows.Next() {
			//var lsh LSH
			// err := sqlstruct.Scan(&lsh, lshrows)

			var (
				legtmp   string
				sitetmp  string
				holetmp  string
				lattemp  float64
				longtemp float64
			)

			err := lshrows.Scan(&legtmp, &sitetmp, &holetmp, &lattemp, &longtemp)
			lsh := LSH{Leg: legtmp, Site: sitetmp, Hole: holetmp, Latitude_degrees: lattemp, Longitude_degrees: longtemp}

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

				metastruct := newModels(measurements[index])
				csvwmeta := metadata.CSVMetadata(metastruct, measurements[index], csvfilename, uri, qry)
				mongo.UploadCSVW("test", "csvwmeta", uri, csvwmeta)
				schemameta := metadata.SchemaOrgDataset(metastruct, strconv.FormatFloat(lsh.Latitude_degrees, 'f', 2, 64), strconv.FormatFloat(lsh.Longitude_degrees, 'f', 2, 64), measurements[index], csvfilename, uri, qry)
				mongo.UploadSchemaOrg("test", "schemaorg", uri, schemameta)

			} else {
				log.Printf("EMPTY Event: %s %s_%s%s  %v  %v   %s\n", measurements[index], lsh.Leg, lsh.Site, lsh.Hole, lsh.Latitude_degrees, lsh.Longitude_degrees, qry)
			}
		}
	}
}

// will need a case for each measurements[index]
func newModels(c string) interface{} {
	switch c {
	case "JanusAgeDatapoint":
		return JanusAgeDatapointModel()
	case "JanusAgeProfile":
		return JanusAgeProfileModel()
	case "JanusChemCarb":
		return JanusChemCarbModel()
	case "JanusCoreImage":
		return JanusCoreImageModel()
	case "JanusCoreSummary":
		return JanusCoreSummaryModel()
	case "JanusCryomagSection":
		return JanusCryomagSectionModel()
	case "JanusDhtApct":
		return JanusDhtApctModel()
	case "JanusGraSection":
		return JanusGraSectionModel()
	case "JanusIcpSample":
		return JanusIcpSampleModel()
	case "JanusMadSection":
		return JanusMadSectionModel()
	case "JanusMs2fSection":
		return JanusMs2fSectionModel()
	case "JanusMsclSection":
		return JanusMsclSectionModel()
	case "JanusMslSection":
		return JanusMslSectionModel()
	case "JanusNcrSection":
		return JanusNcrSectionModel()
	case "JanusNgrSection":
		return JanusNgrSectionModel()
	case "JanusPaleoImage":
		return JanusPaleoImageModel()
	case "JanusPaleoOccurrence":
		return JanusPaleoOccurrenceModel()
	case "JanusPaleoSample":
		return JanusPaleoSampleModel()
	case "JanusPrimeDataImage":
		return JanusPrimeDataImageModel()
	case "JanusPwlSection":
		return JanusPwlSectionModel()
	case "JanusPws1Section":
		return JanusPws1SectionModel()
	case "JanusPws2Section":
		return JanusPws2SectionModel()
	case "JanusPws3Section":
		return JanusPws3SectionModel()
	case "JanusRscSection":
		return JanusRscSectionModel()
	case "JanusSample":
		return JanusSampleModel()
	case "JanusSedThinSectionSample":
		return JanusSedThinSectionSampleModel()
	case "JanusShearStrengthAvs":
		return JanusShearStrengthAvsModel()
	case "JanusShearStrengthPen":
		return JanusShearStrengthPenModel()
	case "JanusShearStrengthTor":
		return JanusShearStrengthTorModel()
	case "JanusSmearSlide":
		return JanusSmearSlideModel()
	case "JanusTensorCore":
		return JanusTensorCoreModel()
	case "JanusThermalConductivity":
		return JanusThermalConductivityModel()
	case "JanusThinSectionImage":
		return JanusThinSectionImageModel()
	case "JanusVcdHardRockImage":
		return JanusVcdHardRockImageModel()
	case "JanusVcdImage":
		return JanusVcdImageModel()
	case "JanusVcdStructureImage":
		return JanusVcdStructureImageModel()
	case "JanusXrdImage":
		return JanusXrdImageModel()
	case "JanusXrfSample":
		return JanusXrfSampleModel()
	}
	return nil
}

func callToMakeJSON(c string, qry string, uri string, filename string, database string, collection string) error {
	switch c {
	case "JanusAgeDatapoint":
		err := JanusAgeDatapointFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusAgeProfile":
		err := JanusAgeProfileFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusChemCarb":
		err := JanusChemCarbFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusCoreImage":
		err := JanusCoreImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusCoreSummary":
		err := JanusCoreSummaryFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusCryomagSection":
		err := JanusCryomagSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusDhtApct":
		err := JanusDhtApctFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusGraSection":
		err := JanusGraSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusIcpSample":
		err := JanusIcpSampleFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusMadSection":
		err := JanusMadSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusMs2fSection":
		err := JanusMs2fSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusMsclSection":
		err := JanusMsclSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusMslSection":
		err := JanusMslSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusNcrSection":
		err := JanusNcrSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusNgrSection":
		err := JanusNgrSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPaleoImage":
		err := JanusPaleoImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPaleoOccurrence":
		err := JanusPaleoOccurrenceFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPaleoSample":
		err := JanusPaleoSampleFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPrimeDataImage":
		err := JanusPrimeDataImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPwlSection":
		err := JanusPwlSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPws1Section":
		err := JanusPws1SectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPws2Section":
		err := JanusPws2SectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusPws3Section":
		err := JanusPws3SectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusRscSection":
		err := JanusRscSectionFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusSample":
		err := JanusSampleFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusSedThinSectionSample":
		err := JanusSedThinSectionSampleFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusShearStrengthAvs":
		err := JanusShearStrengthAvsFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusShearStrengthPen":
		err := JanusShearStrengthPenFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusShearStrengthTor":
		err := JanusShearStrengthTorFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusSmearSlide":
		err := JanusSmearSlideFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusTensorCore":
		err := JanusTensorCoreFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusThermalConductivity":
		err := JanusThermalConductivityFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusThinSectionImage":
		err := JanusThinSectionImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusVcdHardRockImage":
		err := JanusVcdHardRockImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusVcdImage":
		err := JanusVcdImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusVcdStructureImage":
		err := JanusVcdStructureImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusXrdImage":
		err := JanusXrdImageFunc(qry, uri, filename, "test", "jsonld")
		return err
	case "JanusXrfSample":
		err := JanusXrfSampleFunc(qry, uri, filename, "test", "jsonld")
		return err
	}
	return nil
}
