package metadata

import (
	"bytes"
	"fmt"
	"log"
	"text/template"
)

type GLDescription struct {
	OwlSameAs          string
	Title              string
	HasAbstract        string
	Type               string
	HasRepository      string
	HasProject         string
	HasCruise          string
	HasMeasurementType []string // needs to be an array
	HasLandingPage     string
	HasPart            string
	GLDownload         GLDownload
}

type GLDownload struct {
	Source         string
	HasRepository  string
	HasLandingPage string
	HasCruise      string
	HasInstrument  string
	HasFormat      string // link to blank node?
}

const datasetTemplate = `
<{{.GLDownload.Source}}>
    geolink:hasAbstract "IODP scientific ocean drilling dataset";
    geolink:hasCruise <{{.GLDownload.HasCruise}}> ;
    geolink:hasLandingPage "{{.GLDownload.HasLandingPage}}"^^xsd:anyURI ;
    geolink:hasMeasurementType <http://data.oceandrilling.org/id/measurement/12>;
    geolink:hasPart [
        geolink:hasCruise <{{.HasCruise}}> ;
        geolink:hasFormat [
            geolink:hasFormatType "text/tab-separated-values"^^xsd:string ;
            a geolink:Format
        ] ;
        geolink:hasLandingPage "{{.GLDownload.Source}}"^^xsd:anyURI ;
        geolink:hasRepository <{{.HasRepository}}> ;
        geolink:source "{{.GLDownload.Source}}/csv"^^xsd:anyURI ;
        a geolink:DigitalObject
    ] ;
    geolink:hasProject "{{.HasProject}}"@en-us ;
    geolink:hasRepository <{{.HasRepository}}> ;
    geolink:title "{{.Title}}"@en-us ;
    a geolink:Dataset .
    `

func GeoLink(value interface{}, latitude string, longitude string, measurement string, filename string,
	uri string, leg string, site string, hole string, md5string string) string {

	downloaddata := GLDownload{Source: uri,
		HasRepository:  "http://data.geolink.org/id/opencoredata/organization/XXX",
		HasLandingPage: uri,
		HasCruise:      fmt.Sprintf("http://data.oceandrilling.org/id/iodp/cruise/v1/%s", leg),
		HasFormat:      "link to blank node?"}

	owlSameAs := uri
	title := "Data set for Leg, Site, Hole, measurement"
	hasAbstract := "this"
	hastype := "this"
	hasRepository := "http://data.geolink.org/id/ocd/organization/XXX"
	hasProject := "IODP"
	hasCruise := fmt.Sprintf("http://data.oceandrilling.org/id/iodp/cruise/v1/%s", leg)
	var hasMeasurementType []string
	hasMeasurementType = append(hasMeasurementType, "this")
	hasLandingPage := uri
	hasPart := "this"

	description := GLDescription{OwlSameAs: owlSameAs, Title: title, HasAbstract: hasAbstract,
		Type: hastype, HasRepository: hasRepository, HasProject: hasProject, HasCruise: hasCruise,
		HasMeasurementType: hasMeasurementType, HasLandingPage: hasLandingPage, HasPart: hasPart,
		GLDownload: downloaddata}

	// parse template
	ct, err := template.New("RDF template").Parse(datasetTemplate)
	if err != nil {
		log.Printf("RDF template creation failed for cruise: %s", err)
	}

	var buff = bytes.NewBufferString("")
	err = ct.Execute(buff, description)
	if err != nil {
		log.Printf("RDF template execution failed: %s", err)
	}

	// will need a turtle template to use and then save this to a master file

	// geolinkJSON, _ := json.MarshalIndent(description, "", " ")

	//fmt.Printf("\n%s\n", string(buff.Bytes()))

	return string(buff.Bytes())
}
