package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	// "strings"
	"strconv"
	// jsonld "github.com/linkeddata/gojsonld"
)

// W3c csvw metadata structs
// 	context := `["http://www.w3.org/ns/csvw", {"@language": "en"}]`
type CSVWMeta struct {
	Context      CSVContext   `json:"@context"`
	Dc_license   Dc_license   `json:"dc:license"`
	Dc_modified  Dc_modified  `json:"dc:modified"`
	Dc_publisher Dc_publisher `json:"dc:publisher"`
	Dc_title     string       `json:"dc:title"`
	Dcat_keyword []string     `json:"dcat:keyword"`
	TableSchema  TableSchema  `json:"tableSchema"`
	URL          string       `json:"url"`
}

type CSVContext struct {
	Vocab    string `json:"@vocab"`
	Language string `json:"@language"`
}

type Dc_license struct {
	Id string `json:"@id"`
}

type Dc_modified struct {
	Type  string `json:"@type"`
	Value string `json:"@value"`
}

type Dc_publisher struct {
	Schema_name string     `json:"schema:name"`
	Schema_url  Schema_url `json:"schema:url"`
}

type Schema_url struct {
	Id string `json:"@id"`
}

type TableSchema struct {
	AboutURL   string    `json:"aboutUrl"`
	Columns    []Columns `json:"columns"`
	PrimaryKey string    `json:"primaryKey"`
}

type Columns struct {
	Datatype       string   `json:"datatype"`
	Dc_description string   `json:"dc:description"`
	Name           string   `json:"name"`
	Required       bool     `json:"required"`
	Titles         []string `json:"titles"`
}

// schema.org Dataset metadata structs
type SchemaOrgMetadata struct {
	Context             Context      `json:"@context"`
	Type                string       `json:"@type"`
	Author              Author       `json:"author"`
	Description         string       `json:"description"`
	Distribution        Distribution `json:"distribution"`
	GlviewDataset       string       `json:"glview:dataset"`
	GlviewKeywords      string       `json:"glview:keywords"`
	GlviewMD5           string       `json:"glview:md5"`
	OpenCoreLeg         string       `json:"opencore:leg"`
	OpenCoreSite        string       `json:"opencore:site"`
	OpenCoreHole        string       `json:"opencore:hole"`
	OpenCoreProgram     string       `json:"opencore:program"`
	OpenCoreMeasurement string       `json:"opencore:measurement"`
	Keywords            string       `json:"keywords"`
	Name                string       `json:"name"`
	Spatial             Spatial      `json:"spatial"`
	URL                 string       `json:"url"`
}

// type Context struct {
// 	DefNameSpace string
// 	Namespaces Namespace
// }

// type Namespace struct {

// }

type Context struct {
	Vocab    string `json:"@vocab"`
	GeoLink  string `json:"glview"`
	OpenCore string `json:"opencore"`
}

type Author struct {
	Type        string `json:"@type"`
	Description string `json:"description"`
	Name        string `json:"name"`
	URL         string `json:"url"`
}

type Distribution struct {
	Type           string `json:"@type"`
	ContentURL     string `json:"contentUrl"`
	DatePublished  string `json:"datePublished"`
	EncodingFormat string `json:"encodingFormat"`
	InLanguage     string `json:"inLanguage"`
}

type Spatial struct {
	Type string `json:"@type"`
	Geo  Geo    `json:"geo"`
}

type Geo struct {
	Type      string `json:"@type"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
}

func CSVMetadata(value interface{}, measurement string, filename string, uri string, md5string string) string {
	// func reflect(x struct) nil {

	d := []Columns{}
	var t Columns

	val := reflect.ValueOf(value).Elem()
	for i := 0; i < val.NumField(); i++ {
		// valueField := val.Field(i)
		// tag := typeField.Tag
		typeField := val.Type().Field(i)
		dnString := fmt.Sprintf("%v", typeField.Name)
		dtString := fmt.Sprintf("%v", typeField.Type)

		// todo need a map of golang types to csvwtypes to swap out sql.Null*

		t = Columns{Datatype: dtString, Name: dnString}
		d = append(d, t)
	}

	tableschema := TableSchema{AboutURL: "", Columns: d, PrimaryKey: ""}
	schema_url := Schema_url{Id: "http://opencoredata.org"}
	dc_publisher := Dc_publisher{Schema_url: schema_url, Schema_name: "Open Core Data"}
	timenow := time.Now().Format(time.RFC850)
	dc_modified := Dc_modified{Type: "xsd:date", Value: timenow}
	dc_license := Dc_license{Id: "http://opendefinition.org/licenses/cc-by/"}

	fmt.Println(md5string) // just not doing anything with this right now

	keywords := []string{"DSDP", "ODP", "IODP", measurement}
	csvcontext := CSVContext{Vocab: "http://www.w3.org/ns/csvw/", Language: "en"}

	// context := `["http://www.w3.org/ns/csvw", {"@language": "en"}]`
	dctitle := filename // title of the dataset
	url := uri          // url to download the file

	csvwmeta := CSVWMeta{Context: csvcontext, Dc_license: dc_license, Dc_modified: dc_modified,
		Dc_publisher: dc_publisher, Dc_title: dctitle, Dcat_keyword: keywords, TableSchema: tableschema, URL: url}
	cvmeatajson, _ := json.MarshalIndent(csvwmeta, "", " ")

	// fmt.Printf("\n%s\n", string(cvmeatajson))

	return string(cvmeatajson)

}

func SchemaOrgDataset(value interface{}, latitude string, longitude string, measurement string,
	filename string, uri string, leg string, site string, hole string, md5string string) string {

	// set program based on leg number, make a simple function for this
	program := ProgramName(leg)

	geodata := Geo{Type: "GeoCoordinates", Latitude: latitude, Longitude: longitude}
	spatial := Spatial{Type: "Place", Geo: geodata}
	timenow := time.Now().Format(time.RFC850)
	distribution := Distribution{Type: "DataDownload", ContentURL: uri, DatePublished: timenow,
		EncodingFormat: "text/tab-separated-values", InLanguage: "en"}
	author := Author{Type: "Organization", Description: "NSF funded International Ocean Discovery Program operated by JRSO",
		Name: "International Ocean Discovery Program", URL: "http://iodp.org"}
	keywords := fmt.Sprintf("%s, %s", program, measurement)
	context := Context{Vocab: "http://schema.org/",
		GeoLink:  "http://schema.geolink.org/dev/base/main#",
		OpenCore: "http://opencoredata.org/voc/janus/1/"}
	description := fmt.Sprintf("A %s dataset from %s associated with %s_%s%s", measurement, program, leg, site, hole)
	fmt.Sprintf("Data Set Description  %s", keywords)
	schemametadata := SchemaOrgMetadata{Context: context, Type: "Dataset", Author: author, Description: description,
		Distribution: distribution, GlviewDataset: filename, GlviewKeywords: keywords, GlviewMD5: md5string, OpenCoreLeg: leg,
		OpenCoreSite: site, OpenCoreHole: hole, OpenCoreProgram: program, OpenCoreMeasurement: measurement, Keywords: keywords,
		Name: filename, Spatial: spatial, URL: uri}
	schemaorgJSON, _ := json.MarshalIndent(schemametadata, "", " ")

	// dataparsed, _ := jsonld.ParseDataset(schemaorgJSON)
	// fmt.Printf("Serialized:\n %s \n\n", dataparsed.Serialize())

	// fmt.Printf("\n%s\n", string(schemaorgJSON))

	return string(schemaorgJSON)
}

func ProgramName(leg string) string {
	// convert leg to int
	i, err := strconv.Atoi(leg)
	if err != nil {
		log.Printf("There is an error converting leg to int %s", err)
	}

	// 1-99 DSDP, 101-199 ODP, 201-299 IODP , 301- IODP  have to double check on that ...

	program := ""
	switch i {
	case i > 0 && i < 100:
		program = "Deep Sea Drilling Program (DSDP)"
	case i > 100 && i < 200:
		program = "Ocean Drilling Program (ODP)"
	case i > 200 && i < 300:
		program = "Integrated Ocean Drilling Program (IODP)"
	case i > 300 && i < 400:
		program = "International Ocean Discovery Program (IODP)"
	}

	return program

}
