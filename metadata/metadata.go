package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	// "strings"
)

// W3c csvw metadata structs
type CSVWMeta struct {
	Context      string       `json:"@context"`
	Dc_license   Dc_license   `json:"dc:license"`
	Dc_modified  Dc_modified  `json:"dc:modified"`
	Dc_publisher Dc_publisher `json:"dc:publisher"`
	Dc_title     string       `json:"dc:title"`
	Dcat_keyword []string     `json:"dcat:keyword"`
	TableSchema  TableSchema  `json:"tableSchema"`
	URL          string       `json:"url"`
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
	Context         []interface{} `json:"@context"`
	Type            string        `json:"@type"`
	Author          Author        `json:"author"`
	Description     string        `json:"description"`
	Distribution    Distribution  `json:"distribution"`
	Glview_dataset  string        `json:"glview:dataset"`
	Glview_keywords string        `json:"glview:keywords"`
	Keywords        string        `json:"keywords"`
	Name            string        `json:"name"`
	Spatial         Spatial       `json:"spatial"`
	URL             string        `json:"url"`
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

func CSVMetadata(value interface{}, measurement string, filename string, uri string, qry string) string {
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

	keywords := []string{"DSDP", "ODP", "IODP"}

	context := "context string here" //
	dctitle := filename              // title of the dataset
	url := uri                       // url to download the file

	csvwmeta := CSVWMeta{Context: context, Dc_license: dc_license, Dc_modified: dc_modified, Dc_publisher: dc_publisher, Dc_title: dctitle, Dcat_keyword: keywords, TableSchema: tableschema, URL: url}
	cvmeatajson, _ := json.MarshalIndent(csvwmeta, "", " ")
	return string(cvmeatajson)

}

func SchemaOrgDataset(value interface{}, latitude string, longitude string, measurement string, filename string, uri string, qry string) string {
	// set up some of our boiler plate schema.org/Dataset elements
	// need date publishedOn, URL, lat long

	geodata := Geo{Type: "GeoCoordinates", Latitude: latitude, Longitude: longitude}
	spatial := Spatial{Type: "Place", Geo: geodata}
	timenow := time.Now().Format(time.RFC850)
	distribution := Distribution{Type: "DataDownload", ContentURL: uri, DatePublished: timenow, EncodingFormat: "text/tab-separated-values", InLanguage: "en"}
	author := Author{Type: "Organization", Description: "NSF funded International Ocean Discovery Program operated by JRSO", Name: "International Ocean Discovery Program", URL: "http://iodp.org"}

	// contextArray := []interface{"http://schema.org", {"glview": "http://schema.geolink.org/somethingIforgot"}}

	schemametadata := SchemaOrgMetadata{Type: "Dataset", Author: author, Description: "Data set description", Distribution: distribution, Glview_dataset: filename, Glview_keywords: "DSDP, ODP, IODP", Keywords: "DSDP, ODP, IODP", Name: filename, Spatial: spatial, URL: uri}
	// schemametadata := SchemaOrgMetadata{Context:  ["http://schema.org", {"glview": "http://schema.geolink.org/somethingIforgot"}], Type: "Dataset"}

	schemaorgJSON, _ := json.MarshalIndent(schemametadata, "", " ")

	return string(schemaorgJSON)
}
