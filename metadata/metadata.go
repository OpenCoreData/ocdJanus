package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
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
	Context      []interface{} `json:"@context"`
	Type         string        `json:"@type"`
	Author       Author        `json:"author"`
	Description  string        `json:"description"`
	Distribution Distribution  `json:"distribution"`
	Glview_blah  string        `json:"glview:blah"`
	Glview_foo   string        `json:"glview:foo"`
	Keywords     string        `json:"keywords"`
	Name         string        `json:"name"`
	Spatial      Spatial       `json:"spatial"`
	URL          string        `json:"url"`
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

func CSVMetadata(value interface{}) string {
	// func reflect(x struct) nil {

	d := []Columns{}
	var t Columns

	val := reflect.ValueOf(value).Elem()
	for i := 0; i < val.NumField(); i++ {
		// valueField := val.Field(i)
		typeField := val.Type().Field(i)
		// tag := typeField.Tag
		dnString := fmt.Sprintf("%v", typeField.Name)
		dtString := fmt.Sprintf("%v", typeField.Type)
		t = Columns{Datatype: dtString, Name: dnString}
		d = append(d, t)

		// fmt.Printf("Field Name: %s,\t Field Value: %v,\t Tag Value: %s and %s\n", typeField.Name, valueField.Interface(), tag.Get("tag_name"), tag.Get("units"))
	}

	tableschema := TableSchema{AboutURL: "URL", Columns: d, PrimaryKey: "pkey"}
	schema_url := Schema_url{Id: "id value"}
	dc_publisher := Dc_publisher{Schema_url: schema_url, Schema_name: "the schema name"}
	dc_modified := Dc_modified{Type: "type value", Value: "go to the value"}
	dc_license := Dc_license{Id: "this is the ID"}

	keywords := []string{"keyword 1", "keyword 2"}

	csvwmeta := CSVWMeta{Context: "String value", Dc_license: dc_license, Dc_modified: dc_modified, Dc_publisher: dc_publisher, Dc_title: "title here", Dcat_keyword: keywords, TableSchema: tableschema, URL: "url string"}
	cvmeatajson, _ := json.MarshalIndent(csvwmeta, "", " ")
	return string(cvmeatajson)

	// return "Return JSON string for this csvw metadata"
}

func SchemaOrgDataset(value interface{}) string {
	// set up some of our boiler plate schema.org/Dataset elements
	// need date publishedOn, URL, lat long

	geodata := Geo{Type: "GeoCoordinates", Latitude: "111", Longitude: "80"}
	spatial := Spatial{Type: "Place", Geo: geodata}
	distribution := Distribution{Type: "DataDownload", ContentURL: "http://www.bco-dmo.org/dataset/3300/data/download", DatePublished: "2010-02-03", EncodingFormat: "text/tab-separated-values", InLanguage: "en"}
	author := Author{Type: "Dataset", Description: "Author set description", Name: "Data set name", URL: "http://iodp.org"}

	// contextArray := []interface{"http://schema.org", {"glview": "http://schema.geolink.org/somethingIforgot"}}

	schemametadata := SchemaOrgMetadata{Type: "Dataset", Author: author, Description: "Data set description", Distribution: distribution, Glview_blah: "stuff here", Glview_foo: "stuff here", Keywords: "keywords here", Name: "Data set name", Spatial: spatial, URL: "http://foo.org"}
	// schemametadata := SchemaOrgMetadata{Context:  ["http://schema.org", {"glview": "http://schema.geolink.org/somethingIforgot"}], Type: "Dataset"}

	schemaorgJSON, _ := json.MarshalIndent(schemametadata, "", " ")

	return string(schemaorgJSON)
}
