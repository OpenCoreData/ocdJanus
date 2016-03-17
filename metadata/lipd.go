package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	// "strings"
	// jsonld "github.com/linkeddata/gojsonld"
)

// ref http://play.golang.org/p/ODF2CA1PAz
func LiPD(value interface{}, measurement string, filename string,
	uri string, md5string string) string {
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

	fmt.Println(md5string)

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
