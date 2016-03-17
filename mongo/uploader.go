package mongo

import (
	"encoding/json"
	"gopkg.in/mgo.v2"
	"log"
	"opencoredata.org/ocdJanus/metadata"
)

func UploadCSVToMongo(database string, collection string, URI string, filename string, data []byte, mgoconn *mgo.Session) error {
	mgoconn.SetMode(mgo.Strong, true)
	db := mgoconn.DB(database)

	file, err := db.GridFS("fs").Create(filename)
	n, err := file.Write(data)
	err = file.Close()

	// Write a log type message
	log.Printf("File: %s  written with %d bytes\n", filename, n)

	if err != nil {
		log.Fatalf("In UploadCSVToMongo  with %v\n", err)

	}

	return nil
}

// do this as a embedded fucntion I return and use in the function remotely
func UploadSchemaOrg(database string, collection string, URI string, data string, mgoconn *mgo.Session) error {
	mgoconn.SetMode(mgo.Strong, true)
	c := mgoconn.DB(database).C(collection)

	res := metadata.SchemaOrgMetadata{}
	json.Unmarshal([]byte(data), &res)

	err := c.Insert(&res)
	if err != nil {
		log.Fatalf("In UploadSchemaOrg  with %v\n", err)
	}

	return nil
}

func UploadCSVW(database string, collection string, URI string, data string, mgoconn *mgo.Session) error {
	mgoconn.SetMode(mgo.Strong, true)
	c := mgoconn.DB(database).C(collection)

	res := metadata.CSVWMeta{}
	json.Unmarshal([]byte(data), &res)

	err := c.Insert(&res)
	if err != nil {
		log.Fatalf("In UploadCSVW  with %v\n", err)
	}

	return nil
}
