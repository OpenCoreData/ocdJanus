package mongo

import (
	"gopkg.in/mgo.v2"
	"log"
)

// database test, collection jsonld
func UploadCSVToMongo(database string, collection string, URI string, filename string, data []byte) error {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	db := session.DB(database)

	file, err := db.GridFS("fs").Create(filename)
	n, err := file.Write(data)
	err = file.Close()

	// Write a log type message
	log.Printf("%d bytes written to the Mongodb instance\n", n)

	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// do this as a embedded fucntion I return and use in the function remotely
func UploadJSONToMongo(database string, collection string, URI string, data string) error {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(database).C(collection)

	pair := Uriurl{"this", "that"}
	err = c.Insert(&pair)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}
