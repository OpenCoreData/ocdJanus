package mongo

import (
	"fmt"
	"github.com/twinj/uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

type Uriurl struct {
	Uri string
	Url string
}

func AuthorURI(leg string, site string, hole string, measurement string) string {
	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a Strong behavior.
	session.SetMode(mgo.Strong, true)
	c := session.DB("test").C("uniqueids")

	// edge case check for * and just drop it if it exist....
	URL := fmt.Sprintf("http://opencoredata.org/doc/dataset/%v/%v/%v/%v", measurement, leg, site, hole)

	// check for existing URI for this URL and return it if there is one
	num, err := c.Find(bson.M{"url": URL}).Count()
	if err != nil {
		log.Fatal(err)
	}

	// log.Println(num)

	// check if the the result exist..  if so, return it..  not a new one
	if num == 0 {
		uuid := uuid.NewV4()
		if err != nil {
			log.Printf("Create time uuid failed: %s", err)
		}
		URI := fmt.Sprintf("http://opencoredata.org/id/dataset/%v", uuid.String())

		pair := Uriurl{URI, URL}
		err = c.Insert(&pair)
		if err != nil {
			log.Fatal(err)
		}
		session.Close()
		return (URI)

	} else {
		existing := Uriurl{}
		c.Find(bson.M{"url": URL}).One(&existing)
		session.Close()
		return (existing.Uri)
	}

}
