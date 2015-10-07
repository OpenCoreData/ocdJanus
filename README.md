#README.md



*export CGO_CFLAGS=-I/Users/dfils/src/oracle/instantclient_11_2/sdk/include
*export CGO_LDFLAGS="-L/Users/dfils/src/oracle/instantclient_11_2 -lclntsh"
*export DYLD_LIBRARY_PATH=/Users/dfils/src/oracle/instantclient_11_2:$DYLD_LIBRARY_PATH

// TODO..  the error is due to the fact I never unmarshal the data...
  // so the encode is never called...
  // data, err := bson.Marshal(&final)
  // this might have to be something the services do...  they will
  // need the structs too!  ocdServices will need res2B, _ := json.MarshalIndent(final, "", " ")


```
type NullFloat64 struct {
  sql.NullFloat64
}

func (nf NullFloat64) MarshalText() ([]byte, error) {
  if nf.Valid {
    nfv := nf.Float64
    return []byte(strconv.FormatFloat(nfv, 'f', -1, 64)), nil
  } else {
    return []byte("null"), nil
  }
}

var _ encoding.TextMarshaler = NullFloat64{}
```

W3C csvw.csv-metadata.json
```
{
  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
  "url": "tree-ops.csv",
  "dc:title": "Tree Operations",
  "dcat:keyword": ["tree", "street", "maintenance"],
  "dc:publisher": {
    "schema:name": "Example Municipality",
    "schema:url": {"@id": "http://example.org"}
  },
  "dc:license": {"@id": "http://opendefinition.org/licenses/cc-by/"},
  "dc:modified": {"@value": "2010-12-31", "@type": "xsd:date"},
  "tableSchema": {
    "columns": [{
      "name": "GID",
      "titles": ["GID", "Generic Identifier"],
      "dc:description": "An identifier for the operation on a tree.",
      "datatype": "string",
      "required": true
    }, {
      "name": "on_street",
      "titles": "On Street",
      "dc:description": "The street that the tree is on.",
      "datatype": "string"
    }, {
      "name": "species",
      "titles": "Species",
      "dc:description": "The species of the tree.",
      "datatype": "string"
    }, {
      "name": "trim_cycle",
      "titles": "Trim Cycle",
      "dc:description": "The operation performed on the tree.",
      "datatype": "string"
    }, {
      "name": "inventory_date",
      "titles": "Inventory Date",
      "dc:description": "The date of the operation that was performed.",
      "datatype": {"base": "date", "format": "M/d/yyyy"}
    }],
    "primaryKey": "GID",
    "aboutUrl": "#gid-{GID}"
  }
}
```


schema.org/Dataset
```
{
  "@context": [
    "http://schema.org",
    {
      "glview": "http://schema.geolink.org/somethingIforgot"
    } 
  ],
  "@type": "Dataset",
  "name": "larval krill pigments",
  "description": "Southern Ocean larval krill studies- fluorescence and clearance, 2001-2002",
  "url": "http://lod.bco-dmo.org/id/dataset/3300",
  "keywords": "cool words about this cool data",
  "distribution": {
    "@type": "DataDownload",
    "encodingFormat": "text/tab-separated-values",
    "contentUrl": "http://www.bco-dmo.org/dataset/3300/data/download",
    "datePublished": "2010-02-03",
    "inLanguage": "en"
  },
  "glview:foo": "Geolink foo",
  "glview:blah": "Geolink blah",
  "spatial": {
    "@type": "Place",
    "geo": {
      "@type": "GeoCoordinates",
      "latitude": "40.75",
      "longitude": "73.98"
    }
  },
  "author": {
    "@type": "Organization",
    "name": "Megadodo Publications",
    "description": "The company headquarters were located on Ursa Minor Beta, in a pair of 30-story office buildings connected partway up their height by a walkway, so that the entire structure resembled a giant letter H.",
    "url": "http://foo.org"
  }
}
```