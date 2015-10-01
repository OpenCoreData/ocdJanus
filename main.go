package main

import (
	"gopkg.in/rana/ora.v3"
	"opencoredata.org/ocdJanus/ngmeasurements"
)

func main() {
	ngmeasurements.CoreSummary()
}

func init() {
	ora.Register(nil)
}
