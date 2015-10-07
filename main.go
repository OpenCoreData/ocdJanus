package main

import (
	"gopkg.in/rana/ora.v3"
	"opencoredata.org/ocdJanus/ngmeasurements"
)

func main() {
	ngmeasurements.MasterLoop()
}

func init() {
	ora.Register(nil)
}
