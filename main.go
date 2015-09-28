package main

import (
	"gopkg.in/rana/ora.v3"
	// "opencoredata.org/ocdJanus/measurements"
	"opencoredata.org/ocdJanus/setbuilder"
)

func main() {
	setbuilder.LHSLopper()
}

func init() {
	ora.Register(nil)
}
