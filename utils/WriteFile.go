package utils

import (
	"fmt"
	"log"
	"os"
)

func check(e error) {
	if e != nil {
		log.Print(e)
	}
}

func WriteFile(filename string, data []byte) error {

	filepath := fmt.Sprintf("./output/%s", filename)
	f, err := os.Create(filepath)
	check(err)

	defer f.Close()

	n2, err := f.Write(data)
	check(err)
	log.Printf("Wrote file %s with %d bytes\n", filepath, n2)

	f.Close()

	return nil
}
