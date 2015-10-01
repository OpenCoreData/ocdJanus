package utils

import (
	"fmt"
	"math/rand"
	"strings"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func MakeName(suffix string, leg string, site string, hole string, measurement string) string {
	// if hole = "*"   remove it...
	filename := fmt.Sprintf("%s_%s%s_%s_%s.%s", leg, site, strings.Replace(hole, "*", "", -1), measurement, randSeq(8), suffix)
	return filename
}

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
