package utils

import (
	"os"
)

func GetWd() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return dir
}
