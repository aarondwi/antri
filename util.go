package main

import (
	"os"
	"sort"
	"strings"

	"github.com/aarondwi/antri/ds"
)

func indexOfPqItemWithTheGivenKey(itemPlaceholder []*ds.PqItem, key string) int {
	pos := -1
	for i, item := range itemPlaceholder {
		if item.Key == key {
			return i
		}
	}
	return pos
}

// basically, just helper function to split the antri's filename
// and return the last part
func fileSequenceNumberAsString(filename string) string {
	filenameSeparated := strings.Split(filename, "-")
	return filenameSeparated[len(filenameSeparated)-1]
}

// get all files matching the `word`, until just before the `limit`
func sortedListOfItemInDirMatchingARegex(files []os.FileInfo, wordToMatch, limit string) []string {
	result := []string{}
	for _, f := range files {
		if strings.Contains(f.Name(), wordToMatch) &&
			!f.IsDir() &&
			strings.Compare(f.Name(), limit) == -1 {
			result = append(result, f.Name())
		}
	}

	sort.Strings(result)
	return result
}
