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

func sortedListOfFilesInDirMatchingARegex(files []os.FileInfo, wordToMatch string) []string {
	result := []string{}
	for _, f := range files {
		if strings.Contains(f.Name(), wordToMatch) &&
			!f.IsDir() {
			result = append(result, f.Name())
		}
	}

	sort.Strings(result)
	return result
}

// get all files matching the `word`, until just before the `limit`
func sortedListOfFilesInDirMatchingARegexUntilALimit(files []os.FileInfo, wordToMatch, limit string) []string {
	result := []string{}
	for _, f := range sortedListOfFilesInDirMatchingARegex(files, wordToMatch) {
		if strings.Compare(f, limit) == -1 {
			result = append(result, f)
		}
	}

	return result
}
