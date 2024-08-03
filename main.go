package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

// Data Structures
type EnumerationResults struct {
	XMLName       xml.Name `xml:"EnumerationResults"`
	ContainerName string   `xml:"ContainerName,attr"`
	MaxResults    int      `xml:"MaxResults"`
	Blobs         []Blob   `xml:"Blobs>Blob"`
	NextMarker    string   `xml:"NextMarker"`
}

type Blob struct {
	XMLName    xml.Name       `xml:"Blob"`
	Name       string         `xml:"Name"`
	Url        string         `xml:"Url"`
	Properties BlobProperties `xml:"Properties"`
}

type BlobProperties struct {
	LastModified  string `xml:"Last-Modified"`
	Etag          string `xml:"Etag"`
	ContentLength int64  `xml:"Content-Length"`
	ContentType   string `xml:"Content-Type"`
	ContentMD5    string `xml:"Content-MD5"`
	BlobType      string `xml:"BlobType"`
}

const (
	listFilesTemplate = "https://%s.blob.core.windows.net/%s?restype=container&comp=list"
)

var (
	path       = flag.String("path", "", "Path to save the files")
	account    = flag.String("account", "", "Azure storage account")
	container  = flag.String("container", "", "Azure storage container")
	maxresults = flag.Int("maxresults", 500, "Page size")
)

func listFiles() (EnumerationResults, error) {
	urlParsed, err := url.Parse(fmt.Sprintf(listFilesTemplate, *account, *container))
	if err != nil {
		return EnumerationResults{}, fmt.Errorf("error parse url: %s", err)
	}

	query := urlParsed.Query()
	query.Add("maxresults", fmt.Sprintf("%d", *maxresults))
	urlParsed.RawQuery = query.Encode()

	log.Printf("Load files from: %s", urlParsed.String())
	resp, err := http.Get(urlParsed.String())
	if err != nil {
		return EnumerationResults{}, fmt.Errorf("error request: %s", err)
	}

	byts, err := io.ReadAll(resp.Body)
	if err != nil {
		return EnumerationResults{}, fmt.Errorf("error read response: %s", err)
	}

	var results EnumerationResults
	if err := xml.Unmarshal(byts, &results); err != nil {
		return EnumerationResults{}, fmt.Errorf("error unmarshal xml: %s", err)
	}

	return results, nil
}

func downloadFile(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	byts, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return byts, nil
}

func main() {
	flag.Parse()

	if *path == "" {
		panic("path argument is missing")
	}

	if *account == "" {
		panic("account argument is missing")
	}

	if *container == "" {
		panic("container argument is missing")
	}

	files, err := listFiles()
	if err != nil {
		panic(err)
	}

	for _, file := range files.Blobs {
		urlParsed, err := url.Parse(file.Url)
		if err != nil {
			panic(fmt.Sprintf("error parse file url(%s): %s", file.Url, err))
		}

		body, err := downloadFile(urlParsed.String())
		if err != nil {
			panic(fmt.Sprintf("error download file(%s): %s", file.Url, err))
		}

		pathToSaveFile := filepath.Join(*path, urlParsed.Path)

		dir := filepath.Dir(pathToSaveFile)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err := os.MkdirAll(dir, 0755)
			if err != nil {
				panic(fmt.Sprintf("error creating directory(%s): %s", dir, err))
			}
		}

		if _, err := os.Stat(pathToSaveFile); os.IsNotExist(err) {
			err = os.WriteFile(pathToSaveFile, body, os.ModeTemporary)
			if err != nil {
				panic(fmt.Sprintf("error save file(%s): %s", file.Url, err))
			}
		} else {
			fmt.Printf("File: %s is already downloaded\n", pathToSaveFile)
		}

	}
}
