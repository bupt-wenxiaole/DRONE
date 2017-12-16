package tools

import (
	"bytes"
	"github.com/Alluxio/alluxio-go"
	"github.com/Alluxio/alluxio-go/option"
	"time"
	//"strings"
	"fmt"
	//"log"
	"io"
	//"io/ioutil"
	"log"
)

func GenerateAlluxioClient(host string) *alluxio.Client {
	fs := alluxio.NewClient(host, 39999, time.Minute)
	return fs
}

func RemoveFile(fs *alluxio.Client, path string) error {
	err := fs.Delete(path, &option.Delete{})
	return err
}

// every element in data will be written as one line
func WriteToAlluxio(fs *alluxio.Client, path string, data []string) (bool, error) {
	RemoveFile(fs, path)
	writeId, err := fs.CreateFile(path, &option.CreateFile{})
	if err != nil {
		return false, err
	}
	defer fs.Close(writeId)

	b := bytes.NewBuffer(make([]byte, 0))
	for _, line := range data {
		fmt.Fprintln(b, line)
		if b.Len() > MaxBufferSize {
			_, err = fs.Write(writeId, b)
			if err != nil {
				return false, err
			}

			b = bytes.NewBuffer(make([]byte, 0))
		}
	}
	if b.Len() != 0 {
		_, err = fs.Write(writeId, b)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func ReadFromAlluxio(fs *alluxio.Client, path string) (io.ReadCloser, error) {
	readId, err := fs.OpenFile(path, &option.OpenFile{})
	if err != nil {
		return nil, err
	}
	defer fs.Close(readId)

	read, err := fs.Read(readId)
	if err != nil {
		log.Fatal(err)
	}

	return read, nil
}
