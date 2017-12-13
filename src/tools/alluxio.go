package tools

import (
	"github.com/Alluxio/alluxio-go"
	"time"
	"github.com/Alluxio/alluxio-go/option"
	"bytes"
	//"strings"
	"fmt"
	//"log"
	"io"
)

const maxBufferSize = 1024000

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
		if b.Len() > maxBufferSize {
			_, err = fs.Write(writeId, b)
			if err != nil {
				return false, err
			}

			b = bytes.NewBuffer(make([]byte, 0))
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

	buffer, err := fs.Read(readId)
	if err != nil {
		return nil, err
	}

	buffer, nil
}