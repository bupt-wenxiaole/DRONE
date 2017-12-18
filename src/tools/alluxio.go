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
	//"log"
	"os/exec"
	"os"
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

// when read, we pull alluxio file to local fileSystem as buffer, and delete it after read
func ReadFromAlluxio(path, tempDir string) (io.ReadCloser, error) {
	deleteCmd := exec.Command("/usr/bin/rm", tempDir)
	err := deleteCmd.Run()
	if err != nil {
		log.Printf("no such file before, file name:%v\n", tempDir)
	}

	cmd := exec.Command("/opt/alluxio-1.5.0/bin/alluxio", "fs", "copyToLocal", path, tempDir)
	cmd.Run()
	read, err := os.Open(tempDir)
	return read, err
}

func DeleteLocalFile(path string) error {
	deleteCmd := exec.Command("/usr/bin/rm", path)
	err := deleteCmd.Run()
	return err
}
