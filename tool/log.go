package tool

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
)

type Logs struct {
	File string
}

func (logObj Logs) AddLog(msg string) {
	writer1 := &bytes.Buffer{}
	writer2 := os.Stdout
	writer3, err := os.OpenFile(logObj.File, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("create file log.txt failed: %v", err)
	}
	logrus.SetOutput(io.MultiWriter(writer1, writer2, writer3))
	logrus.Info(msg)
}
