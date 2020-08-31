package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"

	"time"

	env "github.com/Netflix/go-env"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	serviceName = "traffic-collector"
	maxRequests = 1000
)

type Environment struct {
	Timeout  int    `env:"TIMEOUT"`
	Filename string `env:"FILENAME"`
	S3Bucket string `env:"S3_BUCKET"`
	Port     string `env:"PORT"`
}

type Collector struct {
	S3Bucket string
	Timeout  int
	Filename string
	Logger   logrus.FieldLogger

	// create a buffer channel
	processedRequestData chan Request
	RequestDataList      []Request
	reqDataWaitGroup     sync.WaitGroup

	// s3 stuff
	Context   context.Context
	S3Session *s3.S3
}

// Request represents an HTTP request
type Request struct {
	Header http.Header `json:"headers"`
	Body   string      `json:"body"`
	Method string      `json:"method"`
	Path   string      `json:"path"`
	Query  url.Values  `json:"query"`
	Host   string      `json:"host"`
}

func (c *Collector) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	reqdata := Request{}

	reqdata.Header = make(http.Header)
	for k, v := range r.Header {
		reqdata.Header[k] = v
	}

	reqdata.Method = r.Method
	reqdata.Path = r.URL.Path
	reqdata.Query = r.URL.Query()
	reqdata.Host = r.Host

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		c.Logger.Fatal(errors.Wrap(err, "reading body"))
	}
	reqdata.Body = string(body)

	c.processedRequestData <- reqdata
}

func (c *Collector) collectRequests() {
	for d := range c.processedRequestData {
		c.RequestDataList = append(c.RequestDataList, d)

		if len(c.RequestDataList) >= maxRequests {
			c.writeRequestsToFile()
		}
	}
}

func (c *Collector) writeRequestsToFile() error {
	time := time.Now()
	jsonData, err := json.MarshalIndent(c.RequestDataList, "", " ")
	if err != nil {
		return errors.Wrap(err, "marshaling request data")
	}

	if c.S3Bucket != "" {
		// write to s3 bucket
		key := fmt.Sprintf("%s/%s.json",
			time.Format("20060102"),
			time.Format("150405"),
		)
		_, err := c.S3Session.PutObjectWithContext(
			c.Context,
			&s3.PutObjectInput{
				ACL:         aws.String("private"),
				Bucket:      aws.String(c.S3Bucket),
				Body:        bytes.NewReader(jsonData),
				ContentType: aws.String("application/json"),
				Key:         aws.String(key),
			},
		)
		if err != nil {
			return errors.Wrap(err, "s3 put object")
		}
	} else {
		fileName := fmt.Sprintf("%s_%s.json", c.Filename, time.Format("150405"))
		c.Logger.Infof("writing to file: %s ", fileName)

		f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}

		_, err = io.WriteString(f, string(jsonData))
		if err != nil {
			return errors.Wrap(err, "writing to file")
		}
	}

	c.RequestDataList = nil
	return nil
}

func initEnvironment(logger logrus.FieldLogger) Environment {
	defaultEnvironment := Environment{
		Timeout:  5,
		Filename: "sample_data",
		Port:     "8081",
		S3Bucket: "",
	}

	_, err := env.UnmarshalFromEnviron(&defaultEnvironment)
	if err != nil {
		logger.Fatal(errors.Wrap(err, "unmarshaling environ"))
	}
	return defaultEnvironment
}

func main() {
	logger := logrus.WithField("service", serviceName)
	env := initEnvironment(logger)

	awsSession := session.Must(session.NewSession())
	cfg := aws.NewConfig()
	s3Service := s3.New(awsSession, cfg)

	ctx := context.Background()
	collector := &Collector{
		S3Bucket:             env.S3Bucket,
		Filename:             env.Filename,
		processedRequestData: make(chan Request),
		RequestDataList:      make([]Request, 0, maxRequests),
		Logger:               logger,
		S3Session:            s3Service,
		Context:              ctx,
	}

	collector.reqDataWaitGroup.Add(1)
	go collector.collectRequests()

	osChannel := make(chan os.Signal, 1)
	signal.Notify(osChannel, os.Interrupt)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		oscall := <-osChannel
		logger.Printf("received system call:%+v..", oscall)

		logger.Printf("remaining items in buffer %d..", len(collector.RequestDataList))
		if len(collector.RequestDataList) > 0 {
			collector.writeRequestsToFile()
		}

		defer wg.Done()
		defer collector.reqDataWaitGroup.Done()
		os.Exit(0)
	}()

	logger.Infof("listening and serving at port %s..", env.Port)
	logrus.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", env.Port), collector))

	wg.Wait()
}
