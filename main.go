package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var sess *session.Session
var svc *s3.S3

type Stream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type Payload struct {
	Streams []Stream `json:"streams"`
}

func sendToLoki(entries [][]string) error {
	var streams []Stream
	s := &Stream{
		Stream: map[string]string{
			"source": "s3_access_log",
			"job":    "lambda",
			"host":   "lambda",
		},
		Values: entries,
	}
	streams = append(streams, *s)
	p := &Payload{
		Streams: streams,
	}
	j, err := json.Marshal(p)
	if err != nil {
		log.Println(err)
		return err
	}
	u, err := url.Parse(os.Getenv("LOKI_URL"))
	if err != nil {
		log.Println(err)
		return err
	}
	u.Path = path.Join(u.Path, "loki", "api", "v1", "push")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(j))
	if err != nil {
		log.Println(err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", os.Getenv("LOKI_TENANT_ID"))

	client := &http.Client{}

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}
	defer res.Body.Close()
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func genSession() {
	var err error
	sess, err = session.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	svc = s3.New(sess)
}

func handler(ctx context.Context, event events.S3Event) (interface{}, error) {
	var err error
	var entries [][]string
	for _, record := range event.Records {
		log.Println("process start:", record.S3.Object.Key)
		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(record.S3.Bucket.Name),
			Key:    aws.String(record.S3.Object.Key),
		})
		if err != nil {
			log.Fatal(err)
		}

		r := obj.Body
		reader := bufio.NewReader(r)
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			e := []string{fmt.Sprint(time.Now().UnixNano()), string(line)}
			entries = append(entries, e)
		}
		r.Close()
	}

	err = sendToLoki(entries)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	resp := &struct {
		StatusCode uint `json:"statusCode"`
	}{StatusCode: 200}
	return resp, nil
}

func init() {
	genSession()
}

func main() {
	lambda.Start(handler)
}
