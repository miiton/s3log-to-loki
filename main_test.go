package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/loki/pkg/logproto"
)

func Test_sendToLoki(t *testing.T) {
	entry := logproto.Entry{Timestamp: time.Now(), Line: "test dayo-"}
	entries := []logproto.Entry{entry}
	err := sendToLoki(entries)
	if err != nil {
		t.Error(err)
	}
}

func Test_parseS3log(t *testing.T) {
	msg := []byte(`bucketowneridexample example-bucket-name [07/Jun/2022:05:39:00 +0000] 192.168.0.1 - D5TRFCQHA762HGZ8 REST.GET.OBJECT keyname.txt "GET /keyname.txt HTTP/1.1" 200 - 123 123 30 29 "-" "Mozilla/5.0 (Linux; Android 12; SC-51A Build/SP1A.210812.016; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/102.0.5005.78 Mobile Safari/537.36" - example/hostid= - ECDHE-RSA-AES128-GCM-SHA256 - example.s3.ap-northeast-1.amazonaws.com TLSv1.2 -`)
	result, err := parseS3log(msg)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(result)
}
