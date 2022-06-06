package main

import (
	"fmt"
	"testing"
	"time"
)

func Test_sendToLoki(t *testing.T) {
	entry := []string{fmt.Sprint(time.Now().UnixNano()), "test dayo-"}
	entries := [][]string{entry}
	err := sendToLoki(entries)
	if err != nil {
		t.Error(err)
	}
}
