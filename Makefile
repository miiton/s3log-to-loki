export
GOOS := linux
GOARCH := amd64
CGO_ENABLED := 0

.PHONY: build

build:
	go build
	
pack:
	7z a ./function.zip s3log-to-loki
