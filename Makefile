
export GOPATH := $(GOPATH):$(PWD)

SRC=src/batch/*.go

.PHONY: all deps test

all: test

deps:

test:
	go test batch -test.v

