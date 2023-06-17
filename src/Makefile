## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

TMP_DIR = ./main/mr-tmp

## mr/build/seq: build map-reduce https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
.PHONY: mr/build/seq
mr/build/seq: mr/build/plugin
	# Ref: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
	-rm ./main/mr-out*
	go run ./main/mrsequential.go wc.so ./main/pg*.txt
	mkdir -p $(TMP_DIR)
	cp mr-out-0 $(TMP_DIR)/mr-out-correct
	more $(TMP_DIR)/mr-out-correct

## mr/test/once: run main/test-mr.sh
mr/test/once:
	cd main && bash test-mr.sh

FOCUS ?=
ifeq ($(FOCUS),)
		FOCUSFLAG :=
else
		FOCUSFLAG := -ginkgo.focus=$(FOCUS)
endif

## mr/test/unit: run unit tests for worker and coordinator
mr/test/unit: mr/build/seq
	# we need to call mr/build/seq to build mr-out-correct first
	# to make output of or worker implementation compare to it
	go test -v ./main/mrworker.go ./main/mrworker_test.go $(FOCUSFLAG)

## mr/build/plugin: build wc.so
mr/build/plugin:
	go build -o wc.so -buildmode=plugin ./mrapps/wc.go

