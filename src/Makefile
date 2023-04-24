## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'


## mr/build/seq: build map-reduce https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
.PHONY: mr/build/seq
mr/build/seq:
	# Ref: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html
	go build -o wc.so -buildmode=plugin ./mrapps/wc.go
	-rm ./main/mr-out*
	go run ./main/mrsequential.go wc.so ./main/pg*.txt
	more mr-out-0
