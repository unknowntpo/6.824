package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
)

const nReduce = 10

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.NewRPCCoordinator(os.Args[1:], nReduce)
	go m.Serve()
	args := &mr.WordCountArgs{FileNames: os.Args[1:]}
	reply := &mr.WordCountReply{}
	if err := m.WordCount(args, reply); err != nil {
		fmt.Fprintf(os.Stderr, "failed on m.WordCount: %v", err)
		return
	}

	for !m.Done() {
		time.Sleep(500 * time.Millisecond)
	}
}
