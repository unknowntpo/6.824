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

	"6.824/mr"

	"github.com/rs/zerolog"
)

const nReduce = 10

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	m := mr.NewRPCCoordinator(os.Args[1:], nReduce)
	args := &mr.WordCountArgs{FileNames: os.Args[1:]}
	reply := &mr.WordCountReply{}
	if err := m.NewWordCount(args, reply); err != nil {
		fmt.Fprintf(os.Stderr, "failed on m.WordCount: %v", err)
		return
	}
}
