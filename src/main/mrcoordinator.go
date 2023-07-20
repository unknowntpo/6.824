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
	"github.com/rs/zerolog/log"
)

const nReduce = 10

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	file, err := os.OpenFile("log-co.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	must(err)
	defer file.Close()

	multi := zerolog.MultiLevelWriter(os.Stderr, file)

	logger := zerolog.New(multi).With().Timestamp().Logger()
	// Set the global logger to use the configured logger
	log.Logger = logger

	m := mr.NewRPCCoordinator(os.Args[1:], nReduce)
	args := &mr.WordCountArgs{FileNames: os.Args[1:]}
	reply := &mr.WordCountReply{}
	if err := m.NewWordCount(args, reply); err != nil {
		fmt.Fprintf(os.Stderr, "failed on m.WordCount: %v", err)
		return
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
