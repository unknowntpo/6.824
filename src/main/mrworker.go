package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"context"
	"fmt"
	"log"
	"os"
	"plugin"
	"time"

	"6.824/mr"
	"6.824/utils"

	"github.com/rs/zerolog"
	zLog "github.com/rs/zerolog/log"
)

const nReduce = 8

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	zerolog.SetGlobalLevel(zerolog.ErrorLevel)

	mapf, reducef := loadPlugin(os.Args[1])

	localWorker := mr.NewWorker(
		&mr.RPCMailBox{},
		mapf,
		reducef,
		nReduce,
		utils.GetWd(),
	)

	file, err := os.OpenFile(fmt.Sprintf("log-worker-%v.txt", localWorker.ID), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	must(err)
	defer file.Close()

	multi := zerolog.MultiLevelWriter(os.Stderr, file)

	logger := zerolog.New(multi).With().Timestamp().Logger()
	// Set the global logger to use the configured logger
	zLog.Logger = logger

	go localWorker.Serve(context.Background())
	for !localWorker.Done() {
		time.Sleep(500 * time.Millisecond)
	}
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v: %v", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
