package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

const nReduce = 10

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.NewRPCCoordinator(os.Args[1:], nReduce)
	go m.Serve()
	for m.Done() == false {
		time.Sleep(time.Second)
	}
}
