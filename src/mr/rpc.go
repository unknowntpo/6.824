package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetJobsArgs struct {
	ReqID    ReqID
	WorkerID WorkerID
}

type GetJobsReply struct {
	Jobs []Job
	Err  error
}

type FinishJobsArgs struct {
	ReqID    ReqID
	WorkerID WorkerID
	JobID    JobID
}

type FinishJobsReply struct {
	Err error
}

type MarkHealthyArgs struct {
	ReqID    ReqID
	WorkerID WorkerID
}

type MarkHealthyReply struct {
	Err error
}

type CheckHealthyArgs struct {
	ReqID ReqID
}

type CheckHealthyReply struct {
	Err error
}

// Add your RPC definitions here.
type WordCountArgs struct {
	X         int
	FileNames []string
}

type WordCountReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
