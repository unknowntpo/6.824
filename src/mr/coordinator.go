package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mailBox MailBox
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	c.mailBox.Serve()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mailBox.Done()
}

func NewLocalCoordinator() *Coordinator {
	m := &localMailBox{}
	c := Coordinator{mailBox: m}
	c.server()

	return &c
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewRPCCoordinator(files []string, nReduce int) *Coordinator {
	m := &rpcMailBox{}
	c := Coordinator{mailBox: m}

	// Your code here.

	c.server()
	return &c
}

type MailBox interface {
	Serve()
	Done() bool
	Example(args *ExampleArgs, reply *ExampleReply) error
}

type localMailBox struct {
}

func (r *localMailBox) Serve() {
	// init with for select
	return
}

func (r *localMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return true
}

func (r *localMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type rpcMailBox struct {
}

func (r *rpcMailBox) Serve() {
	rpc.Register(r)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (r *rpcMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return true
}

func (r *rpcMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
