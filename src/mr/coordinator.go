package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	JobQueue *JobQueue
	MailBox  CoorMailBox
	doneChan chan struct{}
}

type JobQueue struct {
	ch chan Job
}

func NewLocalJobQueue(capacity int) *JobQueue {
	return &JobQueue{ch: make(chan Job, capacity)}
}

func (j *JobQueue) Submit(job Job) error {
	j.ch <- job
	return nil
}

func (j *JobQueue) GetJob() (Job, error) {
	job := <-j.ch
	return job, nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WordCount(args *WordCountArgs, reply *WordCountReply) error {
	for _, fileName := range args.FileNames {
		// assign job for every file
		job := NewJob(fileName, TYPE_MAP)
		if err := c.JobQueue.Submit(job); err != nil {
			return err
		}
	}
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Wait() {
	// wait until job finished
	// there's monitor goroutine who monitor the job progress,
	// when job is done, close doneChan
	<-c.doneChan
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) Serve() {
	c.MailBox.Serve()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.MailBox.Done()
}

func (c *Coordinator) GetJobs(id WorkerID) ([]Job, error) {
	// TODO: Take batch of jobs from jobQueue
	j, err := c.JobQueue.GetJob()
	if err != nil {
		return nil, fmt.Errorf("failed on c.JobQueue.GetJob: %v", err)
	}
	return []Job{j}, nil
}

const DefaultJobQueueCap = 10

func NewLocalCoordinator() *Coordinator {
	c := &Coordinator{}
	m := &localMailBox{coorService: c}
	c.MailBox = m
	c.JobQueue = NewLocalJobQueue(DefaultJobQueueCap)
	c.Serve()

	return c
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewRPCCoordinator(files []string, nReduce int) *Coordinator {
	m := &rpcMailBox{}
	c := Coordinator{MailBox: m}

	// Your code here.

	c.Serve()
	return &c
}

type CoorMailBox interface {
	Serve()
	GetJobs(id WorkerID) ([]Job, error)
	Done() bool
	Example(args *ExampleArgs, reply *ExampleReply) error
}

type localMailBox struct {
	coorService *Coordinator
}

func (l *localMailBox) Serve() {
	return
}
func (l *localMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	jobs, err := l.coorService.GetJobs(workerID)
	if err != nil {
		return nil, fmt.Errorf("failed on l.coorService.GetJobs: %v", err)
	}
	return jobs, nil
}

func (l *localMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return true
}

func (l *localMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
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

func (r *rpcMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	return []Job{}, nil
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
