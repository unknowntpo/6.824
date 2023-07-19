package mr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	// "github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Coordinator struct {

	// Your definitions here.
	JobQueue  *JobQueue
	MailBox   CoorMailBox
	workerMap *WorkerMap
	// Shouldn't be changed after init
	nReduce int

	mapJobWaitGroup    sync.WaitGroup
	reduceJobWaitGroup sync.WaitGroup
	cleanupWaitGroup   sync.WaitGroup

	phase   Phase
	phaseMu sync.RWMutex

	// dead map
	deadMap deadMap

	jobEventCh chan JobEvent
	healthCh   chan HealthEvent
}

type deadMap map[WorkerID]bool

func (dm *deadMap) String() string {
	return debug(dm)
}

func (dm *deadMap) MarkHealthy(workerID WorkerID) error {
	return dm.markWorkerStatus(workerID, false)
}

func (dm deadMap) markWorkerStatus(workerID WorkerID, isDead bool) error {
	// false means not dead
	if _, ok := dm[workerID]; !ok {
		// worker is already dead
		return ErrWorkerDoesNotExist
	}
	dm[workerID] = isDead
	return nil
}

func (dm deadMap) AddWorker(workerID WorkerID) error {
	if _, ok := dm[workerID]; !ok {
		// add non-existed worker to deadMap, and mark it alive
		dm[workerID] = false
	}
	return nil
}

func (dm deadMap) RemoveWorker(workerID WorkerID) error {
	delete(dm, workerID)
	return nil
}

type Phase int32

const (
	PHASE_INIT Phase = iota
	PHASE_MAP
	PHASE_REDUCE
	PHASE_DONE
)

// WorkerMap stores every workers' jobs

type WorkerMap struct {
	mu sync.RWMutex
	m  map[WorkerID]map[JobID]Job
}

func NewWorkerMap() *WorkerMap {
	m := &WorkerMap{}
	m.m = map[WorkerID]map[JobID]Job{}
	return m
}

func (w *WorkerMap) String() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return debug(w.m)
}

func (w *WorkerMap) NoLockNumOfWorker() int {
	return len(w.m)
}

func (w *WorkerMap) GetJobsByWorkerID(workerID WorkerID) ([]Job, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	jobsMap, ok := w.m[workerID]
	if !ok {
		return nil, fmt.Errorf("worker [%v] does not exist")
	}
	jobs := make([]Job, len(jobsMap))
	for _, j := range jobsMap {
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (w *WorkerMap) NumOfWorker() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.m)
}

func (w *WorkerMap) RemoveWorker(workerID WorkerID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.m, workerID)
}

func debug(i interface{}) string {
	b, err := json.MarshalIndent(i, "", "\t")
	if err != nil {
		panic(err)
	}
	return string(b)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// AddJob add job to specific worker.
func (m *WorkerMap) AddJob(j Job, workerID WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	jobsForWorker, ok := m.m[workerID]
	if !ok {
		// it's new worker
		jobsForWorker = map[JobID]Job{}
	}
	_, ok = jobsForWorker[j.ID]
	if ok {
		return fmt.Errorf("job %v already exists", j.ID)
	}
	// Job Not Found
	jobsForWorker[j.ID] = j
	m.m[workerID] = jobsForWorker
	return nil
}

func (m *WorkerMap) FinishJob(workerID WorkerID, jobID JobID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	jobs, ok := m.m[workerID]
	if !ok {
		return fmt.Errorf("worker not found")
	}
	if _, ok := jobs[jobID]; !ok {
		return fmt.Errorf("job not found")
	}
	delete(jobs, jobID)
	m.m[workerID] = jobs
	return nil
}

func (c *Coordinator) logCoordinator(format string, args ...interface{}) {
	log.Info().Msgf("Coordinator[]\t"+format, args...)
}

type JobQueue struct {
	coor *Coordinator
	ch   chan Job
}

func NewLocalJobQueue(capacity int, coor *Coordinator) *JobQueue {
	return &JobQueue{ch: make(chan Job, capacity), coor: coor}
}

func (j *JobQueue) Stop() {
	close(j.ch)
}

func (jq *JobQueue) BatchSubmit(jobs []Job) error {
	for _, j := range jobs {
		jq.ch <- j
	}
	return nil
}

func (j *JobQueue) Submit(job Job) error {
	j.ch <- job
	return nil
}

func (j *JobQueue) NumOfJobs() int {
	return len(j.ch)
}

// return Job, and is chan opened or not
func (j *JobQueue) GetJob() (Job, error) {
	select {
	case job, ok := <-j.ch:
		if !ok {
			return Job{}, ErrDone
		}
		return job, nil
	default:
		return Job{}, ErrNoJob
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// FIXME: Tidy up error return flow
// Where should we put error ?
func (c *Coordinator) MarkHealthy(args *MarkHealthyArgs, reply *MarkHealthyReply) error {
	c.logCoordinator("MarkHealthy is called for workerID: %v, ReqID: %v", args.WorkerID, args.ReqID)
	if c.PhaseIs(PHASE_DONE) {
		reply.Err = ErrDone
		return ErrDone
	}
	ev := HealthEvent{
		ReqID:    args.ReqID,
		Type:     EVENT_MARK_HEALTHY,
		WorkerID: args.WorkerID,
		Req:      *args,
		RespCh:   make(chan any),
	}
	c.healthCh <- ev
	resp := <-ev.RespCh
	*reply = resp.(MarkHealthyReply)
	c.logCoordinator("healthy is marked: %v", c.deadMap.String())
	return nil
}

func (c *Coordinator) FinishJob(args *FinishJobsArgs, reply *FinishJobsReply) error {
	c.logCoordinator("FinishJob is called for workerID: %v, ReqID: %v", args.WorkerID, args.ReqID)
	ev := JobEvent{
		ReqID:    args.ReqID,
		Type:     EVENT_FINISHJOB,
		workerID: args.WorkerID,
		Req:      *args,
		RespCh:   make(chan any),
	}
	c.jobEventCh <- ev
	resp := <-ev.RespCh
	*reply = resp.(FinishJobsReply)
	return nil
}

func (c *Coordinator) NewWordCount(args *WordCountArgs, reply *WordCountReply) error {
	mapJobs := make([]Job, 0, len(args.FileNames))
	reduceJobs := make([]Job, 0, c.nReduce)
	for _, fileName := range args.FileNames {
		job := NewJob(fileName, TYPE_MAP)
		mapJobs = append(mapJobs, job)
	}
	for reduceNum := 0; reduceNum < c.nReduce; reduceNum++ {
		j := Job{
			ID:        NewJobID(),
			JobType:   TYPE_REDUCE,
			ReduceNum: reduceNum,
		}
		reduceJobs = append(reduceJobs, j)
	}
	must(c.doMapReduce(mapJobs, reduceJobs))
	return nil
}

func (c *Coordinator) doMapReduce(mapJobs []Job, reduceJobs []Job) error {
	c.ChangePhase(PHASE_MAP)
	c.mapJobWaitGroup.Add(len(mapJobs))
	for _, j := range mapJobs {
		if err := c.JobQueue.Submit(j); err != nil {
			return err
		}
	}

	if err := c.schedule(mapJobs, PHASE_MAP); err != nil {
		return fmt.Errorf("failed on c.schedule for PHASE_MAP: %v", err)
	}

	c.ChangePhase(PHASE_REDUCE)

	c.reduceJobWaitGroup.Add(c.nReduce)

	for _, j := range reduceJobs {
		if err := c.JobQueue.Submit(j); err != nil {
			return err
		}
	}

	if err := c.schedule(reduceJobs, PHASE_REDUCE); err != nil {
		return fmt.Errorf("failed on c.schedule for PHASE_REDUCE: %v", err)
	}

	c.ChangePhase(PHASE_DONE)

	c.JobQueue.Stop()
	c.cleanupWaitGroup.Add(c.workerMap.NumOfWorker())

	if err := c.schedule(nil, PHASE_DONE); err != nil {
		return fmt.Errorf("failed on c.schedule for PHASE_DONE: %v", err)
	}

	c.logCoordinator("all jobs are done")

	return nil
}

func (c *Coordinator) schedule(jobs []Job, phase Phase) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		switch phase {
		case PHASE_MAP:
			c.WaitForMap()
		case PHASE_REDUCE:
			c.WaitForReduce()
		case PHASE_DONE:
			c.WaitForDone()
		}
		cancel()
	}()

	c.logCoordinator("in schedule for %v", phase)
	for {
		select {
		case ev, ok := <-c.jobEventCh:
			if !ok {
				c.logCoordinator("jobEventCh is closed")
				return nil
			}
			c.logCoordinator("got event: %#v", ev)
			if err := c.handleJobEvent(ev); err != nil {
				return fmt.Errorf("failed on c.handleJobEvent: %v", err)
			}
		case ev := <-c.healthCh:
			c.handleHealthCheck(ev)
		case <-ctx.Done():
			return nil
		}
	}
}

type HealthEvent struct {
	ReqID    ReqID
	Type     EventType
	WorkerID WorkerID
	Req      any
	RespCh   chan any
}

func (c *Coordinator) handleHealthCheck(ev HealthEvent) error {
	switch ev.Type {
	case EVENT_MARK_HEALTHY:
		args, ok := ev.Req.(MarkHealthyArgs)
		c.logCoordinator("handleHealthCheck is called for %v", args.ReqID)
		reply := MarkHealthyReply{}
		if !ok {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("wrong type on req, got %T, want %T", args, MarkHealthyArgs{})
		}
		if err := c.deadMap.MarkHealthy(args.WorkerID); err != nil {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("failed on c.deadMap.MarkHealthy: %v", err)
		}
		ev.RespCh <- reply
		return nil
	case EVENT_CHECK_HEALTHY:
		//args := CheckHealthyArgs{}
		reply := CheckHealthyReply{}
		if err := c.syncWorkerAliveness(); err != nil {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("failed on c.checkWorkerAliveness: %v", err)
		}
	default:
		panic("unexpected event type")
	}
	c.logCoordinator("handleHealthCheck is called")
	return nil
}

type JobEvent struct {
	ReqID    ReqID
	Type     EventType
	workerID WorkerID
	Req      any
	RespCh   chan any
	// TODO: Resp Channel to send resp to req handling goroutine
}

type EventType int

const (
	EVENT_GETJOB EventType = iota
	EVENT_FINISHJOB
	EVENT_MARK_HEALTHY
	EVENT_CHECK_HEALTHY
)

func (c *Coordinator) handleJobEvent(ev JobEvent) error {
	c.logCoordinator("inside handleJobEvent")
	switch ev.Type {
	case EVENT_GETJOB:
		var (
			j     Job
			err   error
			reply GetJobsReply
		)

		if c.PhaseIs(PHASE_DONE) {
			req, ok := ev.Req.(GetJobsArgs)
			if !ok {
				ev.RespCh <- ErrInternal
				return fmt.Errorf("wrong type on req, got %T, want %T", req, GetJobsArgs{})
			}
			reply.Err = ErrDone
			c.deadMap.RemoveWorker(req.WorkerID)
			c.cleanupWaitGroup.Done()
			ev.RespCh <- reply
			return nil
		}

		req, ok := ev.Req.(GetJobsArgs)
		if !ok {
			ev.RespCh <- ErrInternal
			return fmt.Errorf("wrong type on req, got %T, want %T", req, GetJobsArgs{})
		}

		c.logCoordinator("after type assert")

		// add worker if it doesn't exist
		if err := c.deadMap.AddWorker(req.WorkerID); err != nil {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("failed on c.deadMap.AddWorker: %v", err)
		}

		j, err = c.JobQueue.GetJob()
		if err != nil {
			// c.logCoordinator("after c.JobQueue.GetJob, worker %v, req[%v], err %v", args.WorkerID, args.ReqID, err)
			switch {
			//			case err == ErrDone:
			//				goto DONE
			case err == ErrNoJob:
				// c.logCoordinator("workerMap: %v", c.workerMap.String())
				reply = GetJobsReply{}
				reply.Err = ErrNoJob
				ev.RespCh <- reply
				return nil
			default:
				reply.Err = ErrInternal
				ev.RespCh <- reply
				return fmt.Errorf("failed on c.JobQueue.GetJob: %v", err)
			}
		}

		c.logCoordinator("after GetJob")

		// at here, we got our job, so Phase is not possible to be PHASE_DONE
		if err := c.workerMap.AddJob(j, req.WorkerID); err != nil {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("failed on c.workerMap.AddJob: %v", err)
		}

		reply.Jobs = []Job{j}
		ev.RespCh <- reply

		c.logCoordinator("job is added worker %v, req[%v]", req.WorkerID, req.ReqID)
		return nil
	case EVENT_FINISHJOB:
		reply := FinishJobsReply{}
		req, ok := ev.Req.(FinishJobsArgs)
		if !ok {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("wrong type on req, got %T, want %T", req, FinishJobsArgs{})
		}

		if err := c.workerMap.FinishJob(req.WorkerID, req.JobID); err != nil {
			reply.Err = ErrInternal
			ev.RespCh <- reply
			return fmt.Errorf("failed on Coordinator.FinishJob for workerID [%v], jobID: [%v]: %v", req.WorkerID, req.JobID, err)
		}
		// job type
		switch c.phase {
		case PHASE_MAP:
			c.mapJobWaitGroup.Done()
		case PHASE_REDUCE:
			c.reduceJobWaitGroup.Done()
		}
		ev.RespCh <- reply
		return nil
	default:
		panic("unknown event")
	}

	return nil
}

func (c *Coordinator) WordCount(args *WordCountArgs, reply *WordCountReply) error {
	c.ChangePhase(PHASE_MAP)

	mapJobs := make([]Job, 0, len(args.FileNames))

	c.mapJobWaitGroup.Add(len(args.FileNames))

	for _, fileName := range args.FileNames {
		// assign job for every file
		job := NewJob(fileName, TYPE_MAP)
		if err := c.JobQueue.Submit(job); err != nil {
			return err
		}
		c.logCoordinator("length of jobs: %v", c.JobQueue.NumOfJobs())
		mapJobs = append(mapJobs, job)
	}

	c.WaitForMap()

	c.logCoordinator("all map jobs are done")

	c.logCoordinator("try to change phase")
	c.ChangePhase(PHASE_REDUCE)

	c.reduceJobWaitGroup.Add(c.nReduce)

	for reduceNum := 0; reduceNum < c.nReduce; reduceNum++ {
		j := Job{
			ID:        NewJobID(),
			JobType:   TYPE_REDUCE,
			ReduceNum: reduceNum,
		}
		// reuse same job
		if err := c.JobQueue.Submit(j); err != nil {
			return err
		}
	}

	c.WaitForReduce()

	c.ChangePhase(PHASE_DONE)

	c.JobQueue.Stop()

	c.logCoordinator("all jobs are done")

	time.Sleep(3 * time.Second)

	// Wait for all worker to die
	for !c.Done() {
		c.logCoordinator("in wordcount: not done")
		c.logCoordinator("workers num: %v", c.workerMap.NumOfWorker())
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func (c *Coordinator) ChangePhase(desiredPhase Phase) {
	c.phaseMu.Lock()
	defer c.phaseMu.Unlock()
	if c.phase != desiredPhase-1 {
		panic("broken state")
	}
	c.phase = desiredPhase
}

func (c *Coordinator) WaitForReduce() {
	c.reduceJobWaitGroup.Wait()
}

func (c *Coordinator) WaitForMap() {
	c.mapJobWaitGroup.Wait()
}

func (c *Coordinator) WaitForDone() {
	c.cleanupWaitGroup.Wait()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) Serve() {
	c.MailBox.Serve()
	// FIXME: use waitgroup for graceful shutdown and avoid goroutine leak
	go c.monitor()
}

func (c *Coordinator) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		if c.PhaseIs(PHASE_DONE) {
			return
		}
		select {
		case <-ticker.C:
			// create checkWorkerAliveness event
			if err := c.syncWorkerAliveness(); err != nil {
				c.logCoordinator("in monitor, failed on c.checkWorkerAliveness: %v", err)
			}
		}
	}
}

// syncWorkerAliveness checks liveness of worker and set it back to dead again,
// if worker in dead map is dead, re-queue work to c.JobQueue
// set dead == true for worker in dead map
func (c *Coordinator) syncWorkerAliveness() error {
	errSlice := []error{}
	c.logCoordinator("checkWorkerAliveness")
	for workerID, isDead := range c.deadMap {
		if isDead {
			c.logCoordinator("worker[%v] is dead", workerID)
			// get all job of that worker in c.workerMap, ressign them to jobQueue
			jobs, err := c.workerMap.GetJobsByWorkerID(workerID)
			if err != nil {
				errSlice = append(errSlice, fmt.Errorf("failed on c.workerMap.GetJobsByWorkerID for worker[%v]: %v", workerID, err))
			}
			c.workerMap.RemoveWorker(workerID)
			c.deadMap.RemoveWorker(workerID)
			if err := c.JobQueue.BatchSubmit(jobs); err != nil {
				errSlice = append(errSlice, fmt.Errorf("failed on c.JobQueue.BatchSubmit for worker[%v]: %v", workerID, err))
			}
		} else {
			// alive, set isDead to true again, wait for worker to call
			c.deadMap[workerID] = true
			// set it back to false
		}
	}
	c.logCoordinator("liveness: %v", c.deadMap)
	if len(errSlice) != 0 {
		var err error
		for _, e := range errSlice {
			err = fmt.Errorf(fmt.Sprintf("%v ", e.Error()))
		}
		return err
	}
	return nil
}

func (c *Coordinator) PhaseIs(p Phase) bool {
	c.phaseMu.RLock()
	defer c.phaseMu.RUnlock()
	return c.phase == p
}

func (c *Coordinator) NoLockPhaseIs(p Phase) bool {
	return c.phase == p
}

// main/mrcoordinator.go calls Done() periodically to see if all jobs are done.
func (c *Coordinator) Done() bool {
	return c.PhaseIs(PHASE_DONE) && c.workerMap.NumOfWorker() == 0
}

var (
	ErrDone               = errors.New("all jobs are done")
	ErrNoJob              = errors.New("no job right now")
	ErrInternal           = errors.New("internal server error")
	ErrWorkerDoesNotExist = errors.New("worker doesn't exist")
	ErrConn               = errors.New("connection error")
)

func (c *Coordinator) GetJobs(args *GetJobsArgs, reply *GetJobsReply) error {
	c.logCoordinator("Coordinator.GetJobs is called for workerID: %v, ReqID: %v", args.WorkerID, args.ReqID)
	ev := JobEvent{
		ReqID:    args.ReqID,
		Type:     EVENT_GETJOB,
		workerID: args.WorkerID,
		Req:      *args,
		RespCh:   make(chan any),
	}
	c.jobEventCh <- ev
	resp := <-ev.RespCh
	*reply = resp.(GetJobsReply)
	c.logCoordinator("Coordinator.GetJobs is done for workerID: %v, ReqID: %v, Resp: %v", args.WorkerID, args.ReqID, debug(reply))
	c.logCoordinator("err in reply.Err", reply.Err)
	return reply.Err
}

const DefaultJobQueueCap = 10

// FIXME: Coordinator should know nReduce in advance ?
func NewLocalCoordinator(files []string, nReduce int) *Coordinator {
	return newCoordinator(files, nReduce, TYPE_LOCAL)
}

type coorType int

const (
	TYPE_LOCAL coorType = iota
	TYPE_RPC
)

func newCoordinator(files []string, nReduce int, typ coorType) *Coordinator {
	c := &Coordinator{nReduce: nReduce}

	// set up mailbox
	switch typ {
	case TYPE_LOCAL:
		m := &localMailBox{coorService: c}
		c.MailBox = m
	case TYPE_RPC:
		m := &RPCMailBox{coorService: c}
		c.MailBox = m
	default:
		panic("invalid type")
	}

	c.workerMap = NewWorkerMap()
	c.JobQueue = NewLocalJobQueue(DefaultJobQueueCap, c)
	c.nReduce = nReduce
	c.deadMap = map[WorkerID]bool{}
	c.jobEventCh = make(chan JobEvent, 10)
	c.healthCh = make(chan HealthEvent, 10)
	// FIXME: should wait for this goroutine
	go c.Serve()
	return c
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewRPCCoordinator(files []string, nReduce int) *Coordinator {
	return newCoordinator(files, nReduce, TYPE_RPC)
}

type CoorMailBox interface {
	Serve()
	GetJobs(id WorkerID) ([]Job, error)
	FinishJob(workerID WorkerID, jobID JobID) error
	MarkHealthy(id WorkerID) error
	Example(args *ExampleArgs, reply *ExampleReply) error
}

type localMailBox struct {
	coorService *Coordinator
}

func (l *localMailBox) Serve() {
	return
}

func (l *localMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	args := &GetJobsArgs{ReqID: NewReqID(), WorkerID: workerID}
	reply := &GetJobsReply{}
	l.coorService.logCoordinator("localMailBox try to call GetJobs")
	if err := l.coorService.GetJobs(args, reply); err != nil {
		switch {
		case err == ErrDone:
			return nil, reply.Err
		case err == ErrNoJob:
			return nil, reply.Err
		default:
			return nil, fmt.Errorf("failed on l.coorService.GetJobs: %v", err)
		}
	}
	return reply.Jobs, nil
}

func (l *localMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	args := FinishJobsArgs{WorkerID: workerID, JobID: jobID, ReqID: NewReqID()}
	reply := FinishJobsReply{}
	if err := l.coorService.FinishJob(&args, &reply); err != nil {
		return fmt.Errorf("failed on l.coorService.FinishJob with req[%v]: %v", args.ReqID, err)
	}
	return nil
}

func (l *localMailBox) MarkHealthy(workerID WorkerID) error {
	args := MarkHealthyArgs{
		ReqID:    NewReqID(),
		WorkerID: workerID,
	}
	reply := MarkHealthyReply{}
	if err := l.coorService.MarkHealthy(&args, &reply); err != nil {
		switch {
		case err == ErrDone:
			return ErrDone
		default:
			return fmt.Errorf("failed on l.coorService.MarkHealthy: %v", err)
		}
	}
	return nil
}

func (l *localMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type RPCMailBox struct {
	coorService *Coordinator
}

func (r *RPCMailBox) Serve() {
	rpc.Register(r.coorService)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal().Msgf("listen error: %v", e)
	}
	go http.Serve(l, nil)
}

func (r *RPCMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	args := GetJobsArgs{WorkerID: workerID, ReqID: NewReqID()}
	reply := GetJobsReply{}
	rpcName := "Coordinator.GetJobs"
	if err := callWithRetry(rpcName, &args, &reply); err != nil {
		switch {
		case err == ErrDone:
			return nil, ErrDone
		case err == ErrNoJob:
			return nil, ErrNoJob
		default:
			return nil, fmt.Errorf("failed on rpc call [%v]: %v", rpcName, err)
		}
	}
	fmt.Printf("GetJobs for worker is done, Req:[%v], workerID:[%v]\n", args.ReqID, args.WorkerID)
	return reply.Jobs, nil
}

func (r *RPCMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	args := FinishJobsArgs{WorkerID: workerID, JobID: jobID, ReqID: NewReqID()}
	reply := FinishJobsReply{}
	rpcName := "Coordinator.FinishJob"
	if err := callWithRetry(rpcName, &args, &reply); err != nil {
		switch {
		case err == ErrDone:
			return ErrDone
		default:
			return fmt.Errorf("failed on rpc call [%v]: %v", rpcName, err)
		}
	}
	return nil
}

func (r *RPCMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (r *RPCMailBox) MarkHealthy(workerID WorkerID) error {
	rpcName := "Coordinator.MarkHealthy"
	args := MarkHealthyArgs{WorkerID: workerID, ReqID: NewReqID()}
	reply := MarkHealthyReply{}
	fmt.Printf("RPCMailBox: calling MarkHealthy for worker[%v], req:[%v]\n", workerID, args.ReqID)
	if err := callWithRetry(rpcName, &args, &reply); err != nil {
		switch {
		case err == ErrDone:
			return ErrDone
		default:
			return fmt.Errorf("failed on rpc call [%v]: %v", rpcName, err)
		}
	}
	return nil
}
