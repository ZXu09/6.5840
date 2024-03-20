package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusFinish
	TaskStatusErr
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files     []string
	nReduce   int
	taskPhase TaskPhase
	taskStats []TaskStat
	mu        sync.Mutex
	done      bool
	workerSeq int
	taskCh    chan Task
}

func (c *Coordinator) newTask(taskSeq int) Task {
	task := Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMaps:    len(c.files),
		Seq:      taskSeq,
		Phase:    c.taskPhase,
		Alive:    true,
	}
	DPrintf("c:%+v, taskseq:%d, lenfiles:%d, lents:%d", c, taskSeq, len(c.files), len(c.taskStats))
	if task.Phase == MapPhase {
		task.FileName = c.files[taskSeq]
	}
	return task
}

func (c *Coordinator) schedule() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}
	allFinish := true
	for index, t := range c.taskStats {
		switch t.Status {
		case TaskStatusReady:
			allFinish = false
			c.taskCh <- c.newTask(index)
			c.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				c.taskStats[index].Status = TaskStatusQueue
				c.taskCh <- c.newTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			c.taskStats[index].Status = TaskStatusQueue
			c.taskCh <- c.newTask(index)
		default:
			panic("t.status err")
		}
	}
	if allFinish {
		if c.taskPhase == MapPhase {
			c.initReduceTask()
		} else {
			c.done = true
		}
	}
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStat, len(c.files))
}

func (c *Coordinator) initReduceTask() {
	DPrintf("init ReduceTask")
	c.taskPhase = ReducePhase
	c.taskStats = make([]TaskStat, c.nReduce)
}

func (c *Coordinator) updateTaskState(args *GetTaskArgs, task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Phase != c.taskPhase {
		panic("req Task phase neq")
	}

	c.taskStats[task.Seq].Status = TaskStatusRunning
	c.taskStats[task.Seq].WorkerId = args.WorkerId
	c.taskStats[task.Seq].StartTime = time.Now()
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := <-c.taskCh
	reply.Task = &task

	if task.Alive {
		c.updateTaskState(args, &task)
	}
	DPrintf("in get Task, args:%+v, reply:%+v", args, reply)
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, c.taskPhase)

	if c.taskPhase != args.Phase || args.WorkerId != c.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		c.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		c.taskStats[args.Seq].Status = TaskStatusErr
	}

	go c.schedule()
	return nil
}

func (c *Coordinator) GetWorkerId(args *GetWorkerIdArgs, reply *GetWorkerIdReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerSeq += 1
	reply.WorkerId = c.workerSeq
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) tickSchedule() {
	// This is supposed to be one timer per task, but we'll keep it simple here,
	// start a goroutine to schedule tasks for every 0.5s.
	for !c.Done() {
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// create a Master.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mu = sync.Mutex{}
	c.nReduce = nReduce
	c.files = files
	if nReduce > len(files) {
		c.taskCh = make(chan Task, nReduce)
	} else {
		c.taskCh = make(chan Task, len(c.files))
	}

	c.initMapTask()
	go c.tickSchedule()
	c.server()
	DPrintf("master init")
	// Your code here.
	return &c
}
