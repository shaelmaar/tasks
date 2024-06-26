package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"

	"github.com/shaelmaar/tasks/logger"
)

var (
	// ErrIDInUse is returned when a Task ID is specified but already used.
	ErrIDInUse = fmt.Errorf("ID already used")
	// ErrRetryOnErrorIntervalEmpty is returned when retry on error interval is not set
	// for run once task with retries on error.
	ErrRetryOnErrorIntervalEmpty = errors.New("retry on error interval is empty")
	// ErrIntervalEmpty is returned when interval is not set for cron task (not run once task).
	ErrIntervalEmpty = errors.New("interval is empty")
	// ErrTaskExecFunctionsNotSet is returned when task execute functions are not set.
	ErrTaskExecFunctionsNotSet = errors.New("task functions are empty")
	// ErrTaskErrFunctionsNotSet is returned when task err functions are not set.
	ErrTaskErrFunctionsNotSet = errors.New("err functions are empty")
	// ErrTaskLimitExceeded is returned when number of tasks exceeds task limit.
	ErrTaskLimitExceeded = errors.New("task limit exceeded")
)

// StdScheduler stores the internal task list and provides an interface for task management.
type StdScheduler struct {
	sync.RWMutex

	taskSem chan struct{}
	// tasks is the internal task list used to store tasks that are currently scheduled.
	tasks map[string]*Task

	opts StdSchedulerOptions
}

type StdSchedulerOptions struct {
	WorkerLimit int
	TaskLimit   int
	Logger      logger.Logger
}

// NewStdScheduler will create a new std scheduler instance that allows users to create and manage tasks.
func NewStdScheduler(opts StdSchedulerOptions) *StdScheduler {
	var taskSem chan struct{}

	if opts.WorkerLimit > 0 {
		taskSem = make(chan struct{}, opts.WorkerLimit)
	}

	if opts.Logger != nil {
		logger.SetDefault(opts.Logger)
	}

	return &StdScheduler{
		taskSem: taskSem,
		tasks:   make(map[string]*Task),
		opts:    opts,
	}
}

// Add will add a task to the task list and schedule it. Once added, tasks will wait the defined time interval and then
// execute. This means a task with a 15 seconds interval will be triggered 15 seconds after Add is complete. Not before
// or after (excluding typical machine time jitter).
//
//	// Add a task
//	id, err := scheduler.Add(&tasks.Task{
//		Interval: time.Duration(30 * time.Second),
//		TaskFunc: func() error {
//			// Put your logic here
//		}(),
//		ErrFunc: func(err error) {
//			// Put custom error handling here
//		}(),
//	})
//	if err != nil {
//		// Do stuff
//	}
func (s *StdScheduler) Add(t *Task) (string, error) {
	id := xid.New()
	err := s.AddWithID(id.String(), t)
	if errors.Is(err, ErrIDInUse) {
		logger.Infof("id '%s' is already in use, another attempt to add", id.String())

		return s.Add(t)
	}
	return id.String(), err
}

// AddWithID will add a task with an ID to the task list and schedule it. It will return an error if the ID is in-use.
// Once added, tasks will wait the defined time interval and then execute. This means a task with a 15 seconds interval
// will be triggered 15 seconds after Add is complete. Not before or after (excluding typical machine time jitter).
//
//	// Add a task
//	id := xid.NewStdScheduler()
//	err := scheduler.AddWithID(id, &tasks.Task{
//		Interval: time.Duration(30 * time.Second),
//		TaskFunc: func() error {
//			// Put your logic here
//		}(),
//		ErrFunc: func(err error) {
//			// Put custom error handling here
//		}(),
//	})
//	if err != nil {
//		// Do stuff
//	}
func (s *StdScheduler) AddWithID(id string, t *Task) error {
	// Check if TaskFunc is nil before doing anything
	if t.TaskFunc == nil && t.FuncWithTaskContext == nil {
		return ErrTaskExecFunctionsNotSet
	}

	if t.ErrFunc == nil && t.ErrFuncWithTaskContext == nil {
		return ErrTaskErrFunctionsNotSet
	}

	if !t.RunOnce && t.Interval <= time.Duration(0) {
		return ErrIntervalEmpty
	}

	if t.RunOnce && t.RetriesOnError > 0 && t.RetryOnErrorInterval <= time.Duration(0) {
		return ErrRetryOnErrorIntervalEmpty
	}

	// Create Context used to cancel downstream Goroutines
	t.ctx, t.cancel = context.WithCancel(context.Background())

	// Add id to TaskContext
	t.TaskContext.id = id
	if t.TaskContext.Context == nil {
		t.TaskContext.Context, t.TaskContext.Cancel = context.WithCancel(context.Background())
	}

	// Check id is not in use, then add to task list and start background task
	s.Lock()
	defer s.Unlock()
	if s.opts.TaskLimit > 0 && len(s.tasks) >= s.opts.TaskLimit {
		return ErrTaskLimitExceeded
	}

	if _, ok := s.tasks[id]; ok {
		return ErrIDInUse
	}
	t.id = id

	// To make up for bad design decisions we need to copy the task for execution
	task := t.Clone()

	// Add task to schedule
	s.tasks[t.id] = task
	s.scheduleTask(task)

	return nil
}

// Del will unschedule the specified task and remove it from the task list. Deletion will prevent future invocations of
// a task, but not interrupt a triggered task.
func (s *StdScheduler) Del(name string) {
	// Grab task from task list
	t, err := s.Lookup(name)
	if err != nil {
		return
	}

	// Stop the task
	defer t.cancel()
	if t.TaskContext.Cancel != nil {
		defer t.TaskContext.Cancel()
	}

	t.Lock()
	defer t.Unlock()

	if t.timer != nil {
		defer t.timer.Stop()
	}

	// Remove from task list
	s.Lock()
	defer s.Unlock()
	delete(s.tasks, name)
}

// Lookup will find the specified task from the internal task list using the task ID provided.
//
// The returned task should be treated as read-only, and not modified outside of this package. Doing so, may cause
// panics.
func (s *StdScheduler) Lookup(name string) (*Task, error) {
	s.RLock()
	defer s.RUnlock()
	t, ok := s.tasks[name]
	if ok {
		return t.Clone(), nil
	}
	return t, fmt.Errorf("could not find task within the task list")
}

// Has will return true if specified task is present.
func (s *StdScheduler) Has(name string) bool {
	s.RLock()
	defer s.RUnlock()

	_, ok := s.tasks[name]

	return ok
}

// Tasks is used to return a copy of the internal tasks map.
//
// The returned task should be treated as read-only, and not modified outside of this package. Doing so, may cause
// panics.
func (s *StdScheduler) Tasks() map[string]*Task {
	s.RLock()
	defer s.RUnlock()
	m := make(map[string]*Task)
	for k, v := range s.tasks {
		m[k] = v.Clone()
	}
	return m
}

// Stop is used to unschedule and delete all tasks owned by the scheduler instance.
func (s *StdScheduler) Stop() {
	tt := s.Tasks()
	for n := range tt {
		s.Del(n)
	}

	if s.taskSem != nil {
		close(s.taskSem)
	}
}

// scheduleTask creates the underlying scheduled task. If StartAfter is set, this routine will wait until the
// time specified.
func (s *StdScheduler) scheduleTask(t *Task) {
	_ = time.AfterFunc(time.Until(t.StartAfter), func() {
		var err error

		// Verify if task has been cancelled before scheduling
		t.safeOps(func() {
			err = t.ctx.Err()
		})
		if err != nil {
			// Task has been cancelled, do not schedule
			return
		}

		// Schedule task
		t.safeOps(func() {
			t.timer = time.AfterFunc(t.Interval, func() { s.execTask(t) })
		})
	})

	logger.Debugf("task (id: %s) has been scheduled at %s", t.id, t.StartAfter.Format(time.RFC3339))
}

// execTask is the underlying scheduler, it is used to trigger and execute tasks.
func (s *StdScheduler) execTask(t *Task) {
	s.lockSem()

	go func() {
		defer func() { s.unlockSem() }()

		var err error
		if t.FuncWithTaskContext != nil {
			err = t.FuncWithTaskContext(t.TaskContext)
		} else {
			err = t.TaskFunc()
		}

		deleteTask := true

		if err != nil {
			deleteTask = onTaskError(t, err)
		} else {
			logger.Debugf("task (id: %s) has been successfully executed", t.id)
		}
		if t.RunOnce && deleteTask {
			defer s.Del(t.id)
		}
	}()
	if !t.RunOnce {
		t.safeOps(func() {
			t.timer.Reset(t.Interval)
		})
	}
}

func (s *StdScheduler) lockSem() {
	if s.taskSem != nil {
		s.taskSem <- struct{}{}
	}
}

func (s *StdScheduler) unlockSem() {
	if s.taskSem != nil {
		<-s.taskSem
	}
}

func onTaskError(t *Task, err error) (deleteTask bool) {
	if rescheduleExists := rescheduleTaskOnError(t, err); rescheduleExists {
		return deleteTask
	}

	logger.Errorf("task (id: %s, retries left: %d) failed: %s", t.id, t.RetriesOnError, err.Error())

	if t.ErrFuncWithTaskContext != nil {
		go t.ErrFuncWithTaskContext(t.TaskContext, err)
	} else {
		go t.ErrFunc(err)
	}

	if t.RunOnce && t.RetriesOnError > 0 {
		deleteTask = false

		t.safeOps(func() {
			t.RetriesOnError--
			t.timer.Reset(t.RetryOnErrorInterval)
		})
	} else {
		deleteTask = true
	}

	return deleteTask
}

func rescheduleTaskOnError(t *Task, err error) (exists bool) {
	if len(t.rescheduleOnError) == 0 {
		return exists
	}

	for e, opts := range t.rescheduleOnError {
		if !errors.Is(err, e) {
			continue
		}

		exists = true

		if opts.count <= 0 {
			break
		}

		opts.count--
		t.safeOps(func() {
			t.timer.Reset(opts.interval)
			t.rescheduleOnError[e] = opts
		})

		logger.Infof("task (id: %s) has been rescheduled on error: %s, reschedules left: %d",
			t.id, err.Error(), opts.count)

		exists = true
	}

	return exists
}
