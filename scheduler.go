package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"
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
	WorkerLimit int32
}

// NewStdScheduler will create a new std scheduler instance that allows users to create and manage tasks.
func NewStdScheduler(opts StdSchedulerOptions) *StdScheduler {
	var taskSem chan struct{}

	if opts.WorkerLimit > 0 {
		taskSem = make(chan struct{}, opts.WorkerLimit)
	}

	return &StdScheduler{
		taskSem: taskSem,
		tasks:   make(map[string]*Task),
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
		return fmt.Errorf("task function cannot be nil")
	}

	// Ensure Interval is never 0, this would cause Timer to panic
	if t.Interval <= time.Duration(0) {
		return fmt.Errorf("task interval must be defined")
	}

	// Create Context used to cancel downstream Goroutines
	t.ctx, t.cancel = context.WithCancel(context.Background())

	// Add id to TaskContext
	t.TaskContext.id = id

	// Check id is not in use, then add to task list and start background task
	s.Lock()
	defer s.Unlock()
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

var (
	// ErrIDInUse is returned when a Task ID is specified but already used.
	ErrIDInUse = fmt.Errorf("ID already used")
)

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
		if err != nil && (t.ErrFunc != nil || t.ErrFuncWithTaskContext != nil) {
			if t.ErrFuncWithTaskContext != nil {
				go t.ErrFuncWithTaskContext(t.TaskContext, err)
			} else {
				go t.ErrFunc(err)
			}
		}
		if t.RunOnce {
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
