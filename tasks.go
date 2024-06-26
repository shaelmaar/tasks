/*
Package tasks is an easy to use in-process scheduler for recurring tasks in Go. Tasks is focused on high frequency
tasks that run quick, and often. The goal of Tasks is to support concurrent running tasks at scale without scheduler
induced jitter.

Tasks is focused on accuracy of task execution. To do this each task is called within it's own goroutine.
This ensures that long execution of a single invocation does not throw the schedule as a whole off track.

As usage of this scheduler scales, it is expected to have a larger number of sleeping goroutines. As it is
designed to leverage Go's ability to optimize goroutine CPU scheduling.

For simplicity this task scheduler uses the time.Duration type to specify intervals. This allows for a simple
interface and flexible control over when tasks are executed.

Below is an example of starting the scheduler and registering a new task that runs every 30 seconds.

	// Start the StdScheduler
	scheduler := tasks.NewStdScheduler()
	defer scheduler.Stop()

	// Add a task
	id, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(30 * time.Second),
		TaskFunc: func() error {
			// Put your logic here
		},
	})
	if err != nil {
		// Do Stuff
	}

Sometimes schedules need to started at a later time. This package provides the ability to start a task only after
a certain time. The below example shows this in practice.

	// Add a recurring task for every 30 days, starting 30 days from now
	id, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(30 * (24 * time.Hour)),
		StartAfter: time.Now().Add(30 * (24 * time.Hour)),
		TaskFunc: func() error {
			// Put your logic here
		},
	})
	if err != nil {
		// Do Stuff
	}

It is also common for applications to run a task only once. The below example shows scheduling a task to run only once after
waiting for 60 seconds.

	// Add a one time only task for 60 seconds from now
	id, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(60 * time.Second)
		RunOnce:  true,
		TaskFunc: func() error {
			// Put your logic here
		},
	})
	if err != nil {
		// Do Stuff
	}

One powerful feature of Tasks is that it allows users to specify custom error handling. This is done by allowing users to
define a function that is called when a task returns an error. The below example shows scheduling a task that logs when an
error occurs.

	// Add a task with custom error handling
	id, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(30 * time.Second),
		TaskFunc: func() error {
			// Put your logic here
		}(),
		ErrFunc: func(e error) {
			log.Printf("An error occurred when executing task %s - %s", id, e)
		},
	})
	if err != nil {
		// Do Stuff
	}
*/
package tasks

import (
	"context"
	"sync"
	"time"
)

// Task contains the scheduled task details and control mechanisms. This struct is used during the creation of tasks.
// It allows users to control how and when tasks are executed.
type Task struct {
	sync.Mutex

	// id is the Unique ID created for each task. This ID is generated by the Add() function.
	id string

	// TaskContext allows for user-defined context that is passed to task functions.
	TaskContext TaskContext

	// Interval is the frequency that the task executes. Defining this at 30 seconds, will result in a task that
	// runs every 30 seconds.
	//
	// The below are common examples to get started with.
	//
	//  // Every 30 seconds
	//  time.Duration(30 * time.Second)
	//  // Every 5 minutes
	//  time.Duration(5 * time.Minute)
	//  // Every 12 hours
	//  time.Duration(12 * time.Hour)
	//  // Every 30 days
	//  time.Duration(30 * (24 * time.Hour))
	//
	Interval time.Duration

	// RunOnce is used to set this task as a single execution task. By default, tasks will continue executing at
	// the interval specified until deleted. With RunOnce enabled the first execution of the task will result in
	// the task self deleting.
	RunOnce bool

	// RetriesOnError if greater than 0, task will be rescheduled in case of an error on execution.
	RetriesOnError int

	// RetryOnErrorInterval interval for another execution attempt.
	RetryOnErrorInterval time.Duration

	// StartAfter is used to specify a start time for the scheduler. When set, tasks will wait for the specified
	// time to start the schedule timer.
	StartAfter time.Time

	// TaskFunc is the user defined function to execute as part of this task.
	//
	// Either TaskFunc or FuncWithTaskContext must be defined. If both are defined, FuncWithTaskContext will be used.
	TaskFunc func() error

	// ErrFunc allows users to define a function that is called when tasks return an error. If ErrFunc is nil,
	// errors from tasks will be ignored.
	//
	// Either ErrFunc or ErrFuncWithTaskContext must be defined. If both are defined, ErrFuncWithTaskContext will be used.
	ErrFunc func(error)

	// FuncWithTaskContext is a user defined function to execute as part of this task. This function is used in
	// place of TaskFunc with the difference in that it will pass the user defined context from the Task configurations.
	//
	// Either TaskFunc or FuncWithTaskContext must be defined. If both are defined, FuncWithTaskContext will be used.
	FuncWithTaskContext func(TaskContext) error

	// ErrFuncWithTaskContext allows users to define a function that is called when tasks return an error.
	// If ErrFunc is nil, errors from tasks will be ignored. This function is used in place of ErrFunc with
	// the difference in that it will pass the user defined context from the Task configurations.
	//
	// Either ErrFunc or ErrFuncWithTaskContext must be defined. If both are defined, ErrFuncWithTaskContext will be used.
	ErrFuncWithTaskContext func(TaskContext, error)

	// rescheduleOnError allows users to define reschedule on error mechanism.
	// If task execution returns one of specified errors, task will reset its timer to specified duration.
	rescheduleOnError map[error]rescheduleOnErrorOpts

	// timer is the internal task timer. This is stored here to provide control via main scheduler functions.
	timer *time.Timer

	// ctx is the internal context used to control task cancelation.
	ctx context.Context

	// cancel is used to cancel tasks gracefully. This will not interrupt a task function that has already been
	// triggered.
	cancel context.CancelFunc
}

type TaskContext struct {
	// Context is a user-defined context.
	Context context.Context

	// Cancel is used to cancel task execution on FuncWithTaskContext.
	Cancel context.CancelFunc

	// id is the Unique ID created for each task. This ID is generated by the Add() function.
	id string
}

type rescheduleOnErrorOpts struct {
	interval time.Duration
	count    int
}

// safeOps safely change task's data
func (t *Task) safeOps(f func()) {
	t.Lock()
	defer t.Unlock()

	f()
}

// ID will return the task ID. This is the same as the ID generated by the scheduler when adding a task.
// If the task was added with AddWithID, this will be the same as the ID provided.
func (ctx TaskContext) ID() string {
	return ctx.id
}

func (t *Task) WithRescheduleOnError(err error, interval time.Duration, count int) {
	t.safeOps(func() {
		if t.rescheduleOnError == nil {
			t.rescheduleOnError = make(map[error]rescheduleOnErrorOpts)
		}

		t.rescheduleOnError[err] = rescheduleOnErrorOpts{
			interval: interval,
			count:    count,
		}
	})
}

// Clone will create a copy of the existing task. This is useful for creating a new task with the same properties as
// an existing task. It is also used internally when creating a new task.
func (t *Task) Clone() *Task {
	task := &Task{}
	t.safeOps(func() {
		task.TaskFunc = t.TaskFunc
		task.FuncWithTaskContext = t.FuncWithTaskContext
		task.ErrFunc = t.ErrFunc
		task.ErrFuncWithTaskContext = t.ErrFuncWithTaskContext
		task.Interval = t.Interval
		task.StartAfter = t.StartAfter
		task.RunOnce = t.RunOnce
		task.RetriesOnError = t.RetriesOnError
		task.RetryOnErrorInterval = t.RetryOnErrorInterval
		task.id = t.id
		task.ctx = t.ctx
		task.cancel = t.cancel
		task.timer = t.timer
		task.TaskContext = t.TaskContext

		if t.rescheduleOnError == nil {
			return
		}
		rescheduleOnError := make(map[error]rescheduleOnErrorOpts, len(t.rescheduleOnError))
		for k, v := range t.rescheduleOnError {
			rescheduleOnError[k] = v
		}
		task.rescheduleOnError = rescheduleOnError
	})

	return task
}
