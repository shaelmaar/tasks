package tasks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/rs/xid"
	assertions "github.com/stretchr/testify/assert"

	"github.com/shaelmaar/tasks/logger"
)

type InterfaceTestCase struct {
	name   string
	task   *Task
	id     string
	addErr bool
}

type ExecutionTestCase struct {
	name      string
	id        string
	ctx       context.Context
	cancel    context.CancelFunc
	task      *Task
	callsFunc bool
}

func TestTasksInterface(t *testing.T) {
	var tt []InterfaceTestCase

	tt = append(tt, InterfaceTestCase{
		name: "Basic Valid Task",
		task: &Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error { return nil },
			ErrFunc:  func(error) {},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Basic Valid Task with ID",
		task: &Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error { return nil },
			ErrFunc:  func(error) {},
		},
		id: xid.New().String(),
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task with ErrFunc",
		task: &Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error { return nil },
			ErrFunc:  func(e error) {},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task with Context",
		task: &Task{
			Interval:    1 * time.Second,
			TaskFunc:    func() error { return nil },
			ErrFunc:     func(e error) {},
			TaskContext: TaskContext{Context: context.Background()},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task with Context and WithContextFunctions",
		task: &Task{
			Interval:               1 * time.Second,
			FuncWithTaskContext:    func(_ TaskContext) error { return nil },
			ErrFuncWithTaskContext: func(_ TaskContext, e error) {},
			TaskContext:            TaskContext{Context: context.Background()},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task without Context but WithContextFunctions",
		task: &Task{
			Interval:               1 * time.Second,
			FuncWithTaskContext:    func(_ TaskContext) error { return nil },
			ErrFuncWithTaskContext: func(_ TaskContext, e error) {},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task with StartAfter",
		task: &Task{
			Interval:   1 * time.Second,
			TaskFunc:   func() error { return nil },
			StartAfter: time.Now().Add(1 * time.Second),
			ErrFunc:    func(error) {},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task with StartAfter but in the past",
		task: &Task{
			Interval:   1 * time.Second,
			TaskFunc:   func() error { return nil },
			StartAfter: time.Now().Add(-1 * time.Minute),
			ErrFunc:    func(error) {},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "Valid Task with RunOnce",
		task: &Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error { return nil },
			RunOnce:  true,
			ErrFunc:  func(error) {},
		},
	})

	tt = append(tt, InterfaceTestCase{
		name: "No Interval",
		task: &Task{
			TaskFunc: func() error { return nil },
			ErrFunc:  func(error) {},
		},
		addErr: true,
	})

	tt = append(tt, InterfaceTestCase{
		name: "No TaskFunc or FuncWithTaskContext",
		task: &Task{
			Interval: 1 * time.Second,
		},
		addErr: true,
	})

	// Create a base scheduler to use
	scheduler := NewStdScheduler(StdSchedulerOptions{})
	defer scheduler.Stop()

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			id := tc.id

			// Schedule the task
			if tc.id != "" {
				err = scheduler.AddWithID(tc.id, tc.task)
			} else {
				id, err = scheduler.Add(tc.task)
			}
			if err != nil && !tc.addErr {
				t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
			}
			if err == nil && tc.addErr {
				t.Errorf("Expected errors when scheduling an invalid task")
			}
			defer scheduler.Del(id)

			if tc.id != "" {
				t.Run(tc.name+" - Duplicate Task", func(t *testing.T) {
					// Schedule the task
					err := scheduler.AddWithID(tc.id, tc.task)
					if !errors.Is(err, ErrIDInUse) {
						t.Errorf("Expected errors when scheduling a duplicate task")
					}
				})
			}

			t.Run(tc.name+" - Lookup", func(t *testing.T) {
				// Verify if task exists
				_, err = scheduler.Lookup(id)
				if err != nil && !tc.addErr {
					t.Errorf("Unable to find newly scheduled task with Lookup - %s", err)
				}
				if err == nil && tc.addErr {
					t.Errorf("Found task that should not exist - %s", id)
				}
			})

			t.Run(tc.name+" - Has", func(t *testing.T) {
				// Verify if task exists
				exists := scheduler.Has(id)
				if !exists && !tc.addErr {
					t.Errorf("Unable to find newly scheduled task with Has - %s", err)
				}
				if exists && tc.addErr {
					t.Errorf("Found task that should not exist - %s", id)
				}
			})

			t.Run(tc.name+" - Task List", func(t *testing.T) {
				// Check Task Map
				tasks := scheduler.Tasks()
				if len(tasks) != 1 && !tc.addErr {
					t.Errorf("Unable to find newly scheduled task with Tasks")
				}
				if len(tasks) > 0 && tc.addErr {
					t.Errorf("Found task that should not exist - %s", id)
				}
			})

			// Reset for the next test
			scheduler.Del(id)
		})
	}
}

func TestTaskExecution(t *testing.T) {
	// Create a base scheduler to use
	scheduler := NewStdScheduler(StdSchedulerOptions{})
	defer scheduler.Stop()

	// Setup table tests
	var tt []ExecutionTestCase

	// Define a basic task
	tc := ExecutionTestCase{
		name:      "Valid Task",
		callsFunc: true,
	}
	tc.ctx, tc.cancel = context.WithCancel(context.Background())
	tc.task = &Task{
		Interval: 1 * time.Second,
		TaskFunc: func() error { return fmt.Errorf("fake error") },
		ErrFunc: func(e error) {
			if e != nil {
				tc.cancel()
			}
		},
	}
	tt = append(tt, tc)

	// Define a task with TaskContext
	tc2 := ExecutionTestCase{
		name:      "Valid Task with TaskContext",
		callsFunc: true,
	}
	tc2.ctx, tc2.cancel = context.WithCancel(context.Background())
	tc2.task = &Task{
		Interval:    1 * time.Second,
		TaskContext: TaskContext{Context: tc2.ctx, Cancel: tc2.cancel},
		FuncWithTaskContext: func(taskCtx TaskContext) error {
			if taskCtx.Context != tc2.ctx {
				t.Logf("TaskContext.Context does not match expected context")
				// return with no error to trigger a timeout failure
				return nil
			}
			return fmt.Errorf("fake error")
		},
		ErrFuncWithTaskContext: func(taskCtx TaskContext, e error) {
			if e != nil {
				tc2.cancel()
			}
			if !errors.Is(taskCtx.Context.Err(), context.Canceled) {
				t.Errorf("TaskContext.Context should be canceled")
			}
		},
	}
	tt = append(tt, tc2)

	// Define a task then cancel it
	tc3 := ExecutionTestCase{
		name: "Cancel a Task before it's called",
	}
	tc3.ctx, tc3.cancel = context.WithCancel(context.Background())
	tc3.task = &Task{
		Interval:    1 * time.Second,
		StartAfter:  time.Now().Add(5 * time.Second),
		TaskContext: TaskContext{Context: tc3.ctx},
		TaskFunc: func() error {
			tc.cancel()
			return nil
		},
		ErrFunc: func(e error) {
			t.Errorf("ErrFunc should not be called")
		},
	}
	tt = append(tt, tc3)

	// Only call ErrFunc if error
	tc4 := ExecutionTestCase{
		name:      "Only call ErrFunc if error",
		callsFunc: true,
	}
	tc4.ctx, tc4.cancel = context.WithCancel(context.Background())
	tc4.task = &Task{
		Interval: 1 * time.Second,
		TaskFunc: func() error {
			tc4.cancel()
			return nil
		},
		ErrFunc: func(e error) {
			t.Errorf("ErrFunc should not be called")
		},
	}
	tt = append(tt, tc4)

	// Only call ErrFuncWithTaskContext if error
	tc5 := ExecutionTestCase{
		name:      "Only call ErrFuncWithTaskContext if error",
		callsFunc: true,
	}
	tc5.ctx, tc5.cancel = context.WithCancel(context.Background())
	tc5.task = &Task{
		Interval:    1 * time.Second,
		TaskContext: TaskContext{Context: tc5.ctx},
		FuncWithTaskContext: func(taskCtx TaskContext) error {
			tc5.cancel()
			return nil
		},
		ErrFuncWithTaskContext: func(taskCtx TaskContext, e error) {
			t.Errorf("ErrFuncWithTaskContext should not be called")
		},
	}
	tt = append(tt, tc5)

	// Validate TaskContext ID
	tc6 := ExecutionTestCase{
		name:      "Validate TaskContext ID",
		callsFunc: true,
		id:        "test-id",
	}
	tc6.ctx, tc6.cancel = context.WithCancel(context.Background())
	tc6.task = &Task{
		Interval:    1 * time.Second,
		TaskContext: TaskContext{Context: tc6.ctx},
		FuncWithTaskContext: func(taskCtx TaskContext) error {
			if taskCtx.ID() != tc6.id {
				t.Errorf("TaskContext.ID does not match expected ID")
			}
			tc6.cancel()
			return nil
		},
		ErrFunc: func(e error) {
			t.Errorf("ErrFunc should not be called")
		},
	}
	tt = append(tt, tc6)

	// Verify that StartAfter time is respected
	tc7 := ExecutionTestCase{
		name:      "Verify StartAfter time is respected",
		callsFunc: true,
	}
	tc7StartAfter := time.Now().Add(5 * time.Second)
	tc7.ctx, tc7.cancel = context.WithCancel(context.Background())
	tc7.task = &Task{
		Interval:    1 * time.Second,
		StartAfter:  tc7StartAfter,
		TaskContext: TaskContext{Context: tc7.ctx},
		FuncWithTaskContext: func(taskCtx TaskContext) error {
			if time.Now().Before(tc7StartAfter) {
				t.Errorf("Task should not have been called before StartAfter time")
				return nil
			}
			tc7.cancel()
			return nil
		},
		ErrFunc: func(e error) {
			t.Errorf("ErrFunc should not be called")
		},
	}
	tt = append(tt, tc7)

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			id := tc.id

			if tc.id != "" {
				err = scheduler.AddWithID(tc.id, tc.task)
			} else {
				id, err = scheduler.Add(tc.task)
			}
			if err != nil {
				t.Errorf("Unexpected errors when scheduling a task - %s", err)
			}

			// Cancel the task if it's not supposed to be called
			if !tc.callsFunc {
				scheduler.Del(id)
			}

			select {
			case <-tc.ctx.Done():
				if tc.callsFunc {
					return
				}
				t.Errorf("Task was executed when it should not have been")
			case <-time.After(10 * time.Second):
				if !tc.callsFunc {
					return
				}
				t.Errorf("Task did not execute within 10 seconds")
			}
		})
	}
}

func TestAdd(t *testing.T) {
	// Create a base scheduler to use
	scheduler := NewStdScheduler(StdSchedulerOptions{})
	defer scheduler.Stop()

	t.Run("Add a valid task and look it up", func(t *testing.T) {
		id, err := scheduler.Add(&Task{
			Interval: 1 * time.Minute,
			TaskFunc: func() error { return nil },
			ErrFunc:  func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		_, err = scheduler.Lookup(id)
		if err != nil {
			t.Errorf("Unable to find newly scheduled task with Lookup - %s", err)
		}

		tt := scheduler.Tasks()
		if len(tt) < 1 {
			t.Errorf("Unable to find newly scheduled task with Tasks")
		}

	})

	t.Run("Add a valid task with an id and look it up", func(t *testing.T) {
		id := xid.New()
		err := scheduler.AddWithID(id.String(), &Task{
			Interval: 1 * time.Minute,
			TaskFunc: func() error { return nil },
			ErrFunc:  func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		_, err = scheduler.Lookup(id.String())
		if err != nil {
			t.Errorf("Unable to find newly scheduled task with Lookup - %s", err)
		}

		tt := scheduler.Tasks()
		if len(tt) < 1 {
			t.Errorf("Unable to find newly scheduled task with Tasks")
		}

	})

	t.Run("Add a invalid task with an duplicate id and look it up", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error {
				doneCh <- struct{}{}
				return nil
			},
			ErrFunc: func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		err = scheduler.AddWithID(id, &Task{
			Interval: 1 * time.Minute,
			TaskFunc: func() error { return nil },
			ErrFunc:  func(e error) {},
		})
		if !errors.Is(err, ErrIDInUse) {
			t.Errorf("Expected error for task with existing id")
		}

		_, err = scheduler.Lookup(id)
		if err != nil {
			t.Errorf("Unable to find previously scheduled task with Lookup - %s", err)
		}
	})

	t.Run("Check for nil callback", func(t *testing.T) {
		_, err := scheduler.Add(&Task{
			Interval: 1 * time.Minute,
			ErrFunc:  func(e error) {},
		})
		if err == nil {
			t.Errorf("Unexpected success when scheduling an invalid task - %s", err)
		}
	})

	t.Run("Check for nil interval", func(t *testing.T) {
		_, err := scheduler.Add(&Task{
			TaskFunc: func() error { return nil },
			ErrFunc:  func(e error) {},
		})
		if err == nil {
			t.Errorf("Unexpected success when scheduling an invalid task - %s", err)
		}
	})
}

func TestScheduler(t *testing.T) {
	// Create a base scheduler to use
	scheduler := NewStdScheduler(StdSchedulerOptions{})

	t.Run("Verify Tasks Run when Added", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error {
				doneCh <- struct{}{}
				return nil
			},
			ErrFunc: func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}
		defer scheduler.Del(id)

		// Make sure it runs especially when we want it too
		for i := 0; i < 6; i++ {
			select {
			case <-doneCh:
				continue
			case <-time.After(2 * time.Second):
				t.Errorf("StdScheduler failed to execute the scheduled tasks %d run within 2 seconds", i)
			}
		}
	})

	t.Run("Verify TasksWithContext Run when Added", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// User-defined context
		ctx, cancel := context.WithCancel(context.Background())

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval:    1 * time.Second,
			TaskContext: TaskContext{Context: ctx},
			FuncWithTaskContext: func(_ TaskContext) error {
				cancel()
				return fmt.Errorf("fake Error")
			},
			ErrFuncWithTaskContext: func(ctx TaskContext, e error) {
				if ctx.Context != nil && errors.Is(ctx.Context.Err(), context.Canceled) {
					doneCh <- struct{}{}
				}
			},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}
		defer scheduler.Del(id)

		// Make sure it runs especially when we want it too
		for i := 0; i < 6; i++ {
			select {
			case <-doneCh:
				continue
			case <-time.After(2 * time.Second):
				t.Errorf("StdScheduler failed to execute the scheduled tasks %d run within 2 seconds", i)
			}
		}
	})

	t.Run("Verify StartAfter works as expected", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// Create a Start time
		sa := time.Now().Add(10 * time.Second)

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval:   1 * time.Second,
			StartAfter: sa,
			TaskFunc: func() error {
				doneCh <- struct{}{}
				return nil
			},
			ErrFunc: func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}
		defer scheduler.Del(id)

		// Make sure it runs especially when we want it too
		select {
		case <-doneCh:
			if time.Now().Before(sa) {
				t.Errorf("Task executed before the defined start time now %s, supposed to be %s", time.Now().String(), sa.String())
			}
			return
		case <-time.After(15 * time.Second):
			t.Errorf("StdScheduler failed to execute the scheduled tasks within 15 seconds")
		}
	})
}

func TestSchedulerDoesntRun(t *testing.T) {
	// Create a base scheduler to use
	scheduler := NewStdScheduler(StdSchedulerOptions{})

	t.Run("Verify Cancelling a StartAfter works as expected", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// Create a Start time
		sa := time.Now().Add(10 * time.Second)

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval:   1 * time.Second,
			StartAfter: sa,
			TaskFunc: func() error {
				doneCh <- struct{}{}
				return nil
			},
			ErrFunc: func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		// Remove task before it can be scheduled
		scheduler.Del(id)

		// Make sure it doesn't run
		select {
		case <-doneCh:
			t.Errorf("Task executed it was supposed to be cancelled")
			return
		case <-time.After(15 * time.Second):
			return
		}
	})

	t.Run("Verify Tasks Dont run when Deleted", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error {
				doneCh <- struct{}{}
				return nil
			},
			ErrFunc: func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}
		defer scheduler.Del(id)

		// Make sure it runs especially when we want it too
		for i := 0; i < 6; i++ {
			select {
			case <-doneCh:
				if i == 2 {
					scheduler.Del(id)
				}
				if i > 2 {
					t.Errorf("Task should not have exceeded 2, count is %d", i)
				}
				continue
			case <-time.After(2 * time.Second):
				if i > 2 {
					return
				}
				t.Errorf("StdScheduler failed to execute the scheduled tasks %d run within 2 seconds", i)
			}
		}
	})
}

func TestTaskCancellation(t *testing.T) {
	scheduler := NewStdScheduler(StdSchedulerOptions{})

	t.Run("Verify Cancelling interrupts task execution", func(t *testing.T) {
		assert := assertions.New(t)

		doneCh := make(chan struct{})
		execStartedCh := make(chan struct{})

		// Setup A task
		id, err := scheduler.Add(&Task{
			RunOnce:    true,
			StartAfter: time.Now(),
			FuncWithTaskContext: func(taskCtx TaskContext) error {
				go func() {
					execStartedCh <- struct{}{}
				}()

				select {
				case <-taskCtx.Context.Done():
					return taskCtx.Context.Err()
				case <-time.After(1 * time.Second):
				}

				doneCh <- struct{}{}

				return nil
			},
			ErrFunc: func(e error) {
				assert.ErrorIs(e, context.Canceled)
			},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		// wait task to start execution
		<-execStartedCh

		// cancel task by deletion.
		scheduler.Del(id)

		// Make sure it doesn't run
		select {
		case <-doneCh:
			t.Errorf("Task executed it was supposed to be cancelled")
			return
		case <-time.After(2 * time.Second):
			return
		}
	})
}

func TestSchedulerWorkerLimit(t *testing.T) {
	scheduler := NewStdScheduler(StdSchedulerOptions{WorkerLimit: 5})

	t.Run("Verify WorkerLimit applies", func(t *testing.T) {
		doneCh := make(chan struct{})
		t.Cleanup(func() { close(doneCh) })

		done := false
		t.Cleanup(func() {
			done = true
		})

		taskIDs := make([]string, 0, 10)

		for i := 0; i < 10; i++ {
			id, err := scheduler.Add(&Task{
				Interval: 200 * time.Millisecond,
				RunOnce:  true,
				TaskFunc: func() error {
					time.Sleep(time.Second)
					if done {
						return nil
					}

					doneCh <- struct{}{}
					return nil
				},
				ErrFunc: func(err error) {},
			})
			if err != nil {
				t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
			}

			taskIDs = append(taskIDs, id)
		}

		t.Cleanup(func() {
			for _, id := range taskIDs {
				scheduler.Del(id)
			}
		})

		timer := time.NewTimer(1300 * time.Millisecond)
		t.Cleanup(func() {
			timer.Stop()
		})

		for i := 0; i < 10; i++ {
			select {
			case <-doneCh:
				if i >= 5 {
					t.Errorf("WorkerLimit didn't apply, "+
						"task counter should not have exceeded 5, count is %d", i)
				}

				continue
			case <-timer.C:
				if i >= 5 {
					return
				}

				t.Errorf("StdScheduler failed to execute the scheduled task %d run within 1300 ms", i)

				return
			}
		}
	})
}

func TestRetriesOnError(t *testing.T) {
	scheduler := NewStdScheduler(StdSchedulerOptions{})

	t.Run("Verify task retries on error", func(t *testing.T) {
		attemptsCh := make(chan struct{})
		t.Cleanup(func() { close(attemptsCh) })

		id, err := scheduler.Add(&Task{
			Interval:             200 * time.Millisecond,
			RunOnce:              true,
			RetriesOnError:       3,
			RetryOnErrorInterval: 10 * time.Millisecond,
			TaskFunc: func() error {
				attemptsCh <- struct{}{}
				return errors.New("some error")
			},
			ErrFunc: func(err error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		t.Cleanup(func() {
			scheduler.Del(id)
		})

		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()

		for i := 0; i < 10; i++ {
			select {
			case <-attemptsCh:
				if i > 3 {
					t.Errorf("too many attempts (%d) for the task", i)
				}

			case <-timer.C:
				if i > 3 {
					return
				}

				t.Error("StdScheduler failed to execute the scheduled task run with attempts within 500 ms")

				return
			}
		}
	})

	t.Run("Verify schedule error on adding run once task with retries on error", func(t *testing.T) {
		assert := assertions.New(t)

		id, err := scheduler.Add(&Task{
			Interval:       200 * time.Millisecond,
			RunOnce:        true,
			RetriesOnError: 3,
			TaskFunc: func() error {
				return nil
			},
			ErrFunc: func(err error) {},
		})

		assert.ErrorIs(err, ErrRetryOnErrorIntervalEmpty)

		t.Cleanup(func() {
			scheduler.Del(id)
		})
	})
}

func TestRescheduleOnError(t *testing.T) {
	scheduler := NewStdScheduler(StdSchedulerOptions{})

	t.Run("Verify task reschedules on a specific error", func(t *testing.T) {
		attemptsCh := make(chan struct{})
		t.Cleanup(func() { close(attemptsCh) })

		var specificErr = errors.New("some specific error")

		task := &Task{
			StartAfter: time.Now().Add(10 * time.Millisecond),
			RunOnce:    true,
			TaskFunc: func() error {
				attemptsCh <- struct{}{}
				return specificErr
			},
			ErrFunc: func(err error) {},
		}

		task.WithRescheduleOnError(specificErr, 100*time.Millisecond, 3)

		id, err := scheduler.Add(task)
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		t.Cleanup(func() {
			scheduler.Del(id)
		})

		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()

		for i := 0; i < 10; i++ {
			select {
			case <-attemptsCh:
				if i > 3 {
					t.Errorf("too many attempts (%d) for the task", i)
				}

			case <-timer.C:
				if i > 3 {
					return
				}

				t.Error("StdScheduler failed to execute the scheduled task run with attempts within 500 ms")

				return
			}
		}

	})
}

func TestTaskLimit(t *testing.T) {
	scheduler := NewStdScheduler(StdSchedulerOptions{TaskLimit: 5})

	t.Run("Verify TaskLimit applies", func(t *testing.T) {
		assert := assertions.New(t)

		taskIDs := make([]string, 0, 10)

		for i := 0; i < 10; i++ {
			id, err := scheduler.Add(&Task{
				Interval: 200 * time.Millisecond,
				RunOnce:  true,
				TaskFunc: func() error {
					return nil
				},
				ErrFunc: func(err error) {},
			})
			if i >= 5 {
				assert.ErrorIs(err, ErrTaskLimitExceeded)
			} else {
				if err != nil {
					t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
				}
			}

			taskIDs = append(taskIDs, id)
		}

		t.Cleanup(func() {
			for _, id := range taskIDs {
				scheduler.Del(id)
			}
		})
	})
}

func TestSchedulerLogger(t *testing.T) {
	var b bytes.Buffer

	simpleLogger := logger.NewSimpleLogger(log.New(&b, "", log.LstdFlags), logger.LevelDebug)

	scheduler := NewStdScheduler(StdSchedulerOptions{Logger: simpleLogger})

	t.Run("Verify Logger applies", func(t *testing.T) {
		assert := assertions.New(t)

		doneCh := make(chan struct{})

		id := xid.New().String()

		startAfter := time.Now().Add(200 * time.Millisecond)
		err := scheduler.AddWithID(id, &Task{
			StartAfter: startAfter,
			RunOnce:    true,
			TaskFunc: func() error {
				doneCh <- struct{}{}

				return nil
			},
			ErrFunc: func(err error) {},
		})
		assert.NoError(err)

		t.Cleanup(func() {
			scheduler.Del(id)
		})

		select {
		case <-doneCh:
			// sleep to wait for log write.
			time.Sleep(10 * time.Millisecond)
		case <-time.After(time.Second):
			t.Errorf("StdScheduler failed to execute the scheduled task (%s) run within 1 second", id)
		}

		assert.Contains(b.String(), fmt.Sprintf("task (id: %s) has been scheduled at %s",
			id, startAfter.Format(time.RFC3339)))
		assert.Contains(b.String(), fmt.Sprintf("task (id: %s) has been successfully executed", id))
	})

}

func TestSchedulerExtras(t *testing.T) {
	// Create a base scheduler to use
	scheduler := NewStdScheduler(StdSchedulerOptions{})

	t.Run("Verify RunOnce works as expected", func(t *testing.T) {
		// Channel for orchestrating when the task ran
		doneCh := make(chan struct{})

		// Setup A task
		id, err := scheduler.Add(&Task{
			Interval: 1 * time.Second,
			RunOnce:  true,
			TaskFunc: func() error {
				doneCh <- struct{}{}
				return nil
			},
			ErrFunc: func(e error) {},
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}
		defer scheduler.Del(id)

		// Make sure it runs especially when we want it too
		for i := 0; i < 6; i++ {
			select {
			case <-doneCh:
				if i >= 1 {
					t.Errorf("Task should not have exceeded 1, count is %d", i)
				}
				continue
			case <-time.After(2 * time.Second):
				if i == 1 {
					return
				}
				t.Errorf("StdScheduler failed to execute the scheduled tasks %d run within 2 seconds", i)
			}
		}
	})

	t.Run("Test ErrFunc gets called on errors", func(t *testing.T) {
		// Create a channel to signal function exec
		doneCh := make(chan struct{})

		// Add task
		_, err := scheduler.Add(&Task{
			Interval: 1 * time.Second,
			TaskFunc: func() error { return fmt.Errorf("errors are bad") },
			ErrFunc:  func(e error) { doneCh <- struct{}{} },
		})
		if err != nil {
			t.Errorf("Unexpected errors when scheduling a valid task - %s", err)
		}

		// Wait for success, or timeout
		select {
		case <-doneCh:
			return
		case <-time.After(2 * time.Second):
			t.Errorf("Error function was not called when an error occurred")
		}
	})
}
