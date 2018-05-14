/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

// ITask is an interface all TaskHawk tasks are expected to implement
type ITask interface {
	// Name of the task. This is used to serialize/deserialize tasks, and so should be changed carefully
	Name() string

	// Priority of the task by default. A publisher _may_ chose to override.
	Priority() Priority

	// NewInput returns an empty input struct as expected by the Task's Run method. May be `nil`
	// If your task needs to get custom headers set during dispatch, implement interface ITaskHeaders,
	// or embed TaskHeaders
	// If your task needs to get metadata (message id etc), implement interface ITaskMetadata, or embed TaskMetadata
	NewInput() interface{}

	// Run is the main method for a task. The conrete type of the input parameter will be same as whatever is
	// returned by the NewInput() method.
	Run(context context.Context, input interface{}) error
}

type inputer func() interface{}

// Task is a base struct that should be embedded in all TaskHawk tasks.
// It provides partial implementation of the ITask interface by implementing a few methods
type Task struct {
	// Publisher is used to publish messages to taskhawk infra for async executation
	Publisher IPublisher

	// TaskName represents the name of the task
	TaskName string

	// Inputer is a function that returns an empty input object as the task expects.
	// This is optional and if not specified, it implies task doesn't require input
	Inputer inputer

	// DefaultPriority is the default priority of a task. This may be overridden at a specific message level.
	DefaultPriority Priority
}

// Name returns the task name
func (t *Task) Name() string {
	return t.TaskName
}

// NewInput returns an empty input struct of concrete type same as the concrete type expected by the Task's Run method.
func (t *Task) NewInput() interface{} {
	if t.Inputer == nil {
		return nil
	}
	return t.Inputer()
}

// Priority returns the default priority of a task
func (t *Task) Priority() Priority {
	return t.DefaultPriority
}

// DispatchWithPriority dispatches a task asynchronously with custom priority.
// The concrete type of input is expected to be same as the concrete type of NewInput()'s return value.
func (t *Task) DispatchWithPriority(ctx context.Context, priority Priority, input interface{}) error {
	ctx = withSettings(ctx, t.Publisher.Settings())

	taskDef, ok := taskRegistry[t.Name()]
	if !ok {
		return errors.New("task has not been registered: make sure `taskhawk." +
			"RegisterTask` is called before dispatching")
	}

	headers := make(map[string]string)
	for key, value := range getDefaultHeaders(ctx)(ctx, taskDef) {
		headers[key] = value
	}
	if inputTaskHeaders, ok := input.(ITaskHeaders); ok {
		for key, value := range inputTaskHeaders.GetHeaders() {
			headers[key] = value
		}
	}

	message, err := newMessage(
		input,
		headers,
		uuid.Must(uuid.NewV4()).String(),
		priority,
		taskRegistry[t.Name()],
	)
	if err != nil {
		return err
	}

	if getSync(ctx) {
		return taskDef.Run(ctx, input)
	}

	return t.Publisher.Publish(ctx, message)
}

// DispatchWithContext dispatches a task asynchronously with context.
// The concrete type of input is expected to be same as the concrete type of NewInput()'s return value.
func (t *Task) DispatchWithContext(ctx context.Context, input interface{}) error {
	return t.DispatchWithPriority(ctx, t.Priority(), input)
}

// Dispatch a task asynchronously. The concrete type of input is expected to be same as the concrete type of
// NewInput()'s return value.
func (t *Task) Dispatch(input interface{}) error {
	return t.DispatchWithContext(context.Background(), input)
}

// ITaskHeaders interface needs to be implemented by the input struct if your task needs to get custom headers set
// during dispatch
type ITaskHeaders interface {
	// SetHeaders sets the headers on a task input
	SetHeaders(map[string]string)

	// GetHeaders returns the headers set on a task input
	GetHeaders() map[string]string
}

// TaskHeaders provides a default implementation for ITaskHeaders and may be embedded in your input struct
type TaskHeaders struct {
	Headers map[string]string
}

// GetHeaders returns the custom headers passed when the task was dispatched
func (h *TaskHeaders) GetHeaders() map[string]string {
	return h.Headers
}

// SetHeaders sets the custom headers passed when the task was dispatched
func (h *TaskHeaders) SetHeaders(headers map[string]string) {
	h.Headers = headers
}

// ITaskMetadata interface needs to be implemented by the input struct if your task needs to get metatada (
// message id etc)
type ITaskMetadata interface {
	// SetID sets the message id
	SetID(string)

	// SetPriority sets the priority a message was dispatched with
	SetPriority(Priority)

	// SetReceipt sets the message receipt from SQS.
	// This may be used to extend visibility timeout for long running tasks
	SetReceipt(string)

	// SetTimestamp sets the message dispatch timestamp
	SetTimestamp(JSONTime)

	// SetVersion sets the message schema version
	SetVersion(Version)
}

// TaskMetadata provides a default implementation for ITaskMetadata and may be embedded in your input struct
type TaskMetadata struct {
	ID        string
	Priority  Priority
	Receipt   string
	Timestamp JSONTime
	Version   Version
}

// SetID sets the message id
func (m *TaskMetadata) SetID(id string) {
	m.ID = id
}

// SetPriority sets the priority a message was dispatched with
func (m *TaskMetadata) SetPriority(priority Priority) {
	m.Priority = priority
}

// SetReceipt sets the message receipt from SQS.
// This may be used to extend visibility timeout for long running tasks
func (m *TaskMetadata) SetReceipt(receipt string) {
	m.Receipt = receipt
}

// SetTimestamp sets the message dispatch timestamp
func (m *TaskMetadata) SetTimestamp(time JSONTime) {
	m.Timestamp = time
}

// SetVersion sets the message schema version
func (m *TaskMetadata) SetVersion(version Version) {
	m.Version = version
}

// This has to be separate from Task above since it embeds ITask,
// whereas tasks are expected to implement ITask interface themselves
type taskDef struct {
	ITask
}

func (t taskDef) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Name())
}

func (t *taskDef) UnmarshalJSON(b []byte) error {
	var taskName string
	if err := json.Unmarshal(b, &taskName); err != nil {
		return err
	}
	task, found := taskRegistry[taskName]
	if !found {
		return fmt.Errorf("invalid task not found")
	}
	*t = *task
	return nil
}

func (t *taskDef) call(ctx context.Context, message *message, receipt string) error {
	if metadata, ok := message.Input.(ITaskMetadata); ok {
		metadata.SetID(message.ID)
		metadata.SetPriority(message.Metadata.Priority)
		metadata.SetReceipt(receipt)
		metadata.SetTimestamp(message.Metadata.Timestamp)
		metadata.SetVersion(message.Metadata.Version)
	}
	if headers, ok := message.Input.(ITaskHeaders); ok {
		headers.SetHeaders(message.Headers)
	}
	return t.Run(ctx, message.Input)
}

var taskRegistry = make(map[string]*taskDef)

// RegisterTask registers a Taskhawk task. A task needs to be registered before it can be dispatched or run.
func RegisterTask(task ITask) error {
	if task.Name() == "" {
		return errors.New("task name not set")
	}
	if _, found := taskRegistry[task.Name()]; found {
		return fmt.Errorf("task with name '%s' already registered", task.Name())
	}
	inputType := reflect.TypeOf(task.NewInput())
	if inputType != nil && inputType.Kind() != reflect.Ptr {
		// since metadata methods are implemented on Ptr type, let's be strict about this to avoid confusion
		return fmt.Errorf("method NewInput must return a pointer type")
	}
	taskRegistry[task.Name()] = &taskDef{task}
	return nil
}
