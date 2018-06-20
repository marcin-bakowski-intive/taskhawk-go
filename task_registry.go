/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"
	"reflect"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// TaskRegistry manages and dispatches tasks registered to this registry
type TaskRegistry struct {
	// Publisher is used to publish messages to taskhawk infra for async executation
	publisher IPublisher

	tasks map[string]*taskDef
}

// NewTaskRegistry creates a task registry
func NewTaskRegistry(publisher IPublisher) (*TaskRegistry, error) {
	return &TaskRegistry{
		publisher: publisher,
		tasks:     map[string]*taskDef{},
	}, nil
}

func (tr *TaskRegistry) getTask(name string) (*taskDef, error) {
	taskDef, ok := tr.tasks[name]
	if !ok {
		return nil, errors.New(ErrStringTaskNotFound)
	}
	return taskDef, nil
}

// DispatchWithPriority dispatches a task asynchronously with custom priority.
// The concrete type of input is expected to be same as the concrete type of NewInput()'s return value.
func (tr *TaskRegistry) DispatchWithPriority(ctx context.Context, taskName string, priority Priority, input interface{}) error {
	ctx = withSettings(ctx, tr.publisher.Settings())

	taskDef, err := tr.getTask(taskName)
	if err != nil {
		return errors.New("task has not been registered: make sure " +
			"`RegisterTask` is called before dispatching")
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
		taskDef,
		tr,
	)
	if err != nil {
		return err
	}

	if getSync(ctx) {
		return taskDef.Run(ctx, input)
	}

	return tr.publisher.Publish(ctx, message)
}

// DispatchWithContext dispatches a task asynchronously with context.
// The concrete type of input is expected to be same as the concrete type of NewInput()'s return value.
func (tr *TaskRegistry) DispatchWithContext(ctx context.Context, taskName string, input interface{}) error {
	taskDef, err := tr.getTask(taskName)
	if err != nil {
		return errors.New("task has not been registered: make sure " +
			"`RegisterTask` is called before dispatching")
	}
	return tr.DispatchWithPriority(ctx, taskName, taskDef.Priority(), input)
}

// Dispatch a task asynchronously. The concrete type of input is expected to be same as the concrete type of
// NewInput()'s return value.
func (tr *TaskRegistry) Dispatch(taskName string, input interface{}) error {
	return tr.DispatchWithContext(context.Background(), taskName, input)
}

// NewLambdaConsumer creates a new taskhawk consumer for lambda apps
//
// Cancelable context may be used to cancel processing of messages
func (tr *TaskRegistry) NewLambdaConsumer(sessionCache *AWSSessionsCache, settings *Settings) ILambdaConsumer {
	return &lambdaConsumer{
		awsClient:    newAmazonWebServices(withSettings(context.Background(), settings), sessionCache),
		settings:     settings,
		taskRegistry: tr,
	}
}

// NewQueueConsumer creates a new taskhawk consumer for queue apps
//
// Cancelable context may be used to cancel processing of messages
func (tr *TaskRegistry) NewQueueConsumer(sessionCache *AWSSessionsCache, settings *Settings) IQueueConsumer {
	return &queueConsumer{
		awsClient:    newAmazonWebServices(withSettings(context.Background(), settings), sessionCache),
		settings:     settings,
		taskRegistry: tr,
	}
}

// RegisterTask registers the task to the task registry
func (tr *TaskRegistry) RegisterTask(task ITask) error {
	if task.Name() == "" {
		return errors.New("task name not set")
	}
	if _, found := tr.tasks[task.Name()]; found {
		return errors.Errorf("task with name '%s' already registered", task.Name())
	}
	inputType := reflect.TypeOf(task.NewInput())
	if inputType != nil && inputType.Kind() != reflect.Ptr {
		// since metadata methods are implemented on Ptr type, let's be strict about this to avoid confusion
		return errors.Errorf("method NewInput must return a pointer type")
	}
	tr.tasks[task.Name()] = newTaskDef(task, tr)
	return nil
}
