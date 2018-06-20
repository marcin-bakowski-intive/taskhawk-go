/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package taskhawk

import (
	"context"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDispatchWithPriority(t *testing.T) {
	defer resetFakePublisher()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	ctxWithSettings := withSettings(context.Background(), taskRegistry.publisher.Settings())

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}

	td, err := taskRegistry.getTask("task_test.SendEmailTask")
	require.NoError(t, err)
	expectedMessage := &message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityLow,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         td,
		taskRegistry: taskRegistry,
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, taskRegistry.DispatchWithPriority(context.Background(), td.Name(), PriorityLow, input))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

func TestDispatchDefaultHeadersHook(t *testing.T) {
	defer resetFakePublisher()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	input := &SendEmailTaskInput{}

	fakePublisher.settings = &Settings{
		AWSAccountID: "1234567890",
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		DefaultHeaders: func(ctx context.Context, _ ITask) map[string]string {
			return map[string]string{"request_id": ctx.Value("request_id").(string)}
		},
	}

	publisherRequestID := uuid.Must(uuid.NewV4()).String()

	ctxWithRequestID := context.WithValue(context.Background(), "request_id", publisherRequestID)
	ctxWithSettings := withSettings(ctxWithRequestID, taskRegistry.publisher.Settings())

	td, err := taskRegistry.getTask("task_test.SendEmailTask")
	require.NoError(t, err)
	expectedMessage := &message{
		Headers: map[string]string{"request_id": publisherRequestID},
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityLow,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         td,
		taskRegistry: taskRegistry,
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, taskRegistry.DispatchWithPriority(ctxWithRequestID, td.Name(), PriorityLow, input))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

func TestDispatchHeaders(t *testing.T) {
	defer resetFakePublisher()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTaskHeaders()
	require.NoError(t, taskRegistry.RegisterTask(task))

	ctxWithSettings := withSettings(context.Background(), taskRegistry.publisher.Settings())

	input := &SendEmailTaskHeadersInput{}
	input.Headers = map[string]string{
		"request_id": uuid.Must(uuid.NewV4()).String(),
	}

	td, err := taskRegistry.getTask(task.Name())
	require.NoError(t, err)
	expectedMessage := &message{
		Headers: input.Headers,
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         td,
		taskRegistry: taskRegistry,
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, taskRegistry.Dispatch(task.Name(), input))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

func TestDispatchNotRegisteredFail(t *testing.T) {
	defer resetFakePublisher()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()

	input := &SendEmailTaskInput{}

	assert.EqualError(t, taskRegistry.Dispatch(task.Name(), input), "task has not been registered: make sure "+
		"`RegisterTask` is called before dispatching")
}

func TestDispatchSync(t *testing.T) {
	defer resetFakePublisher()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	fakePublisher.settings = &Settings{
		AWSAccountID: "1234567890",
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		Sync:         true,
	}
	InitSettings(fakePublisher.settings)
	input := &SendEmailTaskInput{}

	settings := taskRegistry.publisher.Settings()
	ctx := context.Background()
	ctxWithSettings := withSettings(ctx, settings)

	task.On("Run", ctxWithSettings, input).Return(nil)

	assert.NoError(t, taskRegistry.DispatchWithContext(ctx, task.Name(), input))
	task.AssertExpectations(t)
}

func TestDispatchNoInput(t *testing.T) {
	defer resetFakePublisher()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTaskNoInput()
	require.NoError(t, taskRegistry.RegisterTask(task))

	ctxWithSettings := withSettings(context.Background(), taskRegistry.publisher.Settings())

	td, err := taskRegistry.getTask("task_test.SendEmailTaskNoInput")
	require.NoError(t, err)
	expectedMessage := &message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   nil,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         td,
		taskRegistry: taskRegistry,
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, taskRegistry.Dispatch(task.Name(), nil))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

func TestNewLambdaConsumer(t *testing.T) {
	settings := getLambdaTestSettings()

	sessionCache := &AWSSessionsCache{}

	awsClient := &FakeAWS{}
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)

	iconsumer := taskRegistry.NewLambdaConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}

func TestNewQueueConsumer(t *testing.T) {
	settings := getQueueTestSettings()

	sessionCache := &AWSSessionsCache{}

	awsClient := &FakeAWS{}
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)

	iconsumer := taskRegistry.NewQueueConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}

type SendEmailTaskDuplicate struct {
	Task
}

type SendEmailTaskDuplicateInput struct{}

func (SendEmailTaskDuplicate) Run(context.Context, interface{}) error {
	return nil
}

func (t SendEmailTaskDuplicate) NewInput() interface{} {
	return &SendEmailTaskDuplicateInput{}
}

func (t SendEmailTaskDuplicate) Name() string {
	return "task_test.SendEmailTask"
}

func TestRegisterTask(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	assert.NoError(t, taskRegistry.RegisterTask(NewSendEmailTask()))
	assert.Contains(t, taskRegistry.tasks, "task_test.SendEmailTask")
}

func TestRegisterTaskNoInput(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	assert.NoError(t, taskRegistry.RegisterTask(NewSendEmailTaskNoInput()))
	assert.Contains(t, taskRegistry.tasks, "task_test.SendEmailTaskNoInput")
}

func TestRegisterTaskSendEmailTaskDuplicate(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	require.NoError(t, taskRegistry.RegisterTask(NewSendEmailTask()))
	assert.EqualError(t, taskRegistry.RegisterTask(&SendEmailTaskDuplicate{}),
		"task with name 'task_test.SendEmailTask' already registered")
}

func TestRegisterTaskSendEmailTaskHeaders(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	assert.NoError(t, taskRegistry.RegisterTask(NewSendEmailTaskHeaders()))
	assert.Contains(t, taskRegistry.tasks, "task_test.SendEmailTaskHeaders")
}

func TestRegisterTaskSendEmailTaskMetadata(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	assert.NoError(t, taskRegistry.RegisterTask(&SendEmailTaskMetadata{}))
	assert.Contains(t, taskRegistry.tasks, "task_test.SendEmailTaskMetadata")
}

func TestRegisterTaskSendEmailTaskNoName(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	assert.EqualError(t, taskRegistry.RegisterTask(&SendEmailTask{}), "task name not set")
}
