/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func CleanupTaskRegistry() {
	for k := range taskRegistry {
		delete(taskRegistry, k)
	}
}

// Implements ITask
type SendEmailTask struct {
	Task
	mock.Mock
}

func NewSendEmailTask() *SendEmailTask {
	return &SendEmailTask{
		Task: Task{
			Publisher: fakePublisher,
			TaskName:  "task_test.SendEmailTask",
			Inputer: func() interface{} {
				return new(SendEmailTaskInput)
			},
		},
	}
}

type SendEmailTaskInput struct {
	To      string
	Subject string
	From    string `json:",omitempty"`
}

func (t *SendEmailTask) Run(ctx context.Context, rawInput interface{}) error {
	input := rawInput.(*SendEmailTaskInput)
	args := t.Called(ctx, input)
	return args.Error(0)
}

func TestRegisterTask(t *testing.T) {
	defer CleanupTaskRegistry()

	assert.NoError(t, RegisterTask(NewSendEmailTask()))
	assert.Contains(t, taskRegistry, "task_test.SendEmailTask")
}

func TestCall(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))
	ctx := context.Background()

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	task.On("Run", ctx, input).Return(nil)

	receipt := uuid.Must(uuid.NewV4()).String()
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input: &SendEmailTaskInput{
			To:      "mail@example.com",
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		},
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTask"],
	}
	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
}

func TestDispatchWithPriority(t *testing.T) {
	defer CleanupTaskRegistry()
	defer resetFakePublisher()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	ctxWithSettings := withSettings(context.Background(), task.Publisher.Settings())

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}

	expectedMessage := &message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityLow,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTask"],
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, task.DispatchWithPriority(context.Background(), PriorityLow, input))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

func TestDispatchDefaultHeadersHook(t *testing.T) {
	defer CleanupTaskRegistry()
	defer resetFakePublisher()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	input := &SendEmailTaskInput{}

	publisherRequestId := uuid.Must(uuid.NewV4()).String()

	fakePublisher.settings = &Settings{
		AWSAccountID: "1234567890",
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		DefaultHeaders: func(ITask) map[string]string {
			return map[string]string{"request_id": publisherRequestId}
		},
	}

	ctxWithSettings := withSettings(context.Background(), task.Publisher.Settings())

	expectedMessage := &message{
		Headers: map[string]string{"request_id": publisherRequestId},
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityLow,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTask"],
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, task.DispatchWithPriority(context.Background(), PriorityLow, input))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

func TestDispatchNotRegisteredFail(t *testing.T) {
	defer resetFakePublisher()

	task := NewSendEmailTask()

	input := &SendEmailTaskInput{}

	assert.EqualError(t, task.Dispatch(input), "task has not been registered: make sure `taskhawk."+
		"RegisterTask` is called before dispatching")
}

func TestDispatchSync(t *testing.T) {
	defer CleanupTaskRegistry()
	defer resetFakePublisher()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

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

	settings := task.Publisher.Settings()
	ctx := context.Background()
	ctxWithSettings := withSettings(ctx, settings)

	task.On("Run", ctxWithSettings, input).Return(nil)

	assert.NoError(t, task.DispatchWithContext(ctx, input))
	task.AssertExpectations(t)
}

type SendEmailTaskNoInput struct {
	Task
	mock.Mock
}

func NewSendEmailTaskNoInput() *SendEmailTaskNoInput {
	return &SendEmailTaskNoInput{
		Task: Task{
			Publisher: fakePublisher,
			TaskName:  "task_test.SendEmailTaskNoInput",
		},
	}
}

func (t *SendEmailTaskNoInput) Run(ctx context.Context, input interface{}) error {
	args := t.Called(ctx, input)
	return args.Error(0)
}

func TestRegisterTaskNoInput(t *testing.T) {
	defer CleanupTaskRegistry()

	assert.NoError(t, RegisterTask(NewSendEmailTaskNoInput()))
	assert.Contains(t, taskRegistry, "task_test.SendEmailTaskNoInput")
}

func TestCallNoInput(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTaskNoInput()
	require.NoError(t, RegisterTask(task))
	ctx := context.Background()

	task.On("Run", ctx, nil).Return(nil)

	receipt := uuid.Must(uuid.NewV4()).String()
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input:   nil,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTaskNoInput"],
	}
	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
}

func TestDispatchNoInput(t *testing.T) {
	defer CleanupTaskRegistry()
	defer resetFakePublisher()

	task := NewSendEmailTaskNoInput()
	require.NoError(t, RegisterTask(task))

	ctxWithSettings := withSettings(context.Background(), task.Publisher.Settings())

	expectedMessage := &message{
		Headers: map[string]string{},
		ID:      "message-id",
		Input:   nil,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTaskNoInput"],
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, task.Dispatch(nil))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

type SendEmailTaskHeaders struct {
	Task
	mock.Mock
}

func NewSendEmailTaskHeaders() *SendEmailTaskHeaders {
	return &SendEmailTaskHeaders{
		Task: Task{
			Publisher: fakePublisher,
			TaskName:  "task_test.SendEmailTaskHeaders",
			Inputer: func() interface{} {
				return new(SendEmailTaskHeadersInput)
			},
		},
	}
}

type SendEmailTaskHeadersInput struct {
	TaskHeaders
}

func (t *SendEmailTaskHeaders) Run(ctx context.Context, rawInput interface{}) error {
	input := rawInput.(*SendEmailTaskHeadersInput)
	args := t.Called(ctx, input)
	return args.Error(0)
}

func TestRegisterTaskSendEmailTaskHeaders(t *testing.T) {
	defer CleanupTaskRegistry()

	assert.NoError(t, RegisterTask(NewSendEmailTaskHeaders()))
	assert.Contains(t, taskRegistry, "task_test.SendEmailTaskHeaders")
}

func TestCallHeaders(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTaskHeaders()
	require.NoError(t, RegisterTask(task))
	ctx := context.Background()

	receipt := uuid.Must(uuid.NewV4()).String()
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input:   task.NewInput(),
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTaskHeaders"],
	}
	expectedInput := &SendEmailTaskHeadersInput{}
	expectedInput.Headers = message.Headers

	task.On("Run", ctx, expectedInput).Return(nil)

	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
}

func TestDispatchHeaders(t *testing.T) {
	defer CleanupTaskRegistry()
	defer resetFakePublisher()

	task := NewSendEmailTaskHeaders()
	require.NoError(t, RegisterTask(task))

	ctxWithSettings := withSettings(context.Background(), task.Publisher.Settings())

	input := &SendEmailTaskHeadersInput{}
	input.Headers = map[string]string{
		"request_id": uuid.Must(uuid.NewV4()).String(),
	}

	expectedMessage := &message{
		Headers: input.Headers,
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry[task.Name()],
	}
	fakePublisher.On("Publish", ctxWithSettings, mock.Anything).Return(nil)

	assert.NoError(t, task.Dispatch(input))

	actual := fakePublisher.Calls[0].Arguments.Get(1).(*message)
	// don't check dynamic things
	expectedMessage.ID = actual.ID
	expectedMessage.Metadata.Timestamp = actual.Metadata.Timestamp
	assert.Equal(t, expectedMessage, actual)
	fakePublisher.AssertExpectations(t)
}

type SendEmailTaskMetadata struct {
	Task
	mock.Mock
}

type SendEmailTaskMetadataInput struct {
	TaskMetadata
}

func (t *SendEmailTaskMetadata) Run(ctx context.Context, rawInput interface{}) error {
	input := rawInput.(*SendEmailTaskMetadataInput)
	args := t.Called(ctx, input)
	return args.Error(0)
}

func (t *SendEmailTaskMetadata) NewInput() interface{} {
	return &SendEmailTaskMetadataInput{}
}

func (t *SendEmailTaskMetadata) Name() string {
	return "task_test.SendEmailTaskMetadata"
}

func TestRegisterTaskSendEmailTaskMetadata(t *testing.T) {
	defer CleanupTaskRegistry()

	assert.NoError(t, RegisterTask(&SendEmailTaskMetadata{}))
	assert.Contains(t, taskRegistry, "task_test.SendEmailTaskMetadata")
}

func TestCallMetadata(t *testing.T) {
	defer CleanupTaskRegistry()

	task := &SendEmailTaskMetadata{}
	require.NoError(t, RegisterTask(task))
	ctx := context.Background()

	receipt := uuid.Must(uuid.NewV4()).String()
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input:   &SendEmailTaskMetadataInput{},
		Metadata: &metadata{
			Priority:  PriorityHigh,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTaskMetadata"],
	}

	expectedInput := &SendEmailTaskMetadataInput{}
	expectedInput.ID = message.ID
	expectedInput.SetPriority(PriorityHigh)
	expectedInput.SetReceipt(receipt)
	expectedInput.SetTimestamp(message.Metadata.Timestamp)
	expectedInput.SetVersion(message.Metadata.Version)
	task.On("Run", ctx, expectedInput).Return(nil)

	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
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

func TestRegisterTaskSendEmailTaskDuplicate(t *testing.T) {
	defer CleanupTaskRegistry()

	require.NoError(t, RegisterTask(NewSendEmailTask()))
	assert.EqualError(t, RegisterTask(&SendEmailTaskDuplicate{}),
		"task with name 'task_test.SendEmailTask' already registered")
}

func TestRegisterTaskSendEmailTaskNoName(t *testing.T) {
	defer CleanupTaskRegistry()

	assert.EqualError(t, RegisterTask(&SendEmailTask{}), "task name not set")
}
