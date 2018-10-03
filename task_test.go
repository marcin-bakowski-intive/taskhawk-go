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

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Implements ITask
type SendEmailTask struct {
	Task
	mock.Mock
}

func NewSendEmailTask() *SendEmailTask {
	return &SendEmailTask{
		Task: Task{
			TaskName: "task_test.SendEmailTask",
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

func TestCall(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))
	ctx := context.Background()

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	task.On("Run", ctx, input).Return(nil)

	receipt := uuid.Must(uuid.NewV4()).String()
	fetchedTask, err := taskRegistry.GetTask("task_test.SendEmailTask")
	require.NoError(t, err)
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
		task:         newTaskDef(fetchedTask, taskRegistry),
		taskRegistry: taskRegistry,
	}
	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
}

type SendEmailTaskNoInput struct {
	Task
	mock.Mock
}

func NewSendEmailTaskNoInput() *SendEmailTaskNoInput {
	return &SendEmailTaskNoInput{
		Task: Task{
			TaskName: "task_test.SendEmailTaskNoInput",
		},
	}
}

func (t *SendEmailTaskNoInput) Run(ctx context.Context, input interface{}) error {
	args := t.Called(ctx, input)
	return args.Error(0)
}

func TestCallNoInput(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTaskNoInput()
	require.NoError(t, taskRegistry.RegisterTask(task))
	ctx := context.Background()

	task.On("Run", ctx, nil).Return(nil)

	receipt := uuid.Must(uuid.NewV4()).String()
	fetchedTask, err := taskRegistry.GetTask("task_test.SendEmailTaskNoInput")
	require.NoError(t, err)
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input:   nil,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         newTaskDef(fetchedTask, taskRegistry),
		taskRegistry: taskRegistry,
	}
	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
}

type SendEmailTaskHeaders struct {
	Task
	mock.Mock
}

func NewSendEmailTaskHeaders() *SendEmailTaskHeaders {
	return &SendEmailTaskHeaders{
		Task: Task{
			TaskName: "task_test.SendEmailTaskHeaders",
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

func TestCallHeaders(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTaskHeaders()
	require.NoError(t, taskRegistry.RegisterTask(task))
	ctx := context.Background()

	receipt := uuid.Must(uuid.NewV4()).String()
	fetchedTask, err := taskRegistry.GetTask("task_test.SendEmailTaskHeaders")
	require.NoError(t, err)
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input:   task.NewInput(),
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         newTaskDef(fetchedTask, taskRegistry),
		taskRegistry: taskRegistry,
	}
	expectedInput := &SendEmailTaskHeadersInput{}
	expectedInput.Headers = message.Headers

	task.On("Run", ctx, expectedInput).Return(nil)

	require.NoError(t, message.validate())
	assert.NoError(t, message.callTask(ctx, receipt))
	task.AssertExpectations(t)
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

func TestCallMetadata(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := &SendEmailTaskMetadata{}
	require.NoError(t, taskRegistry.RegisterTask(task))
	ctx := context.Background()

	receipt := uuid.Must(uuid.NewV4()).String()
	fetchedTask, err := taskRegistry.GetTask("task_test.SendEmailTaskMetadata")
	require.NoError(t, err)
	message := message{
		Headers: map[string]string{"request_id": uuid.Must(uuid.NewV4()).String()},
		ID:      "message-id",
		Input:   &SendEmailTaskMetadataInput{},
		Metadata: &metadata{
			Priority:  PriorityHigh,
			Timestamp: JSONTime(time.Now()),
			Version:   CurrentVersion,
		},
		task:         newTaskDef(fetchedTask, taskRegistry),
		taskRegistry: taskRegistry,
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
