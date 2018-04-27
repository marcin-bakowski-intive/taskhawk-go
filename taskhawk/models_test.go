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
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONTime_ToJson(t *testing.T) {
	epochMS := 1521493587123
	ts := JSONTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	tsAsJSON, err := json.Marshal(ts)
	assert.NoError(t, err)
	epochStr := fmt.Sprintf("%d", epochMS)
	assert.Equal(t, epochStr, string(tsAsJSON))
}

func TestJSONTime_FromJson(t *testing.T) {
	epochMS := 1521493587123
	ts := JSONTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	epochStr := fmt.Sprintf("%d", epochMS)
	ts2 := new(JSONTime)
	assert.NoError(t, json.Unmarshal([]byte(epochStr), &ts2))
	assert.Equal(t, time.Time(ts).Unix(), time.Time(*ts2).Unix())
}

func TestJSONTime_FromJson_String(t *testing.T) {
	epochMS := 1524763993123
	epochStr := `"2018-04-26T17:33:13.123Z"`
	ts := JSONTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	ts2 := new(JSONTime)
	assert.NoError(t, json.Unmarshal([]byte(epochStr), &ts2))
	assert.Equal(t, time.Time(ts).Unix(), time.Time(*ts2).Unix())
}

func TestJSONTime_FromJson_InvalidString(t *testing.T) {
	epochStr := `"2016"`
	ts2 := new(JSONTime)
	assert.Error(t, json.Unmarshal([]byte(epochStr), &ts2))
}

func TestJSONTime_FromJson_InvalidValue(t *testing.T) {
	epochStr := ``
	ts2 := new(JSONTime)
	assert.Error(t, json.Unmarshal([]byte(epochStr), &ts2))
}

func getValidMessage(input interface{}) *message {
	epochMS := 1521493587123
	ts := JSONTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	return &message{
		Headers: map[string]string{"request_id": "request-id"},
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: ts,
			Version:   CurrentVersion,
		},
		Task: taskRegistry["task_test.SendEmailTask"],
	}
}

func TestMessageToJson(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	expected := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"Subject":"Hi there!","From":"mail@spammer.com"},"metadata":{"priority":"default","timestamp":1521493587123,` +
		`"version":"1.0"},"task":"task_test.SendEmailTask"}`
	actual, err := json.Marshal(message)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(actual))
}

func TestMessageToJsonMinimal(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTaskNoInput()
	require.NoError(t, RegisterTask(task))

	message := getValidMessage(nil)
	message.Headers = map[string]string{}
	message.Task = taskRegistry["task_test.SendEmailTaskNoInput"]
	expected := `{"headers":{},"id":"message-id","kwargs":null,"metadata":{"priority":"default",` +
		`"timestamp":1521493587123,"version":"1.0"},"task":"task_test.SendEmailTaskNoInput"}`
	actual, err := json.Marshal(message)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(actual))
}

func TestMessageFromJson(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	expected := getValidMessage(input)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","kwargs":{"To":"mail@example.com",` +
		`"Subject":"Hi there!","From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`

	actual := new(message)
	err := json.Unmarshal([]byte(jsonStr), actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestMessageFromJson_FailsOnUnknownTask(t *testing.T) {
	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","input":{"To":"mail@example.com",` +
		`"Subject":"Hi there!","From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"},` +
		`"task":"task_test.SendEmailTask"}`

	actual := new(message)
	err := json.Unmarshal([]byte(jsonStr), actual)
	assert.EqualError(t, err, "invalid task not found")
}

func TestMessageFromJson_NoFailIfNoTask(t *testing.T) {
	expected := getValidMessage(nil)

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","input":{"To":"mail@example.com",` +
		`"Subject":"Hi there!","From":"mail@spammer.com"},"metadata":{"timestamp":1521493587123,"version":"1.0"}}`

	actual := new(message)
	err := json.Unmarshal([]byte(jsonStr), actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestMessageFromJson_NilInput(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTaskNoInput()
	require.NoError(t, RegisterTask(task))

	epochMS := 1521493587123
	ts := JSONTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	expected := &message{
		Headers: map[string]string{"request_id": "request-id"},
		ID:      "message-id",
		Input:   nil,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: ts,
			Version:   CurrentVersion,
		},
		Task: taskRegistry[task.Name()],
	}

	jsonStr := `{"headers":{"request_id":"request-id"},"id":"message-id","input":null,
		"metadata":{"timestamp":1521493587123,"version":"1.0"},"task":"task_test.SendEmailTaskNoInput"}`

	actual := new(message)
	err := json.Unmarshal([]byte(jsonStr), actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestMessage_Validate(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	message := getValidMessage(nil)
	assert.NoError(t, message.validate())
}

func TestMessage_ValidateFail_NoId(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.ID = ""
	assert.EqualError(t, message.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoMetadata(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.Metadata = nil
	assert.EqualError(t, message.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoVersion(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.Metadata.Version = ""
	assert.EqualError(t, message.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoTimestamp(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.Metadata.Timestamp = JSONTime(time.Time{})
	assert.EqualError(t, message.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoHeaders(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.Headers = nil
	assert.EqualError(t, message.validate(), "missing required data")
}

func TestMessage_ValidateFail_NoTask(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.Task = nil
	assert.EqualError(t, message.validate(), "missing required data")
}

func TestMessage_ValidateFail_UnknownTask(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	message.Task = &taskDef{NewSendEmailTask()}
	assert.EqualError(t, message.validate(), "invalid task, not registered: task_test.SendEmailTask")
}

func TestMessage_CallTask(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	ctx := context.Background()

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	// copy object before anyone else modifies it
	expected := *input
	task.On("Run", ctx, &expected).Return(nil)

	message := getValidMessage(input)
	message.validate()
	receipt := uuid.Must(uuid.NewV4()).String()

	assert.NoError(t, message.callTask(ctx, receipt))

	task.AssertExpectations(t)
}

func copyMap(d map[string]string) map[string]string {
	newMap := make(map[string]string)
	for k, v := range d {
		newMap[k] = v
	}
	return newMap
}

func TestMessage_newMessage(t *testing.T) {
	defer CleanupTaskRegistry()

	task := NewSendEmailTask()
	require.NoError(t, RegisterTask(task))

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	// copy objects that are mutable, or passed by ref
	headers := copyMap(message.Headers)
	actual, err := newMessage(input, headers, message.ID, PriorityHigh, message.Task)
	assert.NoError(t, err)
	message.Metadata = &metadata{
		Priority:  PriorityHigh,
		Timestamp: actual.Metadata.Timestamp,
		Version:   CurrentVersion,
	}

	assert.Equal(t, message, actual)
}

func TestMessage_newMessage_Validates(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	message := getValidMessage(input)
	headers := copyMap(message.Headers)
	_, err := newMessage(input, headers, "", message.Metadata.Priority, message.Task)
	assert.EqualError(t, err, "missing required data")
}
