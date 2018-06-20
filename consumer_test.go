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
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConsumer_ListenForMessages(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := context.Background()
	ctxWithSettings := withSettings(context.Background(), settings)

	awsClient := &FakeAWS{}
	numMessages := uint(10)
	visibilityTimeoutS := uint(10)
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)
	awsClient.On("FetchAndProcessMessages", ctxWithSettings, taskRegistry, PriorityHigh, numMessages,
		visibilityTimeoutS).Return(nil)
	consumer := queueConsumer{
		awsClient:    awsClient,
		settings:     settings,
		taskRegistry: taskRegistry,
	}

	err = consumer.ListenForMessages(ctx, &ListenRequest{
		Priority:           PriorityHigh,
		NumMessages:        numMessages,
		VisibilityTimeoutS: visibilityTimeoutS,
		LoopCount:          1,
	})
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
}

func TestConsumer_ListenForMessagesContextCancel(t *testing.T) {
	settings := getQueueTestSettings()
	ctx, cancel := context.WithCancel(context.Background())
	ctxWithSettings := withSettings(ctx, settings)

	awsClient := &FakeAWS{}
	numMessages := uint(10)
	visibilityTimeoutS := uint(10)
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)
	awsClient.On("FetchAndProcessMessages", ctxWithSettings, taskRegistry, PriorityHigh, numMessages,
		visibilityTimeoutS).Return(nil).After(500 * time.Millisecond)
	consumer := queueConsumer{
		awsClient:    awsClient,
		settings:     settings,
		taskRegistry: taskRegistry,
	}
	ch := make(chan bool)
	go func() {
		err := consumer.ListenForMessages(ctx, &ListenRequest{
			Priority:           PriorityHigh,
			NumMessages:        numMessages,
			VisibilityTimeoutS: visibilityTimeoutS,
			LoopCount:          1000,
		})
		assert.EqualError(t, err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(1 * time.Millisecond)
	cancel()
	// wait for co-routine to finish
	<-ch
	awsClient.AssertExpectations(t)
	assert.True(t, len(awsClient.Calls) < 1000)
}

func TestConsumer_ListenForMessagesContextDeadline(t *testing.T) {
	settings := getQueueTestSettings()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(11*time.Second))
	ctxWithSettings := withSettings(ctx, settings)

	awsClient := &FakeAWS{}
	numMessages := uint(10)
	visibilityTimeoutS := uint(10)
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)
	awsClient.On("FetchAndProcessMessages", ctxWithSettings, taskRegistry, PriorityHigh, numMessages,
		visibilityTimeoutS).Return(nil).After(500 * time.Millisecond)
	consumer := queueConsumer{
		awsClient:    awsClient,
		settings:     settings,
		taskRegistry: taskRegistry,
	}
	ch := make(chan bool)
	go func() {
		err := consumer.ListenForMessages(ctx, &ListenRequest{
			Priority:           PriorityHigh,
			NumMessages:        numMessages,
			VisibilityTimeoutS: visibilityTimeoutS,
			LoopCount:          1000,
		})
		assert.NoError(t, err)
		ch <- true
		close(ch)
	}()
	time.Sleep(1 * time.Millisecond)
	defer cancel()
	// wait for co-routine to finish
	<-ch
	awsClient.AssertExpectations(t)
	assert.True(t, len(awsClient.Calls) < 1000)
}

func TestConsumer_ListenForMessagesFailLambda(t *testing.T) {
	settings := getLambdaTestSettings()
	ctx := withSettings(context.Background(), settings)
	awsClient := &FakeAWS{}
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)
	consumer := queueConsumer{
		awsClient:    awsClient,
		settings:     settings,
		taskRegistry: taskRegistry,
	}
	err = consumer.ListenForMessages(ctx, &ListenRequest{
		Priority: PriorityHigh,
	})
	assert.EqualError(t, err, "Can't listen for messages in a Lambda consumer")
}

func TestConsumer_HandleLambdaEvent(t *testing.T) {
	settings := getLambdaTestSettings()
	ctx := context.Background()
	ctxWithSettings := withSettings(context.Background(), settings)

	snsEvent := &events.SNSEvent{
		Records: []events.SNSEventRecord{
			{
				SNS: events.SNSEntity{
					MessageID: uuid.Must(uuid.NewV4()).String(),
					Message:   "message",
				},
			},
		},
	}
	awsClient := &FakeAWS{}
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)
	awsClient.On("HandleLambdaEvent", ctxWithSettings, taskRegistry, snsEvent).Return(nil)
	consumer := lambdaConsumer{
		awsClient:    awsClient,
		settings:     settings,
		taskRegistry: taskRegistry,
	}
	err = consumer.HandleLambdaEvent(ctx, snsEvent)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
}

func TestConsumer_HandleLambdaEventFailSQS(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	awsClient := &FakeAWS{}
	publisher := &publisher{
		awsClient: awsClient,
		settings:  settings,
	}
	taskRegistry, err := NewTaskRegistry(publisher)
	require.NoError(t, err)
	consumer := lambdaConsumer{
		awsClient:    awsClient,
		settings:     settings,
		taskRegistry: taskRegistry,
	}
	err = consumer.HandleLambdaEvent(ctx, &events.SNSEvent{})
	assert.EqualError(t, err, "Can't process lambda event in a SQS consumer")
}

type fakeLambdaConsumer struct {
	mock.Mock
	ILambdaConsumer
}

func (lambdaConsumer fakeLambdaConsumer) HandleLambdaEvent(ctx context.Context, snsEvent *events.SNSEvent) error {
	args := lambdaConsumer.Called(ctx, snsEvent)
	return args.Error(0)
}

func TestLambdaHandler_Invoke(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := LambdaHandler{
		lambdaConsumer: lambdaConsumer,
	}
	ctx := context.Background()
	snsEvent := &events.SNSEvent{}
	payload, err := json.Marshal(snsEvent)
	require.NoError(t, err)

	lambdaConsumer.On("HandleLambdaEvent", ctx, snsEvent).Return(nil)

	response, err := handler.Invoke(ctx, payload)
	assert.NoError(t, err)
	assert.Equal(t, []byte(""), response)

	lambdaConsumer.AssertExpectations(t)
}

func TestLambdaHandler_InvokeFailUnmarshal(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := LambdaHandler{
		lambdaConsumer: lambdaConsumer,
	}
	ctx := context.Background()

	response, err := handler.Invoke(ctx, []byte("bad payload"))
	assert.EqualError(t, err, "invalid character 'b' looking for beginning of value")
	assert.Nil(t, response)
}

func TestLambdaHandler_InvokeFailHandler(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := LambdaHandler{
		lambdaConsumer: lambdaConsumer,
	}
	ctx := context.Background()
	snsEvent := &events.SNSEvent{}
	payload, err := json.Marshal(snsEvent)
	require.NoError(t, err)

	lambdaConsumer.On("HandleLambdaEvent", ctx, snsEvent).Return(errors.New("oops"))

	response, err := handler.Invoke(ctx, payload)
	assert.EqualError(t, err, "oops")
	assert.Nil(t, response)

	lambdaConsumer.AssertExpectations(t)
}

func TestNewLambdaHandler(t *testing.T) {
	lambdaConsumer := &fakeLambdaConsumer{}
	handler := NewLambdaHandler(lambdaConsumer)
	assert.NotNil(t, handler)
}
