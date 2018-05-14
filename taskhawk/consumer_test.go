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

	"github.com/aws/aws-lambda-go/events"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestConsumer_ListenForMessages(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := context.Background()
	ctxWithSettings := withSettings(context.Background(), settings)

	awsClient := &FakeAWS{}
	numMessages := uint(10)
	visibilityTimeoutS := uint(10)
	awsClient.On("FetchAndProcessMessages", ctxWithSettings, PriorityHigh, numMessages,
		visibilityTimeoutS).Return(nil)
	consumer := queueConsumer{
		awsClient: awsClient,
		settings:  settings,
	}
	err := consumer.ListenForMessages(ctx, &ListenRequest{
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
	awsClient.On("FetchAndProcessMessages", ctxWithSettings, PriorityHigh, numMessages,
		visibilityTimeoutS).Return(nil)
	consumer := queueConsumer{
		awsClient: awsClient,
		settings:  settings,
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

func TestConsumer_ListenForMessagesFailLambda(t *testing.T) {
	settings := getLambdaTestSettings()
	ctx := withSettings(context.Background(), settings)
	consumer := queueConsumer{
		awsClient: &FakeAWS{},
		settings:  settings,
	}
	err := consumer.ListenForMessages(ctx, &ListenRequest{
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
	awsClient.On("HandleLambdaEvent", ctxWithSettings, snsEvent).Return(nil)
	consumer := lambdaConsumer{
		awsClient: awsClient,
		settings:  settings,
	}
	err := consumer.HandleLambdaEvent(ctx, snsEvent)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
}

func TestConsumer_HandleLambdaEventFailSQS(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	consumer := lambdaConsumer{
		awsClient: &FakeAWS{},
		settings:  settings,
	}
	err := consumer.HandleLambdaEvent(ctx, &events.SNSEvent{})
	assert.EqualError(t, err, "Can't process lambda event in a SQS consumer")
}

func TestNewQueueConsumer(t *testing.T) {
	settings := getQueueTestSettings()

	sessionCache := &AWSSessionsCache{}

	iconsumer := NewQueueConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}

func TestNewLambdaConsumer(t *testing.T) {
	settings := getLambdaTestSettings()

	sessionCache := &AWSSessionsCache{}

	iconsumer := NewLambdaConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}
