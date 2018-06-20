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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type fakePublisherImpl struct {
	mock.Mock
	settings *Settings
}

func (p *fakePublisherImpl) Publish(ctx context.Context, message *message) error {
	args := p.Called(ctx, message)
	return args.Error(0)
}

func (p *fakePublisherImpl) Settings() *Settings {
	return p.settings
}

var fakePublisher = &fakePublisherImpl{
	settings: getQueueTestSettings(),
}

func resetFakePublisher() {
	fakePublisher.Mock = mock.Mock{}
	fakePublisher.settings = getQueueTestSettings()
}

func TestPublish(t *testing.T) {
	// I miss pytest :(
	allParams := []map[string]interface{}{
		{
			"settings":  getLambdaTestSettings(),
			"awsMethod": "PublishSNS",
		},
		{
			"settings":  getQueueTestSettings(),
			"awsMethod": "SendMessageSQS",
		},
	}

	for _, params := range allParams {
		settings := params["settings"].(*Settings)

		// Stub out AWS portion
		fakeClient := &FakeAWS{}
		publisherObj := publisher{
			settings:  settings,
			awsClient: fakeClient,
		}

		taskRegistry, err := NewTaskRegistry(&publisherObj)
		require.NoError(t, err)
		task := NewSendEmailTask()
		require.NoError(t, taskRegistry.RegisterTask(task))

		message := getValidMessage(t, taskRegistry, nil)
		message.Metadata.Priority = PriorityHigh

		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		ctx := withSettings(context.Background(), settings)

		fakeClient.On(params["awsMethod"].(string), ctx, message.Metadata.Priority, string(msgJSON),
			message.Headers).Return(nil)

		assert.NoError(t, publisherObj.Publish(ctx, message))

		fakeClient.AssertExpectations(t)
	}
}

func TestNewPublisher(t *testing.T) {
	settings := getQueueTestSettings()

	sessionCache := &AWSSessionsCache{}

	ipublisher := NewPublisher(sessionCache, settings)
	assert.NotNil(t, ipublisher)
}
