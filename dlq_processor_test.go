package taskhawk

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDLQProcessor_Requeue(t *testing.T) {
	ctx := context.Background()
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		Queue:        "dev-myapp",
	}
	awsClient := &FakeAWS{}
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(10)
	awsClient.On("RequeueDLQMessages", withSettings(ctx, settings), PriorityDefault, numMessages, visibilityTimeoutS).Return(nil)
	dlqProcessor := &DLQProcessor{
		awsClient: awsClient,
		settings:  settings,
	}
	requeueRequest := RequeueRequest{
		Priority:           PriorityDefault,
		NumMessages:        numMessages,
		VisibilityTimeoutS: visibilityTimeoutS,
	}
	err := dlqProcessor.Requeue(ctx, &requeueRequest)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
	assert.Equal(t, len(awsClient.Calls), 1)
}

func TestDLQProcessor_Requeue_invalid_NumMessages(t *testing.T) {
	ctx := context.Background()
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		Queue:        "dev-myapp",
	}
	awsClient := &FakeAWS{}
	dlqProcessor := &DLQProcessor{
		awsClient: awsClient,
		settings:  settings,
	}
	requeueRequest := RequeueRequest{
		Priority:           PriorityDefault,
		NumMessages:        100,
		VisibilityTimeoutS: 10,
	}
	err := dlqProcessor.Requeue(ctx, &requeueRequest)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("request.NumMessages=%d is not valid. Allowed values: <1-10>", requeueRequest.NumMessages))
	awsClient.AssertExpectations(t)
	assert.Equal(t, len(awsClient.Calls), 0)
}

func TestNewDQLProcessor(t *testing.T) {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		Queue:        "dev-myapp",
	}

	sessionCache := &AWSSessionsCache{}

	iprocessor := NewDQLProcessor(sessionCache, settings)
	assert.NotNil(t, iprocessor)
}
