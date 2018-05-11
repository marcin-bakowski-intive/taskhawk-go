/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	// Priority represents the priority queue for fetching messages from. Defaults to PriorityDefault.
	Priority Priority

	// NumMessages represents the number of SQS messages to fetch in one API call. Defaults ot 1.
	NumMessages uint

	// VisibilityTimeoutS represents the default visibility timeout in seconds for a message.
	// This is the amount of time you have to process your tasks.
	// It defaults to whatever is set in the queue configuration.
	VisibilityTimeoutS uint

	// LoopCount is the number of loops to run for fetching messages.
	// This may be used to limit to only certain number of messages.
	// Defaults to running as an infinite loop until the context is cancelled.
	LoopCount uint
}

// IQueueConsumer represents a taskhawk consumer for SQS apps
type IQueueConsumer interface {
	// ListenForMessages starts a taskhawk listener for message types provided and calls the task like so:
	//
	// task_fn(input).
	//
	// If `input` implements `ITaskMetadata`, metadata will be filled in with the following things: id, version,
	// header, receipt.
	//
	// The message is explicitly deleted only if task function ran successfully. In case of an exception the message is
	// kept on queue and processed again. If the callback keeps failing, SQS dead letter queue mechanism kicks in and
	// the message is moved to the dead-letter queue.
	ListenForMessages(ctx context.Context, request *ListenRequest) error
}

// ILambdaConsumer represents a taskhawk consumer for lambda apps
type ILambdaConsumer interface {
	// HandleLambdaInput processes taskhawk messages for Lambda apps and calls the task like so:
	//
	// task_fn(input).
	//
	// If `input` implements `ITaskMetadata`, metadata will be filled in with the following things: id, version,
	// header, receipt.
	//
	// The error returned by the task is returned to the caller, which should be returned from the Lambda handler.
	HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error
}

const sqsWaitTimeoutSeconds int64 = 20

// ErrRetry indicates that task failed for a known reason and should be retried without logging falure
var ErrRetry = errors.New("Retry exception")

type lambdaConsumer struct {
	awsClient iamazonWebServices
	settings  *Settings
}

func (c *lambdaConsumer) HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error {
	ctx = withSettings(ctx, c.settings)
	if !getIsLambdaApp(ctx) {
		return errors.New("Can't process lambda event in a SQS consumer")
	}
	return c.awsClient.HandleLambdaEvent(ctx, snsEvent)
}

type queueConsumer struct {
	awsClient iamazonWebServices
	settings  *Settings
}

func (c *queueConsumer) ListenForMessages(ctx context.Context, request *ListenRequest) error {
	ctx = withSettings(ctx, c.settings)

	if getIsLambdaApp(ctx) {
		return errors.New("Can't listen for messages in a Lambda consumer")
	}

	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	for i := uint(0); request.LoopCount == 0 || i < request.LoopCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := c.awsClient.FetchAndProcessMessages(
				ctx, request.Priority,
				request.NumMessages, request.VisibilityTimeoutS,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// NewQueueConsumer creates a new taskhawk consumer for queue apps
//
// Cancelable context may be used to cancel processing of messages
func NewQueueConsumer(sessionCache *AWSSessionsCache, settings *Settings) IQueueConsumer {
	return &queueConsumer{
		awsClient: newAmazonWebServices(withSettings(context.Background(), settings), sessionCache),
		settings:  settings,
	}
}

// NewLambdaConsumer creates a new taskhawk consumer for lambda apps
//
// Cancelable context may be used to cancel processing of messages
func NewLambdaConsumer(sessionCache *AWSSessionsCache, settings *Settings) ILambdaConsumer {
	return &lambdaConsumer{
		awsClient: newAmazonWebServices(withSettings(context.Background(), settings), sessionCache),
		settings:  settings,
	}
}
