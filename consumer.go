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
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
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
	//
	// This function never returns by default. Possible shutdown methods:
	// 1. Cancel the context - returns immediately.
	// 2. Set a deadline on the context of less than 10 seconds - returns after processing current messages.
	// 3. Run for limited number of loops by setting LoopCount on the request - returns after running loop a finite
	// number of times
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
	HandleLambdaEvent(ctx context.Context, snsEvent *events.SNSEvent) error
}

const sqsWaitTimeoutSeconds int64 = 20

// ErrRetry indicates that task failed for a known reason and should be retried without logging falure
var ErrRetry = errors.New("Retry exception")

type lambdaConsumer struct {
	awsClient    iamazonWebServices
	settings     *Settings
	taskRegistry ITaskRegistry
}

func (c *lambdaConsumer) HandleLambdaEvent(ctx context.Context, snsEvent *events.SNSEvent) error {
	ctx = withSettings(ctx, c.settings)
	if !getIsLambdaApp(ctx) {
		return errors.New("Can't process lambda event in a SQS consumer")
	}
	return c.awsClient.HandleLambdaEvent(ctx, c.taskRegistry, snsEvent)
}

type queueConsumer struct {
	awsClient    iamazonWebServices
	settings     *Settings
	taskRegistry ITaskRegistry
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
			if deadline, ok := ctx.Deadline(); ok {
				// is shutting down?
				if deadline.Sub(time.Now()) < getShutdownTimeout(ctx) {
					return nil
				}
			}
			if err := c.awsClient.FetchAndProcessMessages(
				ctx, c.taskRegistry, request.Priority,
				request.NumMessages, request.VisibilityTimeoutS,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// LambdaHandler implements Lambda.Handler interface
type LambdaHandler struct {
	lambdaConsumer ILambdaConsumer
}

// Invoke is the function that is invoked when a Lambda executes
func (handler *LambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	snsEvent := &events.SNSEvent{}
	err := json.Unmarshal(payload, snsEvent)
	if err != nil {
		return nil, err
	}

	err = handler.lambdaConsumer.HandleLambdaEvent(ctx, snsEvent)
	if err != nil {
		return nil, err
	}
	return []byte(""), nil
}

// NewLambdaHandler returns a new lambda Handler that can be started like so:
//
//   func main() {
//       lambda.StartHandler(NewLambdaHandler(consumer))
//   }
//
// If you want to add additional error handle (e.g. panic catch etc), you can always use your own Handler,
// and call LambdaHandler.Invoke
func NewLambdaHandler(consumer ILambdaConsumer) lambda.Handler {
	return &LambdaHandler{
		lambdaConsumer: consumer,
	}
}
