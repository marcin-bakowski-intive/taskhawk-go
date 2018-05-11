/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

/*
Package taskhawk is a replacement for celery that works on AWS SQS/SNS, while keeping things pretty simple and
straightforward. Any unbound function can be converted into a TaskHawk task.

For inter-service messaging, see Hedwig: https://godoc.org/github.com/Automatic/hedwig-go/hedwig.

Provisioning

Taskhawk works on SQS and SNS as backing queues. Before you can publish tasks,
you need to provision the required infra. This may be done manually, or, preferably,
using Terraform. Taskhawk provides tools to make infra configuration easier: see
Taskhawk Terraform Generator (https://github.com/Automatic/taskhawk-terraform-generator) for further details.

Using Taskhawk

To use taskhawk, convert your function into a "Task" as shown here:

	type SendEmailTask struct {
		taskhawk.Task
	}

	func (t *SendEmailTask) Run(context.Context, interface{}) error {...}

Tasks may accept input of arbitrary type as long as it's serializable to JSON.

Then, define a few required settings:

	sessionCache := AWSSessionsCache{}

	settings := taskhawk.Settings{
		AWSAccessKey: <YOUR AWS ACCESS KEY>,
		AWSAccountID: <YOUR AWS ACCOUNT ID>,
		AWSRegion: <YOUR AWS REGION>,
		AWSSecretKey: <YOUR AWS SECRET KEY>,

		Queue: <YOUR TASKHAWK QUEUE>,
	}
	taskhawk.InitSettings(settings)

Before the task can be dispatched, it would need to be registered like so:

	func NewSendEmailTask() *SendEmailTask {
		return &SendEmailTask{
			Task: taskhawk.Task{
				TaskName:  "tasks.SendEmailTask",
				Inputer: func() interface{} {
					return &SendEmailTaskInput{}
				},
				Publisher: NewPublisher(sessionCache, settings),
			}
		}
	}

	taskhawk.RegisterTask(NewSendEmailTask())

And finally, dispatch your task asynchronously:

	NewSendEmailTask().dispatch(&SendEmailTaskInput{...})

To pass your context, use:

	NewSendEmailTask().dispatchWithContext(ctx, &SendEmailTaskInput{...})

If you want to include a custom headers with the message (for example,
you can include a request_id field for cross-application tracing),
you can set it on the input object (ITaskHeaders interface).

If you want to customize priority, you can do it like so:

	NewSendEmailTask().dispatchWithPriority(ctx, taskhawk.PriorityHigh, &SendEmailTaskInput{...})

Tasks are held in SQS queue until they're successfully executed, or until they fail a
configurable number of times. Failed tasks are moved to a Dead Letter Queue, where they're
held for 14 days, and may be examined for further debugging.

Priority

Taskhawk provides 4 priority queues to use, which may be customized per task, or per message.
For more details, see https://godoc.org/github.com/Automatic/taskhawk-go/taskhawk#Priority.

Metadata and Headers

If your input struct satisfies `taskhawk.ITaskMetadata` interface, it'll be filled in with the following attributes:

id: task identifier. This represents a run of a task.

priority: the priority this task message was dispatched with.

receipt: SQS receipt for the task. This may be used to extend message visibility if the task is running longer than
expected.

timestamp: task dispatch epoch timestamp (milli-seconds)

version: message format version.

If your input struct satisfies ITaskHeaders interface, it'll be filled with custom headers that the task
was dispatched with.

Helper structs that automatically satisfy these interfaces are available in taskhawk library that you may embed in your
input struct like so:

	type SendEmailTaskInput struct {
		taskhawk.TaskMetadata
		taskhawk.TaskHeaders
		...
	}

For a compile time type assertion check, you may add (in global scope):

	var _ taskhawk.ITaskMetadata = &SendEmailTaskInput{}
	var _ taskhawk.ITaskHeaders = &SendEmailTaskInput{}

This snippet won't consume memory or do anything at runtime.

Consumer

A consumer for SQS based workers can be started as following:

	consumer := taskhawk.NewQueueConsumer(sessionCache, settings)
	consumer.ListenForMessages(ctx, &taskhawk.ListenRequest{...})

This is a blocking function, so if you want to listen to multiple priority queues,
you'll need to run these on separate goroutines.

A consumer for Lambda based workers can be started as following:

	consumer := taskhawk.NewLambdaConsumer(sessionCache, settings)
	consumer.HandleLambdaEvent(ctx, snsEvent)

where snsEvent is the event provided by AWS to your Lambda
function as described in AWS documentation: https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-sns.



*/
package taskhawk
