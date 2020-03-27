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
	"io/ioutil"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetSqsQueue(t *testing.T) {
	settings := getQueueTestSettings()

	expectedQueue := "TASKHAWK-DEV-MYAPP"
	queue := getSQSQueue(withSettings(context.Background(), settings), PriorityDefault)
	assert.Equal(t, expectedQueue, queue)
}

type FakeSQS struct {
	mock.Mock
	// fake interface here
	sqsiface.SQSAPI
}

func (fs *FakeSQS) SendMessageWithContext(ctx aws.Context, in *sqs.SendMessageInput,
	options ...request.Option) (*sqs.SendMessageOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

func (fs *FakeSQS) GetQueueUrlWithContext(ctx aws.Context, in *sqs.GetQueueUrlInput,
	options ...request.Option) (*sqs.GetQueueUrlOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sqs.GetQueueUrlOutput), args.Error(1)
}

func (fs *FakeSQS) GetQueueAttributesWithContext(ctx aws.Context, in *sqs.GetQueueAttributesInput, opts ...request.Option) (*sqs.GetQueueAttributesOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.GetQueueAttributesOutput), args.Error(1)
}

func (fs *FakeSQS) ReceiveMessageWithContext(ctx aws.Context, in *sqs.ReceiveMessageInput,
	options ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (fs *FakeSQS) DeleteMessageWithContext(ctx aws.Context, in *sqs.DeleteMessageInput,
	options ...request.Option) (
	*sqs.DeleteMessageOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

type FakeSns struct {
	mock.Mock
	// fake interface here
	snsiface.SNSAPI
}

func (fs *FakeSns) PublishWithContext(ctx aws.Context, in *sns.PublishInput, options ...request.Option) (
	*sns.PublishOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sns.PublishOutput), args.Error(1)
}

type FakeAWS struct {
	mock.Mock
}

func (fa *FakeAWS) PublishSNS(ctx context.Context, priority Priority, payload string,
	headers map[string]string) error {

	args := fa.Called(ctx, priority, payload, headers)
	return args.Error(0)
}

func (fa *FakeAWS) SendMessageSQS(ctx context.Context, priority Priority, payload string,
	headers map[string]string) error {

	args := fa.Called(ctx, priority, payload, headers)
	return args.Error(0)
}

func (fa *FakeAWS) FetchAndProcessMessages(ctx context.Context, taskRegistry ITaskRegistry,
	priority Priority, numMessages uint, visibilityTimeoutS uint) error {

	args := fa.Called(ctx, taskRegistry, priority, numMessages, visibilityTimeoutS)
	return args.Error(0)
}

func (fa *FakeAWS) HandleLambdaEvent(ctx context.Context, taskRegistry ITaskRegistry, snsEvent *events.SNSEvent) error {
	args := fa.Called(ctx, taskRegistry, snsEvent)
	return args.Error(0)
}

func (fa *FakeAWS) RequeueDLQMessages(ctx context.Context, priority Priority, numMessages uint32, visibilityTimeoutS uint32) error {
	args := fa.Called(ctx, priority, numMessages, visibilityTimeoutS)
	return args.Error(0)
}

func TestAmazonWebServices_EnsureQueueUrl(t *testing.T) {
	settings := getQueueTestSettings()
	priority := PriorityHigh
	queueName := getSQSQueue(withSettings(context.Background(), settings), priority)
	expectedQueueURL := "https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName

	fakeSqs := &FakeSQS{}
	awsClient := &amazonWebServices{
		sqs: fakeSqs,
	}

	expectedInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &expectedQueueURL,
	}

	ctx := withSettings(context.Background(), settings)

	fakeSqs.On("GetQueueUrlWithContext", ctx, expectedInput).Return(output, nil)

	queueURL, err := awsClient.ensureQueueURL(ctx, priority)
	assert.NoError(t, err)
	assert.Equal(t, expectedQueueURL, *queueURL)
	fakeSqs.AssertExpectations(t)

	// another call shouldn't call API
	queueURL, err = awsClient.ensureQueueURL(ctx, priority)
	assert.NoError(t, err)
	assert.Equal(t, expectedQueueURL, *queueURL)
	fakeSqs.AssertExpectations(t)
}

func TestGetSnsTopic(t *testing.T) {
	settings := getQueueTestSettings()

	expectedTopic := "arn:aws:sns:us-east-1:1234567890:taskhawk-dev-myapp"
	topic := getSNSTopic(withSettings(context.Background(), settings), PriorityDefault)
	assert.Equal(t, expectedTopic, topic)
}

func TestAmazonWebServices_PublishSNS(t *testing.T) {
	fakeSns := &FakeSns{}
	fakeAWS := &amazonWebServices{
		sns: fakeSns,
	}

	settings := getLambdaTestSettings()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))
	message := getValidMessage(t, taskRegistry, nil)

	msgJSON, err := json.Marshal(message)
	require.NoError(t, err)

	expectedTopic := "arn:aws:sns:us-east-1:1234567890:taskhawk-dev-myapp-high-priority"

	attributes := make(map[string]*sns.MessageAttributeValue)
	for key, value := range message.Headers {
		attributes[key] = &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: &value,
		}
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(msgJSON)),
		MessageAttributes: attributes,
	}

	ctx := withSettings(context.Background(), settings)

	fakeSns.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return((*sns.PublishOutput)(nil), nil)

	err = fakeAWS.PublishSNS(ctx, PriorityHigh, string(msgJSON), message.Headers)
	assert.NoError(t, err)

	fakeSns.AssertExpectations(t)
}

func TestAmazonWebServices_SendMessageSQS(t *testing.T) {
	fakeSqs := &FakeSQS{}
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueURL := "https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName
	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}

	settings := getQueueTestSettings()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))
	message := getValidMessage(t, taskRegistry, nil)

	msgJSON, err := json.Marshal(message)
	require.NoError(t, err)

	attributes := make(map[string]*sqs.MessageAttributeValue)
	for key, value := range message.Headers {
		attributes[key] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: &value,
		}
	}

	expectedSendMessageInput := &sqs.SendMessageInput{
		QueueUrl:          &queueURL,
		MessageBody:       aws.String(string(msgJSON)),
		MessageAttributes: attributes,
	}

	ctx := withSettings(context.Background(), settings)

	fakeSqs.On("SendMessageWithContext", ctx, expectedSendMessageInput).Return(
		(*sqs.SendMessageOutput)(nil), nil)

	err = awsClient.SendMessageSQS(ctx, PriorityHigh, string(msgJSON), message.Headers)
	assert.NoError(t, err)

	fakeSqs.AssertExpectations(t)
}

func TestAmazonWebServices_messageHandlerTaskNotRegistered(t *testing.T) {
	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)

	epochMS := 1521493587123
	ts := JSONTime(time.Unix(int64(epochMS/1000), int64((epochMS%1000)*1000000)))
	message := &message{
		Headers: map[string]string{"request_id": "request-id"},
		ID:      "message-id",
		Input:   input,
		Metadata: &metadata{
			Priority:  PriorityDefault,
			Timestamp: ts,
			Version:   CurrentVersion,
		},
		taskRegistry: taskRegistry,
	}

	awsClient := amazonWebServices{}
	receipt := uuid.NewV4().String()
	message.ID = ""
	messageJSON, err := json.Marshal(message)
	require.NoError(t, err)
	err = awsClient.messageHandler(context.Background(), taskRegistry, string(messageJSON), receipt)
	assert.EqualError(t, err, "unable to unmarshal message: invalid task, not registered: ")
}

func TestAmazonWebServices_messageHandlerFailsOnValidationFailure(t *testing.T) {
	awsClient := amazonWebServices{}
	receipt := uuid.NewV4().String()

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))
	message := getValidMessage(t, taskRegistry, nil)
	message.ID = ""
	messageJSON, err := json.Marshal(message)
	require.NoError(t, err)
	err = awsClient.messageHandler(context.Background(), taskRegistry, string(messageJSON), receipt)
	assert.EqualError(t, err, "missing required data")
}

func TestAmazonWebServices_messageHandlerFailsOnTaskFailure(t *testing.T) {
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	ctx := context.Background()

	task.On("Run", ctx, &SendEmailTaskInput{}).Return(errors.New("oops"))

	message := getValidMessage(t, taskRegistry, nil)
	awsClient := amazonWebServices{}
	receipt := uuid.NewV4().String()
	messageJSON, err := json.Marshal(message)
	require.NoError(t, err)
	err = awsClient.messageHandler(ctx, taskRegistry, string(messageJSON), receipt)
	assert.EqualError(t, err, "oops")

	task.AssertExpectations(t)
}

func TestAmazonWebServices_messageHandlerFailsOnBadJSON(t *testing.T) {
	awsClient := amazonWebServices{}
	receipt := uuid.NewV4().String()
	messageJSON := "bad json-"
	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	err = awsClient.messageHandler(context.Background(), taskRegistry, string(messageJSON), receipt)
	assert.NotNil(t, err)
}

func TestAmazonWebServices_FetchAndProcessMessages(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	fakeSqs := &FakeSQS{}
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueURL := "https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	outMessages := make([]*sqs.Message, 2)
	for i := 0; i < 2; i++ {
		input := &SendEmailTaskInput{
			To:      fmt.Sprintf("mail%d@example.com", i),
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		}
		// copy object before anyone else modifies it
		expected := *input
		task.On("Run", ctx, &expected).Return(nil)

		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		outMessages[i] = &sqs.Message{
			Body:          aws.String(string(msgJSON)),
			ReceiptHandle: aws.String(uuid.NewV4().String()),
		}

		expectedDeleteMessageInput := &sqs.DeleteMessageInput{
			QueueUrl:      &queueURL,
			ReceiptHandle: outMessages[i].ReceiptHandle,
		}
		fakeSqs.On("DeleteMessageWithContext", ctx, expectedDeleteMessageInput).Return(
			&sqs.DeleteMessageOutput{}, nil)
	}

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: outMessages,
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput).Return(
		receiveMessageOutput, nil)

	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}
	err = awsClient.FetchAndProcessMessages(
		ctx, taskRegistry, PriorityHigh, 10, 10,
	)
	assert.NoError(t, err)
	task.AssertExpectations(t)
}

func TestAmazonWebServices_FetchAndProcessMessagesNoDeleteOnError(t *testing.T) {
	hook := test.NewGlobal()
	logrus.StandardLogger().Out = ioutil.Discard
	defer hook.Reset()

	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	fakeSqs := &FakeSQS{}
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueURL := "https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	input := &SendEmailTaskInput{
		To:      "mail@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	// copy object before anyone else modifies it
	expected := *input
	task.On("Run", ctx, &expected).Return(errors.New("my bad"))

	message := getValidMessage(t, taskRegistry, input)
	msgJSON, err := json.Marshal(message)
	require.NoError(t, err)

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Body:          aws.String(string(msgJSON)),
				ReceiptHandle: aws.String(uuid.NewV4().String()),
			},
		},
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput).Return(
		receiveMessageOutput, nil)

	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}
	err = awsClient.FetchAndProcessMessages(
		ctx, taskRegistry, PriorityHigh, 10, 10,
	)
	// no error is returned here, but we log the error
	assert.NoError(t, err)
	task.AssertExpectations(t)

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Retrying due to unknown exception: my bad", hook.LastEntry().Message)
}

type preProcessHook struct {
	mock.Mock
}

func (p *preProcessHook) PreProcessHookQueueApp(request *QueueRequest) error {
	args := p.Called(request)
	return args.Error(0)
}

func (p *preProcessHook) PreProcessHookLambdaApp(request *LambdaRequest) error {
	args := p.Called(request)
	return args.Error(0)
}

func TestAmazonWebServices_PreprocessHookQueueApp(t *testing.T) {
	preProcessHook := &preProcessHook{}

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",

		PreProcessHookQueueApp: preProcessHook.PreProcessHookQueueApp,
	}
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	fakeSqs := &FakeSQS{}
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueURL := "https://sqs.us-east-1.amazonaws.com/1234567890/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	outMessages := make([]*sqs.Message, 2)
	for i := 0; i < 2; i++ {
		input := &SendEmailTaskInput{
			To:      fmt.Sprintf("mail%d@example.com", i),
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		}
		// copy object before anyone else modifies it
		expected := *input
		task.On("Run", ctx, &expected).Return(nil)

		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		outMessages[i] = &sqs.Message{
			Body:          aws.String(string(msgJSON)),
			ReceiptHandle: aws.String(uuid.NewV4().String()),
		}

		expectedDeleteMessageInput := &sqs.DeleteMessageInput{
			QueueUrl:      &queueURL,
			ReceiptHandle: outMessages[i].ReceiptHandle,
		}
		fakeSqs.On("DeleteMessageWithContext", ctx, expectedDeleteMessageInput).Return(
			&sqs.DeleteMessageOutput{}, nil)

		queueRequest := &QueueRequest{
			Ctx:          ctx,
			QueueURL:     queueURL,
			QueueName:    queueName,
			QueueMessage: outMessages[i],
			TaskRegistry: taskRegistry,
		}
		preProcessHook.On("PreProcessHookQueueApp", queueRequest).Return(nil)
	}

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: outMessages,
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput).Return(
		receiveMessageOutput, nil)

	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}
	err = awsClient.FetchAndProcessMessages(
		ctx, taskRegistry, PriorityHigh, 10, 10,
	)
	assert.NoError(t, err)
	task.AssertExpectations(t)

	preProcessHook.AssertExpectations(t)
}

func TestAmazonWebServices_PreprocessHookQueueApp_Error(t *testing.T) {
	preProcessHook := &preProcessHook{}

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",

		PreProcessHookQueueApp: preProcessHook.PreProcessHookQueueApp,
	}
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	fakeSqs := &FakeSQS{}
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueUrl := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	expectedReceiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}

	outMessages := make([]*sqs.Message, 2)
	for i := 0; i < 2; i++ {
		input := &SendEmailTaskInput{
			To:      fmt.Sprintf("mail%d@example.com", i),
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		}

		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		outMessages[i] = &sqs.Message{
			Body:          aws.String(string(msgJSON)),
			ReceiptHandle: aws.String(uuid.NewV4().String()),
		}

		expectedDeleteMessageInput := &sqs.DeleteMessageInput{
			QueueUrl:      &queueUrl,
			ReceiptHandle: outMessages[i].ReceiptHandle,
		}
		fakeSqs.On("DeleteMessageWithContext", ctx, expectedDeleteMessageInput).Return(
			&sqs.DeleteMessageOutput{}, nil)

		queueRequest := &QueueRequest{
			Ctx:          ctx,
			QueueURL:     queueUrl,
			QueueName:    queueName,
			QueueMessage: outMessages[i],
			TaskRegistry: taskRegistry,
		}
		preProcessHook.On("PreProcessHookQueueApp", queueRequest).Return(errors.New("oops"))
	}

	receiveMessageOutput := &sqs.ReceiveMessageOutput{
		Messages: outMessages,
	}
	fakeSqs.On("ReceiveMessageWithContext", ctx, expectedReceiveMessageInput).Return(
		receiveMessageOutput, nil)

	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueUrl,
		},
	}
	err = awsClient.FetchAndProcessMessages(
		ctx, taskRegistry, PriorityHigh, 10, 10,
	)
	// the error is NOT bubbled up
	assert.NoError(t, err)
	task.AssertExpectations(t)

	preProcessHook.AssertExpectations(t)
}

func TestAmazonWebServices_PreprocessHookLambdaApp(t *testing.T) {
	preProcessHook := &preProcessHook{}

	awsClient := &amazonWebServices{}

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		IsLambdaApp:  true,

		PreProcessHookLambdaApp: preProcessHook.PreProcessHookLambdaApp,
	}
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	snsRecords := make([]events.SNSEventRecord, 2)

	for i := 0; i < 2; i++ {
		input := &SendEmailTaskInput{
			To:      fmt.Sprintf("mail%d@example.com", i),
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		}
		// copy object before anyone else modifies it
		expected := *input
		task.On("Run", mock.Anything, &expected).Return(nil)

		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.NewV4().String(),
				Message:   string(msgJSON),
			},
		}

		lambdaRequest := &LambdaRequest{
			Record:       &snsRecords[i],
			TaskRegistry: taskRegistry,
		}

		requestMatcher := func(request2 *LambdaRequest) bool {
			return lambdaRequest.Record == request2.Record
		}

		preProcessHook.
			On("PreProcessHookLambdaApp", mock.MatchedBy(requestMatcher)).
			Return(nil)
	}
	snsEvent := &events.SNSEvent{
		Records: snsRecords,
	}

	err = awsClient.HandleLambdaEvent(ctx, taskRegistry, snsEvent)
	assert.NoError(t, err)

	task.AssertExpectations(t)

	preProcessHook.AssertExpectations(t)
}

func TestAmazonWebServices_PreprocessHookLambdaApp_Error(t *testing.T) {
	preProcessHook := &preProcessHook{}

	awsClient := &amazonWebServices{}

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		IsLambdaApp:  true,

		PreProcessHookLambdaApp: preProcessHook.PreProcessHookLambdaApp,
	}
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	snsRecords := make([]events.SNSEventRecord, 2)

	for i := 0; i < 2; i++ {
		input := &SendEmailTaskInput{
			To:      fmt.Sprintf("mail%d@example.com", i),
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		}

		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.NewV4().String(),
				Message:   string(msgJSON),
			},
		}

		lambdaRequest := &LambdaRequest{
			Record:       &snsRecords[i],
			TaskRegistry: taskRegistry,
		}

		requestMatcher := func(request2 *LambdaRequest) bool {
			return lambdaRequest.Record == request2.Record
		}

		preProcessHook.
			On("PreProcessHookLambdaApp", mock.MatchedBy(requestMatcher)).
			Return(errors.New("oops"))
	}
	snsEvent := &events.SNSEvent{
		Records: snsRecords,
	}

	err = awsClient.HandleLambdaEvent(ctx, taskRegistry, snsEvent)
	assert.EqualError(t, errors.Cause(err), "oops")

	task.AssertExpectations(t)

	preProcessHook.AssertExpectations(t)
}

func TestAmazonWebServices_HandleLambdaEvent(t *testing.T) {
	awsClient := &amazonWebServices{}

	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		IsLambdaApp:  true,
	}
	InitSettings(settings)
	ctx := withSettings(context.Background(), settings)

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	snsRecords := make([]events.SNSEventRecord, 2)

	for i := 0; i < 2; i++ {
		input := &SendEmailTaskInput{
			To:      fmt.Sprintf("mail%d@example.com", i),
			From:    "mail@spammer.com",
			Subject: "Hi there!",
		}
		// copy object before anyone else modifies it
		expected := *input
		task.On("Run", mock.Anything, &expected).Return(nil)

		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)

		snsRecords[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.NewV4().String(),
				Message:   string(msgJSON),
			},
		}
	}
	snsEvent := &events.SNSEvent{
		Records: snsRecords,
	}

	err = awsClient.HandleLambdaEvent(ctx, taskRegistry, snsEvent)
	assert.NoError(t, err)

	task.AssertExpectations(t)
}

func TestAmazonWebServices_HandleLambdaEventForwardTaskError(t *testing.T) {
	hook := test.NewGlobal()
	logrus.StandardLogger().Out = ioutil.Discard
	defer hook.Reset()

	awsClient := &amazonWebServices{}

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	settings := taskRegistry.publisher.Settings()
	ctx := withSettings(context.Background(), settings)

	input := &SendEmailTaskInput{
		To:      "mail%d@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	// copy object before anyone else modifies it
	expected := *input
	task.On("Run", mock.Anything, &expected).Return(errors.New("oops"))

	message := getValidMessage(t, taskRegistry, input)
	msgJSON, err := json.Marshal(message)
	require.NoError(t, err)

	snsEvent := &events.SNSEvent{
		Records: []events.SNSEventRecord{
			{
				SNS: events.SNSEntity{
					MessageID: uuid.NewV4().String(),
					Message:   string(msgJSON),
				},
			},
		},
	}

	err = awsClient.HandleLambdaEvent(ctx, taskRegistry, snsEvent)
	assert.EqualError(t, err, "oops")

	task.AssertExpectations(t)

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "failed to process lambda event with error: oops", hook.LastEntry().Message)
}

func TestAmazonWebServices_HandleLambdaEventContextCancel(t *testing.T) {
	settings := getLambdaTestSettings()
	ctx, cancel := context.WithCancel(context.Background())
	ctxWithSettings := withSettings(ctx, settings)

	awsClient := &amazonWebServices{}

	taskRegistry, err := NewTaskRegistry(fakePublisher)
	require.NoError(t, err)
	task := NewSendEmailTask()
	require.NoError(t, taskRegistry.RegisterTask(task))

	input := &SendEmailTaskInput{
		To:      "mail%d@example.com",
		From:    "mail@spammer.com",
		Subject: "Hi there!",
	}
	// copy object before anyone else modifies it
	expected := *input
	task.On("Run", mock.Anything, &expected).Return(nil)

	records := make([]events.SNSEventRecord, 1000)
	for i := range records {
		message := getValidMessage(t, taskRegistry, input)
		msgJSON, err := json.Marshal(message)
		require.NoError(t, err)
		records[i] = events.SNSEventRecord{
			SNS: events.SNSEntity{
				MessageID: uuid.NewV4().String(),
				Message:   string(msgJSON),
			},
		}
	}
	snsEvent := &events.SNSEvent{
		Records: records,
	}

	ch := make(chan bool)
	go func() {
		err := awsClient.HandleLambdaEvent(ctxWithSettings, taskRegistry, snsEvent)
		assert.EqualError(t, err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(1 * time.Millisecond)
	cancel()
	// wait for co-routine to finish
	<-ch
	task.AssertExpectations(t)
	assert.True(t, len(task.Calls) < 1000)
}

func TestNewAmazonWebServices(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)

	sessionCache := &AWSSessionsCache{}

	iaws := newAmazonWebServices(ctx, sessionCache)
	assert.NotNil(t, iaws)
}

func TestAWSClient_getSQSQueueDlqURL(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	fakeSqs := &FakeSQS{}
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueDLQName := fmt.Sprintf("%s-DLQ", queueName)
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	expectedQueueDLQURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueDLQName
	attributeName := "RedrivePolicy"

	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}

	expectedInputQueueAttributes := sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []*string{&attributeName},
	}
	arn := fmt.Sprintf("arn:aws:sqs:us-east-1:686176732873:%s", queueDLQName)
	redrivePolicy := fmt.Sprintf("{\"DeadLetterTargetArn\": \"%s\", \"MaxReceiveCount\": 4}", arn)
	outputQueueAttributes := sqs.GetQueueAttributesOutput{
		Attributes: map[string]*string{"RedrivePolicy": &redrivePolicy},
	}
	expectedInputQueueUrl := sqs.GetQueueUrlInput{
		QueueName: &queueDLQName,
	}
	outputQueueUrl := sqs.GetQueueUrlOutput{
		QueueUrl: &expectedQueueDLQURL,
	}

	fakeSqs.On("GetQueueAttributesWithContext", ctx, &expectedInputQueueAttributes, mock.Anything).Return(&outputQueueAttributes, nil)
	fakeSqs.On("GetQueueUrlWithContext", ctx, &expectedInputQueueUrl, mock.Anything).Return(&outputQueueUrl, nil)

	queueDLQURL, err := awsClient.getSQSQueueDlqURL(ctx, queueURL)
	assert.NoError(t, err)
	assert.Equal(t, expectedQueueDLQURL, *queueDLQURL)
	fakeSqs.AssertExpectations(t)
}

func TestAWSClient_getSQSQueueDlqURL_errorNoPolicy(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	attributeName := "RedrivePolicy"

	fakeSqs := &FakeSQS{}
	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}

	expectedInputQueueAttributes := sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []*string{&attributeName},
	}
	outputQueueAttributes := sqs.GetQueueAttributesOutput{
		Attributes: map[string]*string{},
	}

	fakeSqs.On("GetQueueAttributesWithContext", ctx, &expectedInputQueueAttributes, mock.Anything).Return(&outputQueueAttributes, nil)

	_, err := awsClient.getSQSQueueDlqURL(ctx, queueURL)
	assert.NotNil(t, err)
	assert.EqualError(t, fmt.Errorf("%s", err), "RedrivePolicy attribute is null or empty string")
	fakeSqs.AssertExpectations(t)
}

func TestAWSClient_getSQSQueueDlqURL_invalidJsonPolicy(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	attributeName := "RedrivePolicy"

	fakeSqs := &FakeSQS{}
	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}

	redrivePolicy := "invalid"
	outputQueueAttributes := sqs.GetQueueAttributesOutput{
		Attributes: map[string]*string{"RedrivePolicy": &redrivePolicy},
	}
	expectedInputQueueAttributes := sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []*string{&attributeName},
	}

	fakeSqs.On("GetQueueAttributesWithContext", ctx, &expectedInputQueueAttributes, mock.Anything).Return(&outputQueueAttributes, nil)

	_, err := awsClient.getSQSQueueDlqURL(ctx, queueURL)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid RedrivePolicy, unable to unmarshal")
	fakeSqs.AssertExpectations(t)
}

func TestAWSClient_enqueueSQSMessage(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueDLQName := fmt.Sprintf("%s-DLQ", queueName)
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueDLQURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueDLQName
	messageId := "123"

	fakeSqs := &FakeSQS{}
	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}
	msgBody := "body"
	queueMessage := sqs.Message{Body: &msgBody, MessageId: &messageId}

	sendMessageInput := sqs.SendMessageInput{
		QueueUrl:          &queueURL,
		MessageBody:       queueMessage.Body,
		MessageAttributes: queueMessage.MessageAttributes,
	}
	sendMessageOutput := sqs.SendMessageOutput{MessageId: &messageId}
	deleteMessageInput := sqs.DeleteMessageInput{
		QueueUrl:      &queueDLQURL,
		ReceiptHandle: queueMessage.ReceiptHandle,
	}
	deleteMessageOutput := sqs.DeleteMessageOutput{}

	fakeSqs.On("SendMessageWithContext", ctx, &sendMessageInput, mock.Anything).Return(&sendMessageOutput, nil)
	fakeSqs.On("DeleteMessageWithContext", ctx, &deleteMessageInput, mock.Anything).Return(&deleteMessageOutput, nil)

	err := awsClient.enqueueSQSMessage(ctx, &queueMessage, &queueDLQURL, &queueURL)
	assert.NoError(t, err)
	fakeSqs.AssertExpectations(t)
}

func TestAWSClient_RequeueDLQMessages(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)
	queueName := "TASKHAWK-DEV-MYAPP-HIGH-PRIORITY"
	queueDLQName := fmt.Sprintf("%s-DLQ", queueName)
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueDLQURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueDLQName
	attributeName := "RedrivePolicy"
	messageId := "123"

	fakeSqs := &FakeSQS{}
	awsClient := &amazonWebServices{
		sqs: fakeSqs,
		// pre-filled cache
		queueUrls: map[Priority]*string{
			PriorityHigh: &queueURL,
		},
	}

	expectedInputQueueAttributes := sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []*string{&attributeName},
	}
	arn := fmt.Sprintf("arn:aws:sqs:us-east-1:686176732873:%s", queueDLQName)
	redrivePolicy := fmt.Sprintf("{\"DeadLetterTargetArn\": \"%s\", \"MaxReceiveCount\": 4}", arn)
	outputQueueAttributes := sqs.GetQueueAttributesOutput{
		Attributes: map[string]*string{"RedrivePolicy": &redrivePolicy},
	}
	expectedInputQueueDlqUrl := sqs.GetQueueUrlInput{
		QueueName: &queueDLQName,
	}
	outputQueueDlqUrl := sqs.GetQueueUrlOutput{
		QueueUrl: &queueDLQURL,
	}
	expectedReceiveMessageInput := sqs.ReceiveMessageInput{
		QueueUrl:            &queueDLQURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(sqsRequeueWaitTimeoutSeconds),
	}
	msgBody := "body"
	queueMessage := sqs.Message{Body: &msgBody, MessageId: &messageId}
	expectedReceiveMessageOutput := sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&queueMessage},
	}

	sendMessageInput := sqs.SendMessageInput{
		QueueUrl:          &queueURL,
		MessageBody:       queueMessage.Body,
		MessageAttributes: queueMessage.MessageAttributes,
	}
	sendMessageOutput := sqs.SendMessageOutput{MessageId: &messageId}
	deleteMessageInput := sqs.DeleteMessageInput{
		QueueUrl:      &queueDLQURL,
		ReceiptHandle: queueMessage.ReceiptHandle,
	}
	deleteMessageOutput := sqs.DeleteMessageOutput{}

	fakeSqs.On("GetQueueAttributesWithContext", ctx, &expectedInputQueueAttributes, mock.Anything).Return(&outputQueueAttributes, nil)
	fakeSqs.On("GetQueueUrlWithContext", ctx, &expectedInputQueueDlqUrl, mock.Anything).Return(&outputQueueDlqUrl, nil)
	fakeSqs.On("SendMessageWithContext", ctx, &sendMessageInput, mock.Anything).Return(&sendMessageOutput, nil)
	fakeSqs.On("DeleteMessageWithContext", ctx, &deleteMessageInput, mock.Anything).Return(&deleteMessageOutput, nil)
	fakeSqs.On("ReceiveMessageWithContext", ctx, &expectedReceiveMessageInput, mock.Anything).Times(1).Return(&expectedReceiveMessageOutput, nil)
	fakeSqs.On("ReceiveMessageWithContext", ctx, &expectedReceiveMessageInput, mock.Anything).Times(1).Return(&sqs.ReceiveMessageOutput{}, nil)

	err := awsClient.RequeueDLQMessages(ctx, PriorityHigh, 10, 10)
	assert.NoError(t, err)
	fakeSqs.AssertExpectations(t)
}
