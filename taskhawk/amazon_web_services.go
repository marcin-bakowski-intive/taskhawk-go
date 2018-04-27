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
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
)

type iamazonWebServices interface {
	PublishSNS(ctx context.Context, priority Priority, payload string, headers map[string]string) error
	SendMessageSQS(ctx context.Context, priority Priority, payload string,
		headers map[string]string) error
	FetchAndProcessMessages(ctx context.Context, priority Priority, numMessages uint,
		visibilityTimeoutS uint) error
	HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error
}

func getSQSQueue(ctx context.Context, priority Priority) string {
	queue := fmt.Sprintf("TASKHAWK-%s", strings.ToUpper(getQueue(ctx)))
	switch priority {
	case PriorityDefault:
	case PriorityHigh:
		queue += "-HIGH-PRIORITY"
	case PriorityLow:
		queue += "-LOW-PRIORITY"
	case PriorityBulk:
		queue += "-BULK"
	default:
		panic(fmt.Sprintf("unhandled priority %v", priority))
	}
	return queue
}

func getSNSTopic(ctx context.Context, priority Priority) string {
	topic := fmt.Sprintf(
		"arn:aws:sns:%s:%s:taskhawk-%s",
		getAWSRegion(ctx),
		getAWSAccountID(ctx),
		strings.ToLower(getQueue(ctx)))
	switch priority {
	case PriorityDefault:
	case PriorityHigh:
		topic += "-high-priority"
	case PriorityLow:
		topic += "-low-priority"
	case PriorityBulk:
		topic += "-bulk"
	default:
		panic(fmt.Sprintf("unhandled priority %v", priority))
	}
	return topic
}

// amazonWebServices wrapper struct for taskhawk
type amazonWebServices struct {
	sns       snsiface.SNSAPI
	sqs       sqsiface.SQSAPI
	queueUrls map[Priority]*string
}

// PublishSNS handles publishing to AWS SNS
func (a *amazonWebServices) PublishSNS(ctx context.Context, priority Priority, payload string,
	headers map[string]string) error {

	topic := getSNSTopic(ctx, priority)

	attributes := make(map[string]*sns.MessageAttributeValue)
	for key, value := range headers {
		attributes[key] = &sns.MessageAttributeValue{
			StringValue: &value,
			DataType:    aws.String("String"),
		}
	}

	_, err := a.sns.PublishWithContext(
		ctx,
		&sns.PublishInput{
			TopicArn:          &topic,
			Message:           &payload,
			MessageAttributes: attributes,
		},
	)
	return err
}

func (a *amazonWebServices) ensureQueueUrl(ctx context.Context, priority Priority) (*string, error) {
	if a.queueUrls == nil {
		a.queueUrls = make(map[Priority]*string)
	}
	queueUrl, ok := a.queueUrls[priority]
	if !ok {
		queueName := getSQSQueue(ctx, priority)
		out, err := a.sqs.GetQueueUrlWithContext(
			ctx,
			&sqs.GetQueueUrlInput{
				QueueName: &queueName,
			})
		if err != nil {
			return nil, err
		}
		a.queueUrls[priority] = out.QueueUrl
		queueUrl = out.QueueUrl
	}
	return queueUrl, nil
}

// PublishSqs handles publishing to AWS SNS
func (a *amazonWebServices) SendMessageSQS(ctx context.Context, priority Priority, payload string,
	headers map[string]string) error {

	attributes := make(map[string]*sqs.MessageAttributeValue)
	for key, value := range headers {
		attributes[key] = &sqs.MessageAttributeValue{
			StringValue: &value,
			DataType:    aws.String("String"),
		}
	}

	queueUrl, err := a.ensureQueueUrl(ctx, priority)
	if err != nil {
		return err
	}

	_, err = a.sqs.SendMessageWithContext(
		ctx,
		&sqs.SendMessageInput{
			QueueUrl:          queueUrl,
			MessageBody:       &payload,
			MessageAttributes: attributes,
		},
	)

	return err
}

func (a *amazonWebServices) messageHandler(ctx context.Context, messageBody string, receipt string) error {
	message := message{}
	err := json.Unmarshal([]byte(messageBody), &message)
	if err != nil {
		logrus.Errorf("invalid message, unable to unmarshal")
		return err
	}

	err = message.validate()
	if err != nil {
		return err
	}

	return message.callTask(ctx, receipt)
}

func (a *amazonWebServices) messageHandlerSQS(ctx context.Context, message *sqs.Message) error {
	return a.messageHandler(ctx, *message.Body, *message.ReceiptHandle)
}

func (a *amazonWebServices) messageHandlerLambda(ctx context.Context, record *events.SNSEventRecord) error {
	return a.messageHandler(ctx, record.SNS.Message, "")
}

// waitGroupError is like sync.WaitGroup but provides one extra field for storing error
type waitGroupError struct {
	sync.WaitGroup
	Error error
}

// DoneWithError may be used instead .Done() when there's an error
// This method clobbers the original error so you only see the last set error
func (w *waitGroupError) DoneWithError(err error) {
	w.Error = err
	w.Done()
}

func (a *amazonWebServices) processRecord(wge *waitGroupError, ctx context.Context, record *events.SNSEventRecord) {

	getPreProcessHookLambdaApp(ctx)(record)
	err := a.messageHandlerLambda(ctx, record)
	if err == nil {
		wge.Done()
		return
	}
	logrus.Errorf("failed to process lambda event with error: %v", err)
	wge.DoneWithError(err)
}

func (a *amazonWebServices) HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error {
	wge := waitGroupError{}
	for i := range snsEvent.Records {
		select {
		case <-ctx.Done():
			break
		default:
			wge.Add(1)
			go a.processRecord(&wge, ctx, &snsEvent.Records[i])
		}
	}
	wge.Wait()
	if ctx.Err() != nil {
		// if context was cancelled, signal appropriately
		return ctx.Err()
	}
	return wge.Error
}

func (a *amazonWebServices) processMessage(wg *sync.WaitGroup, ctx context.Context, queueMessage *sqs.Message,
	queueUrl *string, queueName string) {
	defer wg.Done()
	getPreProcessHookQueueApp(ctx)(&queueName, queueMessage)
	err := a.messageHandlerSQS(ctx, queueMessage)
	switch err {
	case nil:
		_, err := a.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      queueUrl,
			ReceiptHandle: queueMessage.ReceiptHandle,
		})
		if err != nil {
			logrus.Errorf("Failed to delete message with error: %v", err)
		}
	case RetryException:
		logrus.Debug("Retrying due to exception")
	default:
		logrus.Errorf("Retrying due to unknown exception: %v", err)
	}
}

func (a *amazonWebServices) FetchAndProcessMessages(ctx context.Context, priority Priority,
	numMessages uint, visibilityTimeoutS uint) error {

	queueName := getSQSQueue(ctx, priority)
	queueUrl, err := a.ensureQueueUrl(ctx, priority)
	if err != nil {
		return err
	}

	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(numMessages)),
		QueueUrl:            queueUrl,
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}
	if visibilityTimeoutS != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeoutS))
	}

	out, err := a.sqs.ReceiveMessageWithContext(ctx, input)
	wg := sync.WaitGroup{}
	for _, queueMessage := range out.Messages {
		select {
		case <-ctx.Done():
			break
		default:
			wg.Add(1)
			go a.processMessage(&wg, ctx, queueMessage, queueUrl, queueName)
		}
	}
	wg.Wait()
	// if context was cancelled, signal appropriately
	return ctx.Err()
}

func newAmazonWebServices(sessionCache *AWSSessionsCache, ctx context.Context) iamazonWebServices {
	awsSession := sessionCache.GetSession(ctx)
	amazonWebServices := amazonWebServices{
		sns:       sns.New(awsSession),
		sqs:       sqs.New(awsSession),
		queueUrls: nil,
	}
	return &amazonWebServices
}
