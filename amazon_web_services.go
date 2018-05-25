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

	"golang.org/x/sync/errgroup"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
)

type iamazonWebServices interface {
	PublishSNS(ctx context.Context, priority Priority, payload string, headers map[string]string) error
	SendMessageSQS(ctx context.Context, priority Priority, payload string,
		headers map[string]string) error
	FetchAndProcessMessages(ctx context.Context, priority Priority, numMessages uint,
		visibilityTimeoutS uint) error
	HandleLambdaEvent(ctx context.Context, snsEvent *events.SNSEvent) error
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
	return errors.Wrap(err, "Failed to publish message to SNS")
}

func (a *amazonWebServices) ensureQueueURL(ctx context.Context, priority Priority) (*string, error) {
	if a.queueUrls == nil {
		a.queueUrls = make(map[Priority]*string)
	}
	queueURL, ok := a.queueUrls[priority]
	if !ok {
		queueName := getSQSQueue(ctx, priority)
		out, err := a.sqs.GetQueueUrlWithContext(
			ctx,
			&sqs.GetQueueUrlInput{
				QueueName: &queueName,
			})
		if err != nil {
			return nil, errors.Wrap(err, "unable to get queue url")
		}
		a.queueUrls[priority] = out.QueueUrl
		queueURL = out.QueueUrl
	}
	return queueURL, nil
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

	queueURL, err := a.ensureQueueURL(ctx, priority)
	if err != nil {
		return err
	}

	_, err = a.sqs.SendMessageWithContext(
		ctx,
		&sqs.SendMessageInput{
			QueueUrl:          queueURL,
			MessageBody:       &payload,
			MessageAttributes: attributes,
		},
	)

	return errors.Wrap(err, "Failed to send message to SQS")
}

func (a *amazonWebServices) messageHandler(ctx context.Context, messageBody string, receipt string) error {
	message := message{}
	err := json.Unmarshal([]byte(messageBody), &message)
	if err != nil {
		logrus.WithError(err).Errorf("invalid message, unable to unmarshal")
		return errors.Wrap(err, "unable to unmarshal message")
	}

	err = message.validate()
	if err != nil {
		return err
	}

	return message.callTask(ctx, receipt)
}

func (a *amazonWebServices) messageHandlerSQS(request *QueueRequest) error {
	return a.messageHandler(request.Ctx, *request.QueueMessage.Body, *request.QueueMessage.ReceiptHandle)
}

func (a *amazonWebServices) messageHandlerLambda(request *LambdaRequest) error {
	return a.messageHandler(request.Ctx, request.Record.SNS.Message, "")
}

func (a *amazonWebServices) processRecord(request *LambdaRequest) error {
	err := getPreProcessHookLambdaApp(request.Ctx)(request)
	if err != nil {
		logrus.WithError(err).Errorf("Pre-process hook failed with error: %v", err)
		return errors.Wrap(err, "failed to execute pre process hook")
	}
	err = a.messageHandlerLambda(request)
	if err == nil {
		return nil
	}
	logrus.WithError(err).Errorf("failed to process lambda event with error: %v", err)
	return err
}

func (a *amazonWebServices) HandleLambdaEvent(ctx context.Context, snsEvent *events.SNSEvent) error {
	wg, childCtx := errgroup.WithContext(ctx)
	for i := range snsEvent.Records {
		request := &LambdaRequest{
			Ctx:    childCtx,
			Record: &snsEvent.Records[i],
		}
		select {
		case <-ctx.Done():
			break
		default:
			wg.Go(func() error {
				return a.processRecord(request)
			})
		}
	}
	err := wg.Wait()
	if ctx.Err() != nil {
		// if context was cancelled, signal appropriately
		return ctx.Err()
	}
	return err
}

func (a *amazonWebServices) processMessage(wg *sync.WaitGroup, request *QueueRequest) {
	defer wg.Done()
	err := getPreProcessHookQueueApp(request.Ctx)(request)
	if err != nil {
		logrus.WithError(err).Errorf("Pre-process hook failed with error: %v", err)
		return
	}
	err = a.messageHandlerSQS(request)
	switch err {
	case nil:
		_, err := a.sqs.DeleteMessageWithContext(request.Ctx, &sqs.DeleteMessageInput{
			QueueUrl:      &request.QueueURL,
			ReceiptHandle: request.QueueMessage.ReceiptHandle,
		})
		if err != nil {
			logrus.WithError(err).Errorf("Failed to delete message with error: %v", err)
		}
	case ErrRetry:
		logrus.Debug("Retrying due to exception")
	default:
		logrus.WithError(err).Errorf("Retrying due to unknown exception: %v", err)
	}
}

func (a *amazonWebServices) FetchAndProcessMessages(ctx context.Context, priority Priority,
	numMessages uint, visibilityTimeoutS uint) error {

	queueName := getSQSQueue(ctx, priority)
	queueURL, err := a.ensureQueueURL(ctx, priority)
	if err != nil {
		return err
	}

	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(numMessages)),
		QueueUrl:            queueURL,
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}
	if visibilityTimeoutS != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeoutS))
	}

	out, err := a.sqs.ReceiveMessageWithContext(ctx, input)
	if err != nil {
		return errors.Wrap(err, "failed to receive SQS message")
	}
	wg := sync.WaitGroup{}
	for _, queueMessage := range out.Messages {
		request := &QueueRequest{
			Ctx:          ctx,
			QueueMessage: queueMessage,
			QueueURL:     *queueURL,
			QueueName:    queueName,
		}
		select {
		case <-ctx.Done():
			break
		default:
			wg.Add(1)
			go a.processMessage(&wg, request)
		}
	}
	wg.Wait()
	// if context was cancelled, signal appropriately
	return ctx.Err()
}

func newAmazonWebServices(ctx context.Context, sessionCache *AWSSessionsCache) iamazonWebServices {
	awsSession := sessionCache.GetSession(ctx)
	amazonWebServices := amazonWebServices{
		sns:       sns.New(awsSession),
		sqs:       sqs.New(awsSession),
		queueUrls: nil,
	}
	return &amazonWebServices
}
