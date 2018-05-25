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
	"github.com/pkg/errors"
)

// IPublisher interface represents all publish related functions
type IPublisher interface {
	// Publish publishes a message on Taskhawk broker
	Publish(ctx context.Context, message *message) error

	// Settings returns publisher's settings
	Settings() *Settings
}

// publisher handles taskhawk publishing for Automatic
type publisher struct {
	awsClient iamazonWebServices
	settings  *Settings
}

// Publish a message on Taskhawk
func (ap *publisher) Publish(ctx context.Context, message *message) error {
	msgJSON, err := json.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "unable to marshal message")
	}

	if getIsLambdaApp(ctx) {
		return ap.awsClient.PublishSNS(ctx, message.Metadata.Priority, string(msgJSON), message.Headers)
	}

	return ap.awsClient.SendMessageSQS(ctx, message.Metadata.Priority, string(msgJSON), message.Headers)
}

func (ap *publisher) Settings() *Settings {
	return ap.settings
}

// NewPublisher creates a new publisher
func NewPublisher(sessionCache *AWSSessionsCache, settings *Settings) IPublisher {
	return &publisher{
		awsClient: newAmazonWebServices(withSettings(context.Background(), settings), sessionCache),
		settings:  settings,
	}
}
