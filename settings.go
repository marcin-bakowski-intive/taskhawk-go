/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"
	"fmt"
	"time"
)

// PreProcessHookQueueApp is the type of the function for pre-process hook for SQS apps
type PreProcessHookQueueApp func(request *QueueRequest) error

// PreProcessHookLambdaApp is the type of the function for pre-process hook for lambda apps
type PreProcessHookLambdaApp func(request *LambdaRequest) error

// DefaultHeaders is the type of the function for injecting custom headers for every task
type DefaultHeaders func(ctx context.Context, task ITask) map[string]string

// Settings is used to create Taskhawk settings
type Settings struct {
	AWSRegion    string
	AWSAccountID string
	AWSAccessKey string
	AWSSecretKey string

	// AWSSessionToken represents temporary credentials (for example, for Lambda apps)
	AWSSessionToken string // optional;

	// DefaultHeaders is a function that may be used to inject custom headers into every message,
	// for example, request id. This hook is called right before dispatch, and
	// any headers that are explicitly specified when dispatching may override
	// these headers.
	DefaultHeaders DefaultHeaders // optional;

	// IsLambdaApp indicates if this is a lambda app (which uses SNS instead of SQS).
	IsLambdaApp bool

	// PreProcessHookQueueApp is a function which can be used to plug into the message processing pipeline
	// BEFORE any processing happens. This hook may be used to perform
	// initializations such as set up a global request id based on message
	// headers.
	PreProcessHookQueueApp PreProcessHookQueueApp // optional;

	// PreProcessHookLambdaApp is a function which can be used to plug into the message processing pipeline
	// BEFORE any processing happens. This hook may be used to perform
	// initializations such as set up a global request id based on message
	// headers.
	PreProcessHookLambdaApp PreProcessHookLambdaApp // optional;

	// Queue is the name of the taskhawk queue for this project (exclude the TASKHAWK- prefix)
	Queue string

	// ShutdownTimeout is the time the app has to shut down before being brutally killed
	ShutdownTimeout time.Duration // optional; defaults to 10s

	// Sync changes taskhawk dispatch to synchronous mode. This is similar
	// to Celery's Eager mode and is helpful for integration testing
	Sync bool
}

func noOpPreProcessHookQueueApp(_ *QueueRequest) error {
	return nil
}

func noOpPreProcessHookLambdaApp(_ *LambdaRequest) error {
	return nil
}

func emptyDefaultHeaders(_ context.Context, _ ITask) map[string]string {
	return map[string]string{}
}

// type to use for all keys in context so we don't clobber values set by other packages
type contextKey string

const (
	settingsKey = contextKey("Settings")
)

func getAWSRegion(ctx context.Context) string {
	return ctx.Value(settingsKey).(*Settings).AWSRegion
}

func getAWSAccountID(ctx context.Context) string {
	return ctx.Value(settingsKey).(*Settings).AWSAccountID
}

func getAWSAccessKey(ctx context.Context) string {
	return ctx.Value(settingsKey).(*Settings).AWSAccessKey
}

func getAWSSecretKey(ctx context.Context) string {
	return ctx.Value(settingsKey).(*Settings).AWSSecretKey
}

func getAWSSessionToken(ctx context.Context) string {
	return ctx.Value(settingsKey).(*Settings).AWSSessionToken
}

func getDefaultHeaders(ctx context.Context) DefaultHeaders {
	return ctx.Value(settingsKey).(*Settings).DefaultHeaders
}

func getIsLambdaApp(ctx context.Context) bool {
	return ctx.Value(settingsKey).(*Settings).IsLambdaApp
}

func getPreProcessHookQueueApp(ctx context.Context) PreProcessHookQueueApp {
	return ctx.Value(settingsKey).(*Settings).PreProcessHookQueueApp
}

func getPreProcessHookLambdaApp(ctx context.Context) PreProcessHookLambdaApp {
	return ctx.Value(settingsKey).(*Settings).PreProcessHookLambdaApp
}

func getQueue(ctx context.Context) string {
	return ctx.Value(settingsKey).(*Settings).Queue
}

func getShutdownTimeout(ctx context.Context) time.Duration {
	return ctx.Value(settingsKey).(*Settings).ShutdownTimeout
}

func getSync(ctx context.Context) bool {
	return ctx.Value(settingsKey).(*Settings).Sync
}

func withSettings(ctx context.Context, settings *Settings) context.Context {
	return context.WithValue(ctx, settingsKey, settings)
}

func withDefaults(s *Settings) {
	if s.PreProcessHookQueueApp == nil {
		s.PreProcessHookQueueApp = PreProcessHookQueueApp(noOpPreProcessHookQueueApp)
	}
	if s.PreProcessHookLambdaApp == nil {
		s.PreProcessHookLambdaApp = PreProcessHookLambdaApp(noOpPreProcessHookLambdaApp)
	}
	if s.DefaultHeaders == nil {
		s.DefaultHeaders = DefaultHeaders(emptyDefaultHeaders)
	}
	if s.ShutdownTimeout == 0 {
		s.ShutdownTimeout = 10 * time.Second
	}
	return
}

func verifyRequired(input *Settings) {
	var missing []string
	if input.AWSRegion == "" {
		missing = append(missing, "AWSRegion")
	}
	if input.AWSAccountID == "" {
		missing = append(missing, "AWSAccountID")
	}
	if input.AWSAccessKey == "" {
		missing = append(missing, "AWSAccessKey")
	}
	if input.AWSSecretKey == "" {
		missing = append(missing, "AWSSecretKey")
	}
	if input.Queue == "" {
		missing = append(missing, "Queue")
	}
	if len(missing) > 0 {
		panic(fmt.Sprintf("Taskhawk required settings missing: %v", missing))
	}
}

// InitSettings initializes a settings object after validating all values and filling in defaults.
// This function must be called before Settings object is used.
func InitSettings(settings *Settings) {
	verifyRequired(settings)
	withDefaults(settings)
}
