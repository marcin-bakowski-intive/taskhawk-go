/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func createSession(region string, awsAccessKey string, awsSecretAccessKey string,
	awsSessionToken string) *session.Session {
	return session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Credentials: credentials.NewStaticCredentialsFromCreds(
					credentials.Value{
						AccessKeyID:     awsAccessKey,
						SecretAccessKey: awsSecretAccessKey,
						SessionToken:    awsSessionToken,
					},
				),
				Region:     aws.String(region),
				DisableSSL: aws.Bool(false),
			},
		}))
}

type sessionKey struct {
	awsRegion       string
	awsAccessKeyID  string
	awsSessionToken string
}

// AWSSessionsCache is a cache that holds AWS sessions
type AWSSessionsCache struct {
	sessionMap sync.Map
}

// NewAWSSessionsCache creates a new session cache
func NewAWSSessionsCache() *AWSSessionsCache {
	return &AWSSessionsCache{
		sessionMap: sync.Map{},
	}
}

func (c *AWSSessionsCache) getOrCreateSession(ctx context.Context) *session.Session {
	region := getAWSRegion(ctx)
	awsAccessKey := getAWSAccessKey(ctx)
	awsSecretAccessKey := getAWSSecretKey(ctx)
	awsSessionToken := getAWSSessionToken(ctx)

	key := sessionKey{awsRegion: region, awsAccessKeyID: awsAccessKey, awsSessionToken: awsSessionToken}
	s, ok := c.sessionMap.Load(key)
	if !ok {
		s = createSession(region, awsAccessKey, awsSecretAccessKey, awsSessionToken)
		s, _ = c.sessionMap.LoadOrStore(key, s)
	}
	return s.(*session.Session)
}

// GetSession retrieves a session if it is cached, otherwise creates one
func (c *AWSSessionsCache) GetSession(ctx context.Context) *session.Session {
	return c.getOrCreateSession(ctx)
}
