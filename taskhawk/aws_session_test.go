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
	"testing"
)

func lenSyncMap(p *sync.Map) int {
	count := 0
	p.Range(func(interface{}, interface{}) bool {
		count++
		return true
	})
	return count
}

func TestCreateSession(t *testing.T) {
	awsAccessKey := "fakeAccess"
	awsSecretAccessKey := "fakeSecretKey"
	awsRegion := "us-east-1"
	awsSessionKey := "temp-session"
	s := createSession(awsRegion, awsAccessKey, awsSecretAccessKey, awsSessionKey)

	if *s.Config.Region != awsRegion {
		t.Errorf("Expected region to be %s, got %s", awsRegion, *s.Config.Region)
	}

	value, err := s.Config.Credentials.Get()
	if err != nil {
		t.Errorf("Expected err to be nil, but got %+v", err)
	}

	if value.AccessKeyID != awsAccessKey {
		t.Errorf("Expected access key to be %s, got %s", awsAccessKey, value.AccessKeyID)
	}
	if value.SecretAccessKey != awsSecretAccessKey {
		t.Errorf("Expected secret access key to be %s, got %s", awsSecretAccessKey, value.SecretAccessKey)
	}
}

func TestGetSessionCached(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)

	sessionCache := AWSSessionsCache{}
	if lenSyncMap(&sessionCache.sessionMap) != 0 {
		t.Errorf("Expecting empty session map")
	}

	s := sessionCache.GetSession(ctx)
	if lenSyncMap(&sessionCache.sessionMap) != 1 {
		t.Errorf("Expecting session map to be updated")
	}

	settingsCopy := getQueueTestSettings()
	ctxCopy := withSettings(context.Background(), settingsCopy)

	sCopy := sessionCache.GetSession(ctxCopy)
	if lenSyncMap(&sessionCache.sessionMap) != 1 {
		t.Errorf("Expecting session map to not be updated")
	}
	if s != sCopy {
		t.Errorf("Expecting session pointers to be the same")
	}
}

func TestGetSessionDifferent(t *testing.T) {
	settings := getQueueTestSettings()
	ctx := withSettings(context.Background(), settings)

	sessionCache := AWSSessionsCache{}
	if lenSyncMap(&sessionCache.sessionMap) != 0 {
		t.Errorf("Expecting empty session map")
	}

	s := sessionCache.GetSession(ctx)
	if lenSyncMap(&sessionCache.sessionMap) != 1 {
		t.Errorf("Expecting session map to be updated")
	}

	settingsCopy := &Settings{
		AWSAccountID: "1234567890",
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_2",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
	}
	ctxCopy := withSettings(context.Background(), settingsCopy)

	sCopy := sessionCache.GetSession(ctxCopy)
	if lenSyncMap(&sessionCache.sessionMap) != 2 {
		t.Errorf("Expecting session map to not be updated")
	}
	if s == sCopy {
		t.Errorf("Expecting session pointers to be different")
	}
}
