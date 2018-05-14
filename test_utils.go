// +build test

/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package taskhawk

func getLambdaTestSettings() *Settings {
	settings := &Settings{
		AWSAccountID: "1234567890",
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
		IsLambdaApp:  true,
	}
	InitSettings(settings)
	return settings
}

func getQueueTestSettings() *Settings {
	settings := &Settings{
		AWSAccountID: "1234567890",
		AWSRegion:    "us-east-1",
		AWSAccessKey: "fake_access_1",
		AWSSecretKey: "fake_secret_2",
		Queue:        "dev-myapp",
	}
	InitSettings(settings)
	return settings
}
