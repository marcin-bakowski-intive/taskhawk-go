# TaskHawk Go

[![Build Status](https://travis-ci.org/Automatic/taskhawk-go.svg?branch=master)](https://travis-ci.org/Automatic/taskhawk-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/Automatic/taskhawk-go)](https://goreportcard.com/report/github.com/Automatic/taskhawk-go)
[![Godoc](https://godoc.org/github.com/Automatic/taskhawk-go/taskhawk?status.svg)](http://godoc.org/github.com/Automatic/taskhawk-go/taskhawk)

TaskHawk is a replacement for celery that works on AWS SQS/SNS, while
keeping things pretty simple and straight forward. Any unbound function
can be converted into a TaskHawk task.

Only Go 1.10+ is supported currently.

This project uses [semantic versioning](http://semver.org/).

## Quick Start

First, install the library:

```bash
go get github.com/Automatic/taskhawk-go
```

Convert your function into a "Task" as shown here:

```go 
type SendEmailTaskInput struct {...}

type SendEmailTask struct {
    taskhawk.Task
}

func (t *SendEmailTask) Run(rawInput interface{}) {
    input := rawInput.(*SendEmailTaskInput)
    
    // send email
}
```

Tasks may accept input of arbitrary type as long as it's serializable to JSON

Then, define a few required settings:

```go 
sessionCache := AWSSessionsCache{}

settings := taskhawk.Settings{
	AWSAccessKey: <YOUR AWS ACCESS KEY>,
	AWSAccountID: <YOUR AWS ACCOUNT ID>,
    AWSRegion: <YOUR AWS REGION>,
    AWSSecretKey: <YOUR AWS SECRET KEY>,

    Queue: <YOUR TASKHAWK QUEUE>,
}
taskhawk.InitSettings(settings)
```

Before the task can be dispatched, it would need to be registered like so:

```go 
func NewSendEmailTask() *SendEmailTask {
    return &SendEmailTask{
        Task: taskhawk.Task{
            Inputer: func() interface{} {
                return &SendEmailTaskInput{}
            },
            Publisher: NewPublisher(sessionCache, settings),
        }
    }
}

taskhawk.RegisterTask(NewSendEmailTask())
```

And finally, dispatch your task asynchronously:

```go 
NewSendEmailTask().dispatch(&SendEmailTaskInput{...})
```

## Development

### Getting Started

Assuming that you have golang installed, set up your environment like so:

```bash

$ cd ${GOPATH}/src/github.com/Automatic/taskhawk-go
$ govendor sync
```

### Running tests

```bash

$ make test  
# OR
$ go test -tags test ./...
```

## Getting Help

We use GitHub issues for tracking bugs and feature requests.

* If it turns out that you may have found a bug, please [open an issue](https://github.com/Automatic/taskhawk-go/issues/new>)

## Release notes

**Current version: v1.0.2-dev**

### v1.0.0

  - Initial version
