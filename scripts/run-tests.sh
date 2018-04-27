#!/bin/bash

set -x

go test -v -tags test -race ./...
