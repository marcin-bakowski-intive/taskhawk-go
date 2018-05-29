#!/usr/bin/env bash

set -e

find . -not -path "./vendor/*" -name "*.go" | xargs gofmt -s -w
