#!/bin/bash

if [ -n "$(gofmt -l .)" ]; then
  echo >&2 "Code not properly formatted. Run 'gofmt -s -w'"
  gofmt -d .
  exit 1
fi
