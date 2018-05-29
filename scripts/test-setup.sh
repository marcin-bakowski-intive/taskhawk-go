#!/bin/bash

set -x

if [[ "${TRAVIS}" == "true" ]]; then
    go get github.com/kardianos/govendor

    go tool vet .

    unformatted=$(find . -not -path "./vendor/*" -name "*.go" | xargs gofmt -s -l)

    if [ ! -z "$unformatted" ]; then
        echo >&2 "Code not properly formatted. Run 'make gofmt'"
        exit 1
    fi

    govendor sync

    if [ ! -z "$(govendor list -no-status +outside)" ]; then
        echo "External dependencies found, only vendored dependencies supported"
        exit 1
    fi
fi
