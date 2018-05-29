.PHONY: test

gofmt:
	./scripts/gofmt.sh

test_setup:
	./scripts/test-setup.sh

test: test_setup
	./scripts/run-tests.sh
