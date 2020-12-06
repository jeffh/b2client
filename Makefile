.PHONY: test integration_tests

GO := $(shell which go)

# Example goargs
# GOARGS=-race for race condition checking

test:
	$(GO) test $(GOARGS) ./...

integration_tests:
	env TEST_B2_INTEGRATION_TESTS=true $(GO) test $(GOARGS) ./...
