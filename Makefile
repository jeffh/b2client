.PHONY: test

GO := $(shell which go)

# Example goargs
# GOARGS=-race for race condition checking

test:
	$(GO) test $(GOARGS) ./...
