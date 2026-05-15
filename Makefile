.PHONY: clean all

all:
	@buf format -w
	@buf generate
	@go mod tidy
