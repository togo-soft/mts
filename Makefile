.PHONY : clean all

all:
    buf generate
    go mod tidy
