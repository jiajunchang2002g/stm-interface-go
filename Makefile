GO ?= go
GOFMT ?= gofmt "-s"
GOFILES := $(shell find . -name "*.go")

CC = clang
CXX = clang++

CFLAGS := $(CFLAGS) -g -O3 -Wall -Wextra -pedantic -Werror -std=c18 -pthread
CXXFLAGS := $(CXXFLAGS) -g -O3 -Wall -Wextra -pedantic -Werror -std=c++20 -pthread

all: stminterface client

stminterface:
	$(GO) build -o $@ ./main

client: cpp-src/client.cpp cpp-src/io.h
	$(CXX) $(CXXFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f *.o client stminterface

.PHONY: fmt stminterface
fmt:
	$(GOFMT) -w $(GOFILES)

# dependency handling
# https://make.mad-scientist.net/papers/advanced-auto-dependency-generation/#tldr

DEPDIR := .deps
DEPFLAGS = -MT $@ -MMD -MP -MF $(DEPDIR)/$<.d

COMPILE.cpp = $(CXX) $(DEPFLAGS) $(CXXFLAGS) $(CPPFLAGS) $(TARGET_ARCH) -c

%.cpp.o: %.cpp
%.cpp.o: %.cpp $(DEPDIR)/%.cpp.d | $(DEPDIR)
	$(COMPILE.cpp) $(OUTPUT_OPTION) $<

$(DEPDIR): ; @mkdir -p $@

DEPFILES := $(SRCS:%=$(DEPDIR)/%.d)
$(DEPFILES):

include $(wildcard $(DEPFILES))
