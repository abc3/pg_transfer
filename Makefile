.PHONY: build install clean

all: build

build:
	zig build

install:
	zig build install

clean:
	rm -rf .zig-cache zig-out
