default: build

build:
	@mkdir -p $@
	cd src && $(MAKE)
	@mv src/corvus .

init:
	@git submodule update --init --recursive

clean:
	@rm -rf build
	@find . -name '*.[oa]' -delete
	@rm -f corvus

test:
	cd tests && $(MAKE) test

.PHONY: clean init test
