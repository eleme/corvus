default: corvus

.DEFAULT:
	@mkdir -p build
	cd src && $(MAKE) $@
	@mv src/corvus .

init:
	@git submodule update --init --recursive

clean:
	@rm -rf build
	@find . -name '*.[oa]' -delete
	@rm -f corvus
	@rm -f tests/corvus_test

test:
	cd tests && $(MAKE) test

.PHONY: clean init test
