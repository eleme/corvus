default: build

build:
	cd src && $(MAKE)
	@mv src/corvus .

clean:
	@find . -name '*.[oa]' -delete
	@if [ -f corvus ]; then rm corvus; fi

.PHONY: clean
