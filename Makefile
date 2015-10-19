DEPS=./deps

default: build

%.o: $(DEPS)/read.c $(DEPS)/sds.c
$(DEPS)/libdeps.a: $(DEPS)/read.o $(DEPS)/sds.o
	$(AR) $(ARFLAGS) $@ $^

build: $(DEPS)/libdeps.a
	cd src && $(MAKE)
	@mv src/corvus .

clean:
	@find . -name '*.[oa]' -delete
	@if [ -f corvus ]; then rm corvus; fi

.PHONY: clean
