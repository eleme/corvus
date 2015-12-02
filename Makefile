default: corvus

.DEFAULT:
	$(MAKE) $@ -C src

clean:
	$(MAKE) $@ -C src
	$(MAKE) $@ -C tests

test:
	$(MAKE) $@ -C tests

.PHONY: clean init test
