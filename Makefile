default: corvus

.DEFAULT:
	@$(MAKE) $@ -C src

deps:
	@$(MAKE) $@ -C deps

clean:
	@$(MAKE) $@ -C src
	@$(MAKE) $@ -C tests

test:
	@$(MAKE) $@ -C tests

.PHONY: clean test deps
