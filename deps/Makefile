jemalloc:
	cd jemalloc && ./configure --with-jemalloc-prefix=je_ --enable-cc-silence
	cd jemalloc && $(MAKE) lib/libjemalloc.a

jemalloc_conf:
	cd jemalloc && autoconf

distclean:
	$(MAKE) distclean -C jemalloc > /dev/null

deps: jemalloc_conf

.PHONY: jemalloc jemalloc_conf deps
