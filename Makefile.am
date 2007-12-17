dfschlibexec_LTLIBRARIES=postgres.la

nobase_include_headers=dfsch-ext/postgres.h

postgres_la_SOURCES=src/postgres.c
postgres_la_LIBADD=-ldfsch $(SQLITE_LIBS)
postgres_la_CFLAGS=$(SQLITE_CFLAGS)
postgres_la_LDFLAGS=-module