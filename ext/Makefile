MODULE_big = walextract
OBJS = walextract_ext.o walextract_core.o
EXTENSION = walextract
DATA = walextract--1.0.sql
PGFILEDESC = "walextract - mine DDL/DML effects from WAL"
REGRESS = walextract walextract_txn walextract_boundary walextract_sidecar walextract_insert walextract_dml walextract_dml_sidecar walextract_dml_batch walextract_xid_poison walextract_batch

# Core source lives in ../core (sibling dir in the distributed package).
# No hardcoded PG_CONFIG: override via command line or environment.
PG_CPPFLAGS += -I../core

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Compile the shared core from ../core without putting ../core on VPATH
# (VPATH would also redirect the .control/DATA install lookups).
walextract_core.o: ../core/walextract_core.c ../core/walextract_core.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<
