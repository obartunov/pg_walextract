# pg_waldump parity contract

The harness uses pg_waldump as the source-grounded oracle for WAL record
classes. It compares STRUCTURED CLASSES (rmgr / rectype / user-relfilenode /
counts), never raw human text.

## Record categories (by rmgr|rectype and relfilenode)

- user DML  (relfilenode >= 16384):
    Heap2|MULTI_INSERT[+INIT]   -> rows = sum(ntuples)
    Heap|INSERT[+INIT]          -> single insert
    Heap|UPDATE, Heap|HOT_UPDATE
    Heap|DELETE
- dictionary-learning (catalog, relfilenode < 16384): same rectypes on
    pg_class / pg_attribute / other catalogs -> consumed internally.
- lifecycle: Transaction|COMMIT, Transaction|ABORT.
- forensic / meta: XLOG, Standby, Storage, Btree/Gin/Gist/..., Heap2|PRUNE*,
    VISIBLE, FREEZE_PAGE, Heap|LOCK/INPLACE, etc. -> carry no user row.

## Allowed walextract outcomes per user-DML class

| class                        | event path           | batch (machine) path        |
|------------------------------|----------------------|-----------------------------|
| Heap2 MULTI_INSERT (user)    | decoded INSERT       | decoded ChangeBatch         |
| Heap INSERT (user)           | decoded INSERT       | fail-closed (single_insert) |
| Heap UPDATE/HOT_UPDATE (user)| (forensic-only*)     | fail-closed (update)        |
| Heap DELETE (user)           | (forensic-only*)     | fail-closed (delete)        |
| same-txn TRUNCATE / DROP     | suppressed (boundary)| pruned at COMMIT            |
| ABORT                        | discarded            | discarded                   |
| open-tx at range end         | not delivered        | not delivered, freed        |

* event path currently emits no marker for user UPDATE/DELETE (INSERT-only
  forensic scope). See KNOWN FINDINGS.

## Forbidden (gate-failing)
- silent skip of user-visible WAL
- silent partial machine stream
- fake apply-candidate output
- human-text parsing as the product API

## Known findings (surfaced by this harness; NOT P2A scope)
- F1 event-path UPDATE/DELETE: no explicit unsupported marker (silent at row
  level). Machine/batch path fail-closes correctly. -> future gate
  "event-path explicit-unsupported markers".
- F2 toast-relation classification: a TOAST table's relfilenode is >= 16384 and
  is not recognised as non-user, so toast-chunk single inserts are seen as user
  INSERTs (event mode) and make a COPY-with-external-TOAST fail closed in batch
  mode (workload J). No silent loss (explicit), but the ChangeBatch
  has_external_toast path is only reachable once toast rels are excluded.
  -> future gate "P2A.2 toast-relation stream exclusion".
