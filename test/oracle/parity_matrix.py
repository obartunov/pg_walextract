#!/usr/bin/env python3
"""
pg_waldump parity + machine-throughput engine for walextract.

For each workload range recorded in wx_ranges, feed the SAME [s0,s1] LSN range to
  1. pg_waldump            (source-grounded oracle: which WAL classes exist)
  2. walextract event path (forensic SRF: walextract_wal2sql / wal2event_count)
  3. walextract batch path (machine path: walextract_batch_stats)
and emit a correctness/coverage table and a performance table.

The oracle is used to classify records, NOT to imitate.  Comparison is on
structured classes (rmgr / rectype / user-relfilenode / counts), never raw text.

User-visible DML = Heap2 MULTI_INSERT, Heap INSERT/UPDATE/HOT_UPDATE/DELETE on a
relfilenode >= FirstNormalObjectId (16384).  Everything else (catalog DML, index,
vm/freeze/prune, xlog/standby/storage meta, transaction records) is dictionary
-learning or forensic and may be consumed internally.

A "silent gap" is a user-visible DML class present in the oracle that walextract
neither decoded, explicitly refused, nor failed closed on.
"""
import argparse, re, subprocess, sys, time

FIRST_NORMAL_OID = 16384
RE_RMGR = re.compile(r"rmgr:\s+(\S+)")
RE_DESC = re.compile(r"desc:\s+(\S+)")
RE_REL = re.compile(r"rel\s+\d+/\d+/(\d+)")
RE_NTUP = re.compile(r"ntuples:\s+(\d+)")
RE_TIME = re.compile(r"Time:\s+([0-9.]+)\s+ms")


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


class Args:
    pass


def psql_base(a):
    return [a.psql, "-p", str(a.port), "-h", a.host, "-U", a.user, "-d", a.db, "-X"]


def psql_scalar_rows(a, sql):
    """Run sql, return (rows, error_message). rows = list of '|'-split field lists."""
    p = run(psql_base(a) + ["-q", "-t", "-A", "-F", "|", "-c", sql])
    if p.returncode != 0:
        msg = (p.stderr or p.stdout).strip().splitlines()
        return None, (msg[-1] if msg else "error")
    rows = [ln.split("|") for ln in p.stdout.strip().splitlines() if ln.strip()]
    return rows, None


def timed(a, sql, iters=3):
    """Return (min_ms, error). Runs `sql` iters times under \\timing; None ms on error."""
    script = "\\timing on\n" + ("%s;\n" % sql) * iters
    p = run(psql_base(a) + ["-q", "-t", "-A"], input=script)
    if "ERROR" in (p.stderr + p.stdout):
        err = ""
        for ln in (p.stderr + p.stdout).splitlines():
            if "ERROR" in ln:
                err = ln.split("ERROR:", 1)[-1].strip()
                break
        return None, err
    times = [float(m) for m in RE_TIME.findall(p.stdout + p.stderr)]
    return (min(times) if times else None), None


def toast_relfilenodes(a):
    """relfilenodes that pg_class marks as TOAST (relkind='t'): internal storage."""
    rows, _ = psql_scalar_rows(a, "SELECT relfilenode FROM pg_class WHERE relkind='t' AND relfilenode<>0")
    return set(int(r[0]) for r in rows) if rows else set()


def oracle(a, s0, s1, toast_set):
    """Run pg_waldump over [s0,s1]; return (summary dict, wall_ms).
    Heap records into a TOAST relfilenode are internal storage, not user DML."""
    t0 = time.perf_counter()
    p = run([a.waldump, "-p", a.pgdata, "--start", s0, "--end", s1])
    wall = (time.perf_counter() - t0) * 1000.0
    out = p.stdout
    cls = {}                         # "Rmgr|RECTYPE" -> count
    u = dict(mi_rec=0, mi_rows=0, ins=0, upd=0, dele=0)
    c = dict(mi_rec=0, mi_rows=0, ins=0, upd=0, dele=0)
    toast_ins = 0
    forensic = 0
    for line in out.splitlines():
        mr, md = RE_RMGR.search(line), RE_DESC.search(line)
        if not mr or not md:
            continue
        rmgr, desc = mr.group(1), md.group(1)
        head = desc.split("+")[0]
        cls[rmgr + "|" + desc] = cls.get(rmgr + "|" + desc, 0) + 1
        relm = RE_REL.search(line)
        relfile = int(relm.group(1)) if relm else None
        is_toast = relfile is not None and relfile in toast_set
        is_user = relfile is not None and relfile >= FIRST_NORMAL_OID and not is_toast
        if rmgr == "Heap2" and head == "MULTI_INSERT":
            nt = int(RE_NTUP.search(line).group(1)) if RE_NTUP.search(line) else 0
            if is_toast:
                toast_ins += nt
            else:
                d = u if is_user else c
                d["mi_rec"] += 1; d["mi_rows"] += nt
        elif rmgr == "Heap" and head == "INSERT":
            if is_toast:
                toast_ins += 1
            else:
                (u if is_user else c)["ins"] += 1
        elif rmgr == "Heap" and head in ("UPDATE", "HOT_UPDATE"):
            (u if is_user else c)["upd"] += 1
        elif rmgr == "Heap" and head == "DELETE":
            (u if is_user else c)["dele"] += 1
        elif rmgr == "Transaction":
            pass
        else:
            forensic += 1
    return dict(cls=cls, user=u, cat=c, toast=toast_ins, forensic=forensic, raw=out), wall


def short_classes(o):
    u = o["user"]
    parts = []
    if u["mi_rows"]:
        parts.append("MI rows=%d" % u["mi_rows"])
    if u["ins"]:
        parts.append("INSERT=%d" % u["ins"])
    if u["upd"]:
        parts.append("UPDATE=%d" % u["upd"])
    if u["dele"]:
        parts.append("DELETE=%d" % u["dele"])
    if not parts:
        parts.append("no user DML")
    if o.get("toast"):
        parts.append("toast-internal=%d" % o["toast"])
    parts.append("forensic+cat=%d" % (o["forensic"] + sum(o["cat"].values())))
    return ", ".join(parts)


def assess(a, wl, s0, s1, o):
    u = o["user"]
    user_dml = u["mi_rows"] + u["ins"] + u["upd"] + u["dele"]

    # event path correctness via wal2sql (count user INSERT events); capture errors
    ev_rows, ev_err = psql_scalar_rows(
        a, "SELECT count(*) FROM walextract_wal2sql('%s','%s') WHERE op='INSERT'" % (s0, s1))
    ev_insert = int(ev_rows[0][0]) if ev_rows else None

    # batch path correctness via batch_stats; capture fail-closed reason
    b_rows, b_err = psql_scalar_rows(
        a, "SELECT n_batches, total_rows, COALESCE(any_external_toast,false) "
           "FROM walextract_batch_stats('%s','%s')" % (s0, s1))
    if b_rows:
        b_nbatch, b_rows_n, b_toast = int(b_rows[0][0]), int(b_rows[0][1]), b_rows[0][2] == "t"
    else:
        b_nbatch = b_rows_n = None; b_toast = False

    # ---- event outcome + gap ----
    ev_expected_ins = u["mi_rows"] + u["ins"]   # event decodes INSERT (multi + single)
    if ev_err:
        ev_outcome = "fail-closed: " + ev_err
        ev_gap = "no"
    else:
        ev_outcome = "decoded INSERT=%d" % (ev_insert or 0)
        # silent gap if user UPDATE/DELETE exist (event emits nothing for them)
        # or if expected inserts were not all decoded
        gap = (u["upd"] + u["dele"] > 0) or (ev_insert is not None and ev_insert < ev_expected_ins
                                             and ev_expected_ins > 0
                                             and not _aborted(wl) and not _pruned(wl) and not _open(wl))
        ev_gap = "UPDATE/DELETE" if (u["upd"] + u["dele"] > 0) else ("yes" if gap else "no")

    # ---- batch outcome + gap ----
    if b_err:
        b_outcome = "fail-closed: " + b_err
        b_gap = "no"   # explicit refusal is not a silent gap
    else:
        extra = " +toast-flagged" if b_toast else ""
        b_outcome = "decoded rows=%d (%d batches)%s" % (b_rows_n, b_nbatch, extra)
        # silent gap if user single INSERT/UPDATE/DELETE present but batch neither
        # failed closed nor (for MI) decoded all rows
        if u["ins"] + u["upd"] + u["dele"] > 0:
            b_gap = "yes"     # should have fail-closed
        elif u["mi_rows"] > 0 and not _aborted(wl) and not _pruned(wl) and not _open(wl):
            b_gap = "no" if b_rows_n == u["mi_rows"] else "yes"
        else:
            b_gap = "no"

    return dict(ev_outcome=ev_outcome, ev_gap=ev_gap, b_outcome=b_outcome,
                b_gap=b_gap, user_dml=user_dml)


def _aborted(wl):
    return wl in ("D", "E")


def _pruned(wl):
    return wl in ("F",)


def _open(wl):
    return wl in ("K",)


def perf(a, s0, s1, supported_batch):
    t = []
    for _ in range(3):
        t0 = time.perf_counter()
        run([a.waldump, "-p", a.pgdata, "--start", s0, "--end", s1])
        t.append((time.perf_counter() - t0) * 1000.0)
    waldump_ms = min(t)
    ev_ms, ev_err = timed(a, "SELECT n_events FROM walextract_wal2event_count('%s','%s')" % (s0, s1))
    b_ms, b_err = timed(a, "SELECT total_rows FROM walextract_batch_stats('%s','%s')" % (s0, s1))
    delivered = None
    if not b_err:
        rws, _ = psql_scalar_rows(a, "SELECT total_rows FROM walextract_batch_stats('%s','%s')" % (s0, s1))
        delivered = int(rws[0][0]) if rws else None
    return dict(waldump_ms=waldump_ms, ev_ms=ev_ms, ev_err=ev_err,
                b_ms=b_ms, b_err=b_err, delivered=delivered)


def fmt_ms(ms, err):
    if err:
        return "fail-closed"
    if ms is None:
        return "-"
    return "%.2f" % ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", default="5440")
    ap.add_argument("--host", default="/tmp")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--db", default="parity")
    ap.add_argument("--pgdata", default="/home/pgdata")
    ap.add_argument("--waldump", default="/usr/local/pgsql/bin/pg_waldump")
    ap.add_argument("--psql", default="/usr/local/pgsql/bin/psql")
    ap.add_argument("--mode", choices=["correctness", "performance", "both"], default="both")
    a = ap.parse_args()

    ranges, err = psql_scalar_rows(
        a, "SELECT workload, s0, s1, kind, pg_wal_lsn_diff(s1,s0) FROM wx_ranges ORDER BY seq")
    if ranges is None:
        print("could not read wx_ranges:", err); sys.exit(1)

    toast_set = toast_relfilenodes(a)
    rows = []
    for wl, s0, s1, kind, walb in ranges:
        o, owall = oracle(a, s0, s1, toast_set)
        rows.append((wl, kind, s0, s1, int(float(walb)), o))

    if a.mode in ("correctness", "both"):
        print("\n=== CORRECTNESS / COVERAGE (oracle = pg_waldump) ===")
        hdr = ["WL", "oracle user-DML classes", "event outcome", "ev gap?",
               "batch outcome", "batch gap?"]
        line = []
        for wl, kind, s0, s1, walb, o in rows:
            r = assess(a, wl, s0, s1, o)
            line.append([wl, short_classes(o), r["ev_outcome"], r["ev_gap"],
                         r["b_outcome"], r["b_gap"]])
        widths = [max(len(str(x)) for x in [hdr[i]] + [r[i] for r in line]) for i in range(len(hdr))]
        def pr(cols):
            print(" | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols)))
        pr(hdr); pr(["-" * w for w in widths])
        for r in line:
            pr(r)

    if a.mode in ("performance", "both"):
        print("\n=== PERFORMANCE (machine path vs event vs forensic oracle) ===")
        hdr = ["WL", "rows", "WALkB", "waldump ms", "event ms", "batch ms",
               "b/ev x", "b/dump x", "rows/s(batch)", "MB/s(batch)"]
        line = []
        for wl, kind, s0, s1, walb, o in rows:
            pf = perf(a, s0, s1, True)
            dl = pf["delivered"]
            bev = (pf["ev_ms"] / pf["b_ms"]) if (pf["ev_ms"] and pf["b_ms"]) else None
            bdump = (pf["waldump_ms"] / pf["b_ms"]) if (pf["waldump_ms"] and pf["b_ms"]) else None
            deliver_ok = dl and dl > 0
            rps = (dl / (pf["b_ms"] / 1000.0)) if (pf["b_ms"] and deliver_ok) else None
            mbs = ((walb / 1e6) / (pf["b_ms"] / 1000.0)) if (pf["b_ms"] and deliver_ok) else None
            padded = " *" if wl in ("D", "E", "K") else ""
            line.append([
                wl, (dl if dl is not None else "-"), str(walb // 1024) + padded,
                fmt_ms(pf["waldump_ms"], None),
                fmt_ms(pf["ev_ms"], pf["ev_err"]), fmt_ms(pf["b_ms"], pf["b_err"]),
                ("%.1f" % bev) if bev else "-", ("%.1f" % bdump) if bdump else "-",
                ("%.0f" % rps) if rps else "-", ("%.0f" % mbs) if mbs else "-",
            ])
        widths = [max(len(str(x)) for x in [hdr[i]] + [r[i] for r in line]) for i in range(len(hdr))]
        def pr(cols):
            print(" | ".join(str(c).rjust(widths[i]) for i, c in enumerate(cols)))
        pr(hdr); pr(["-" * w for w in widths])
        for r in line:
            pr(r)


if __name__ == "__main__":
    main()
