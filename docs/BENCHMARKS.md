# golb benchmarks (v0.1)

Measured by QA on 2026-11-21 against commit `9fb2267` (after the fix round),
with the corrected load generator. These numbers replace the preliminary ones
taken at `6414ef7`. Every number was produced by the command shown and is
reported as measured. Targets are from `docs/PRD.md` sections 1.1 and 3.2.

**Every run in this document is labelled noisy.** The 1-minute load average
was between 19 and 56 on 11 CPUs during the measurements (work unrelated to
golb that could not be stopped); the threshold for a clean run was 15. The
load average is quoted beside each block. Treat absolute values as
pessimistic, and a difference between two rows as real only when their
min-max ranges do not overlap.

## 1. Conditions

| | |
|---|---|
| Machine | Apple M3 Pro, 11 logical CPUs (5 performance + 6 efficiency cores), 18 GB RAM |
| OS | macOS 15.5 (Darwin 24.5.0), arm64 |
| Go | go1.25.6 darwin/arm64, `GOMAXPROCS` unset (11) |
| Commit | `9fb2267` |
| Network | loopback only; load generator, golb and backends share the machine |
| Limits | `ulimit -n` 1048576; ephemeral ports 49152-65535 |

## 2. Micro-benchmarks

```sh
go test -run '^$' -bench=. -benchmem -count=5 ./internal/balancer
go test -run '^$' -bench=. -benchmem -count=5 ./internal/metrics
go test -run '^$' -bench=. -benchmem -count=5 ./internal/proxy
```

Load average 19.1 before, 53.6 after (noisy). `benchstat` is not installed;
minimum / median / maximum of five samples, ns/op, 11 procs.

| Benchmark | min | median | max | allocs/op |
|---|---|---|---|---|
| `BenchmarkPickDone/round_robin/16` | 4.60 | 4.99 | 5.19 | 0 |
| `BenchmarkPickDone/round_robin_weighted/16` | 5.05 | 5.96 | 6.83 | 0 |
| `BenchmarkPickDone/least_conn/16` | 6.03 | 7.11 | 10.92 | 0 |
| `BenchmarkPickDone/p2c_ewma/16` | 26.09 | 30.59 | 34.46 | 0 |
| `BenchmarkPickDone/ring_hash/100` | 19.05 | 22.42 | 25.65 | 0 |
| `BenchmarkPickDoneDegraded/round_robin/16` | 13.14 | 18.73 | 29.52 | 0 |
| `BenchmarkPickDoneDegraded/round_robin_weighted/16` | 18.38 | 22.48 | 67.31 | 0 |
| `BenchmarkPickDoneDegraded/least_conn/16` | 53.58 | 100.9 | 166.0 | 0 |
| `BenchmarkPickDoneDegraded/p2c_ewma/16` | 63.55 | 120.7 | 273.8 | 0 |
| `BenchmarkPickDoneDegraded/ring_hash/100` | 43.86 | 51.01 | 79.97 | 0 |
| `BenchmarkCounterInc` | 2.28 | 2.43 | 5.47 | 0 |
| `BenchmarkCounterIncParallel` | 13.45 | 18.35 | 20.30 | 0 |
| `BenchmarkGaugeAdd` | 2.17 | 2.25 | 2.84 | 0 |
| `BenchmarkHistogramObserve` | 6.70 | 7.93 | 10.01 | 0 |
| `BenchmarkHistogramObserveParallel` | 52.21 | 57.75 | 62.09 | 0 |
| `BenchmarkWriteTo` (one scrape, 25,032 B/op) | 10,070 | 11,650 | 19,290 | 14 |
| `BenchmarkHandler` (golb, 4 backends, 7.4 kB/op) | 12,180 | 14,030 | 29,480 | 83 |
| `BenchmarkHandlerAccessLog` | 12,790 | 15,660 | 26,250 | 84 |
| `BenchmarkBareReverseProxy` (1 backend, 40 kB/op) | 32,720 | 36,180 | 40,780 | 79-80 |
| `BenchmarkRouterMatch` | 60.88 | 64.74 | 71.15 | 0 |

"Degraded" is a retry: one backend excluded, a quarter of the pool
unavailable. `least_conn` and `p2c_ewma` are several times slower there than
before the fix round (medians 15.8 and 50.1 ns at `6414ef7`); there is no PRD
target for this case, and the samples spread by 3-4x, so the size of the
change is uncertain.

Scaling from 1 to 11 procs:

```sh
go test -run '^$' -bench 'PickDoneParallel' -cpu 1,11 -count=5 ./internal/balancer
```

Load average 53.6 before, 54.4 after (noisy).

| Benchmark (`b.RunParallel`) | 1 proc min / median / max | 11 procs min / median / max | median ratio |
|---|---|---|---|
| `round_robin/16` | 3.97 / 4.33 / 8.34 | 41.6 / 51.2 / 54.3 | 11.8x |
| `round_robin_weighted/16` | 4.00 / 5.78 / 12.95 | 45.6 / 52.6 / 54.4 | 9.1x |
| `least_conn/16` | 4.53 / 5.79 / 9.98 | 52.9 / 55.0 / 56.1 | 9.5x |
| `p2c_ewma/16` | 27.6 / 33.9 / 45.0 | 27.0 / 29.8 / 34.8 | 0.9x |
| `ring_hash/100` | 20.0 / 25.4 / 28.4 | 10.4 / 10.9 / 11.6 | 0.4x |

Caveats, unchanged since the first round:

- The pick benchmarks call `Balancer.Pick` and `Backend.Acquire`/`Release`,
  not `Pool.Done` (latency sample, outlier detection). The PRD's
  "`Pick`+`Done`" targets are therefore only partly measured.
- The handler comparison does not use a stub transport as the PRD specifies,
  and golb has four backends where the bare proxy has one. The bare proxy
  also has no buffer pool (hence 40 kB/op). The time comparison is not like
  for like; the allocation counts are comparable.
- At 11 procs `round_robin` and `least_conn` cost about 50 ns per pick
  because every goroutine writes the same counters. One request through the
  handler costs about 14,000 ns.

## 3. End-to-end

Load generator: `golb-bench load` at `9fb2267`. Since the fix round its
latency figures include failed requests, fixed-rate latency runs from the
intended send time, and it reports offered and successful ("ok", status
below 400) req/s separately. Checks made on the earlier version (percentiles
against a backend with a known delay, agreement with `ab`, request count
equal to golb's own counter, generator headroom) were not repeated on this
version. No run below had a single error or non-2xx response.

Closed-loop latencies are service times seen by a client that waits for each
response; use the fixed-rate rows for latency.

### 3.1 Direct versus through golb, 64 connections

```sh
CONNS_LIST=64 test/bench/e2e.sh OUT_DIR
```

Four echo backends in one process, golb `round_robin`, access log off, a
fresh golb per row, 3 runs of 15 s after 3 s warm-up; median run by ok req/s
with the min-max of the three. Load average 23.3 at the start, 56.0 after the
1 KiB closed-loop rows, 51.0 after the 10 KiB ones, 38.0 at the end (noisy).

| response | target | ok req/s median | min - max | p50 | p99 | p99.9 | golb/direct | golb CPU (cores) | golb peak RSS (MiB) |
|---|---|---|---|---|---|---|---|---|---|
| 1 KiB | direct | 106,630 | 101,291 - 110,926 | 434 µs | 3.31 ms | 12.32 ms | | | |
| 1 KiB | golb | 35,988 | 35,433 - 38,181 | 1.23 ms | 9.37 ms | 29.23 ms | 34 % | 2.50 | 27 |
| 10 KiB | direct | 66,558 | 61,753 - 74,180 | 668 µs | 6.26 ms | 19.92 ms | | | |
| 10 KiB | golb | 29,591 | 23,299 - 30,955 | 1.62 ms | 10.62 ms | 29.62 ms | 44 % | 2.63 | 28 |

"Direct" is the ceiling of this setup (generator plus echo server), not the
capacity of a backend. A separate 4-second single run of `scripts/bench.sh`
at load average about 20-40 gave golb `round_robin` 38,994 and direct 93,172
req/s (42 %); at `6414ef7` the 15-second figure was 42,768 (38,560-43,634).
The data does not show whether the fix round changed throughput: the
differences are inside the run-to-run spread on this machine.

### 3.2 Added latency at a fixed rate (50 % of golb's closed-loop median)

Same script and conditions; 64 connections. Median run, then the three runs.

| response | rate | target | p50 | p90 | p99 | p99.9 | p50 per run | p99 per run |
|---|---|---|---|---|---|---|---|---|
| 1 KiB | 17,994 req/s | direct | 92 µs | 211 µs | 19.01 ms | 77.59 ms | 107 / 92 / 97 µs | 43.0 / 19.0 / 39.6 ms |
| 1 KiB | 17,994 req/s | golb | 223 µs | 1.38 ms | 44.30 ms | 69.21 ms | 218 / 223 / 214 µs | 16.7 / 44.3 / 11.2 ms |
| 10 KiB | 14,795 req/s | direct | 103 µs | 293 µs | 11.14 ms | 26.48 ms | 110 / 103 / 106 µs | 36.2 / 11.1 / 28.8 ms |
| 10 KiB | 14,795 req/s | golb | 248 µs | 1.58 ms | 48.76 ms | 78.64 ms | 260 / 248 / 251 µs | 71.3 / 48.8 / 20.1 ms |

Added by golb (golb minus direct, median runs): p50 +131 µs at 1 KiB and
+145 µs at 10 KiB, stable across runs (+107 to +157 µs). p99 +25 ms and
+38 ms, p99.9 -8 ms and +52 ms: but the direct path's own p99 moved between
11 and 43 ms from run to run, so the tail added by golb cannot be separated
from machine noise here. The second-scale delays in the preliminary document
did not recur.

### 3.3 One slow backend (PRD B4)

Three fast echo backends and one with a 200 ms delay; both policies at the
same fixed rate, half of what `round_robin` sustains closed loop with the
slow backend in the pool (1,263 req/s, range 1,257-1,273). 64 connections,
3 runs of 15 s after 3 s warm-up. Load average 38.0 before, 19.1 after
(noisy).

```sh
golb-bench echo -addr 127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003 -size 1024
golb-bench echo -addr 127.0.0.1:19005 -size 1024 -delay 200ms
golb -config CONFIG          # pool of the four addresses above, policy round_robin or p2c_ewma
golb-bench load -url http://127.0.0.1:18080/ -c 64 -rate 631 -d 15s -warmup 3s -runs 3
```

| policy at 631 req/s | p50 | p90 | p99 | p99.9 | p99 per run |
|---|---|---|---|---|---|
| `round_robin` | 17.43 ms | 301.99 ms | 692.06 ms | 1.38 s | 501 / 692 / 419 ms |
| `p2c_ewma` | 532 µs | 1.04 ms | 6.91 ms | 202.38 ms | 17.4 / 6.9 / 9.8 ms |

`scripts/bench.sh` now contains the same comparison; its 4-second trial gave
p99 512 ms against 14 ms at 640 req/s.

### 3.4 Other rows from `scripts/bench.sh` (4-second trial, one run each)

```sh
DURATION=4s WARMUP=1s RUNS=1 scripts/bench.sh
```

Run to look for the errors reported in the "access log on" row, not as a
measurement: one 4-second run per row has no spread. Load average about
54 at the start, 19.5 at the end (noisy). Errors 0 and non-2xx 0 in every row.

| scenario | ok req/s | p50 | p99 |
|---|---|---|---|
| direct to one backend | 93,172 | 434 µs | 4.78 ms |
| golb `round_robin` | 38,994 | 1.11 ms | 9.11 ms |
| golb `p2c_ewma` | 27,806 | 1.21 ms | 21.10 ms |
| golb `least_conn` | 38,700 | 1.10 ms | 8.39 ms |
| golb `ring_hash` | 34,234 | 1.21 ms | 11.21 ms |
| golb `round_robin`, access log on | 43,317 | 1.11 ms | 6.03 ms |
| golb `round_robin`, SIGHUP reload every 1 s | 21,744 | 1.35 ms | 29.62 ms |
| bare `ReverseProxy`, idle pool 64 per host | 16,911 | 2.70 ms | 27.79 ms |
| nginx, HAProxy, Caddy, Traefik, Envoy | not measured (not installed) | | |

Access log on, measured separately (golb `round_robin`, access log to a
file, 64 connections, 2 runs of 15 s after 3 s warm-up, load average 13.0
before and 28.1 after): 32,502 and 46,826 ok req/s, 0 errors in 1,189,927
measured requests, `golb_accesslog_dropped_total` 0, one log line per request.

## 4. PRD targets versus measured

All verdicts are on noisy runs.

| Target | Measured | Verdict |
|---|---|---|
| Pick `round_robin`, 16 backends: at most 25 ns, 0 allocs | median 4.99 ns (4.60-5.19), 0 allocs | met (`Done` not included) |
| Pick `least_conn`, 16 backends: at most 25 ns, 0 allocs | median 7.11 ns (6.03-10.92), 0 allocs | met (`Done` not included) |
| Pick `p2c_ewma`, 16 backends: at most 40 ns, 0 allocs | median 30.59 ns (26.09-34.46), 0 allocs | met (`Done` not included) |
| Pick `ring_hash`, 100 backends: at most 80 ns, 0 allocs | median 22.42 ns (19.05-25.65), 0 allocs | met (`Done` not included) |
| Any policy under `RunParallel` at 11 procs at most 3x the 1-proc time | `round_robin` 11.8x, weighted 9.1x, `least_conn` 9.5x, `p2c_ewma` 0.9x, `ring_hash` 0.4x | **missed** for `round_robin` and `least_conn` |
| `Counter.Inc`, `Histogram.Observe`: 0 allocs | 0 allocs | met |
| Handler versus bare `ReverseProxy`: at most +5 allocs/op | 83 versus 79-80 | met |
| Handler versus bare `ReverseProxy`: at most +15 % ns/op | 14,030 versus 36,180 ns | met as measured; not like for like (section 2) |
| Throughput at least 50 % of direct (1 KiB, 64 connections) | 34 % (35,988 / 106,630) | **missed** |
| Throughput at least 40,000 req/s | 35,988 (35,433-38,181) | **missed** on this run; 38,994 and 42,768 in other runs |
| Added latency at 50 % load: p50 at most 150 µs | +131 µs (1 KiB), +145 µs (10 KiB) | met, by a small margin |
| Added latency at 50 % load: p99 at most 1 ms | +25 ms, with a direct p99 that itself varies by 30 ms | not measurable on this machine; no evidence that it is met |
| Throughput at least equal to bare `ReverseProxy` | 38,994 versus 16,911 req/s (one 4 s run each) | met on the available data |
| Access log on: throughput loss at most 10 % | 43,317 versus 38,994 (4 s runs); 32,502 and 46,826 in two 15 s runs | no loss visible; spread larger than 10 % |
| Reload every second: p99 within 10 % of the no-reload run | 29.62 ms versus 9.11 ms (one 4 s run each) | **missed** on the available data; needs a proper run |
| Reload every second: 0 errors | 0 errors | met |
| B4: p99 with `p2c_ewma` at most 0.5x `round_robin`, one slow backend | 6.91 ms versus 692 ms (0.01x); worst `p2c_ewma` run 17.4 ms against best `round_robin` run 419 ms | met |
| Comparison with nginx, HAProxy, Caddy, Traefik, Envoy | | not measured |

## 5. Limitations

- **Noisy machine.** Load average 19 to 56 on 11 CPUs in every block, from
  unrelated work. golb used 1.8 to 2.6 cores and was competing for them. The
  throughput targets and the p99 target cannot be judged reliably until the
  runs are repeated with the load average below about 15.
- **Loopback, one machine.** Generator and backends take cores from golb;
  there is no real network. Mixed fast and slow cores add spread.
- **No competitor comparison.** None of nginx, HAProxy, Caddy, Traefik or
  Envoy is installed, none was measured, and this document says nothing
  about golb relative to them. The only other proxy measured is a bare Go
  `httputil.ReverseProxy`.
- **Narrow workload.** HTTP/1.1 keep-alive `GET`, 1 KiB and 10 KiB
  responses, 64 connections, four backends in one process. Not measured:
  other connection counts on this commit, HTTP/2, TLS, uploads, WebSocket,
  the TCP proxy, connection churn, memory per idle connection, long runs.
  Chunked HTTP/1 uploads now close their client connection after each
  request (the fix for requests carrying both Content-Length and
  Transfer-Encoding); the cost of that for upload-heavy clients was not
  measured.
- **Three runs per scenario**, and section 3.4 has one short run per row.
- **CPU and RSS** come from `ps` (CPU time over the scenario's wall time
  including warm-up; highest RSS sampled every 2 s).
- **Micro-benchmark caveats** in section 2.
- **Load generator** checks were made on the previous version only
  (section 3).

### Reproducing against competitors

Install any of nginx, HAProxy, Caddy, Traefik or Envoy so the binary is on
`PATH`, then run `scripts/bench.sh > bench-report.md`. It starts each proxy
it finds in front of the same four echo backends (round robin, keep-alive,
access log off) and prints its rows beside golb's, in the closed-loop table
only; rows for proxies it does not find say "not measured". The competitor
configurations in the script have never been run: treat the first run of
each as a test of the configuration, check that the proxy reuses upstream
connections and reports no errors, and record its version. Use a quiet
machine, preferably with generator and backends on other hosts, and repeat
the script several times before drawing any conclusion.
