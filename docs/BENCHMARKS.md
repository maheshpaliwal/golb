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

## 6. After optimisation (2026-11-21)

Measured on the working tree on top of `821bf20`, same machine and Go version
as section 1. Every run is noisy: the 1-minute load average was between 17
and 78 (quoted per block). "Before" is a binary built from `821bf20`, "after"
the working tree; the two were run back to back in every round, each on a
fresh process, after a direct-to-backend run. 64 connections, 1 KiB responses,
`round_robin`, access log off, 2 s warm-up, unless stated. The harness was a
throwaway script around `golb-bench echo` and `golb-bench load` with the
configuration of `test/bench/e2e.sh`; it is not in the repository.

CPU seconds per 100,000 requests is golb's CPU time from `ps` divided by
`golb_requests_total`, warm-up included. It moved by 2-3 % between rounds
while req/s moved by up to 60 %, so it is the figure to compare.

### 6.1 What limits the comparison with "direct"

- golb is not short of cores because of the generator or the backends. In the
  golb rows golb used 2.1 to 3.5 cores, the echo backends 1.0 to 1.6 and the
  generator 0.8 to 1.3, on 11 CPUs that other work kept busy. In the direct
  rows the same backends and generator used about 2.7 and 2.2 cores, so
  neither was at its limit when driving golb.
- A direct request costs about 4.4 CPU-seconds per 100,000 (backend 2.4-2.5,
  generator 1.9-2.0). Through golb the backend and generator cost about 5.0
  (2.8 + 2.2) and golb 6.9 before, 5.9 after. On one shared machine the
  "50 % of direct" target therefore needs golb to cost about 3.5 or less,
  which is close to the kernel time of its four socket operations alone
  (2.8-3.1, below). The ratio measures how three processes share one
  machine as much as it measures the proxy.

### 6.2 Where the time goes

OS accounting (`ps` user and total CPU time, one 8 s run each, load average
17-29):

| | CPU-s per 100k | user | system |
|---|---|---|---|
| before | 6.93 | 3.92 (57 %) | 3.01 |
| after | 5.87 | 3.12 (53 %) | 2.76 |

CPU profile (`/debug/pprof/profile` on the admin listener, `admin.pprof:
true`, 8 s under load; 3 profiles before, 5 after), cumulative share:

| | before | after |
|---|---|---|
| `syscall.read` (sockets) | 43.0 % | 44.7 % |
| `syscall.write` (sockets) | 41.8 % | 45.4 % |
| `runtime.schedule` | 6.8 % | 5.6 % |
| `runtime.kevent` | 3.1 % | 2.8 % |
| `runtime.usleep` (idle spinning) | 2.1 % | 1.3 % |
| GC mark workers and assists | 2.2 % | 0.6 % |
| `runtime.mallocgc` | 1.4 % | 0.7 % |
| `proxy.Handler.ServeHTTP`, including `ReverseProxy` and the retry transport, excluding I/O | 1.3 % | 1.1 % |

**The profile is not trustworthy for ranking.** It puts 85 % of the samples
in socket system calls, while the kernel's own accounting gives 43 % system
time; on this platform the Go profiler over-counts system calls. The
user-space costs were therefore ranked by experiment (CPU-s per 100k, the
"before" binary with runtime settings changed through the environment,
3 rounds of 6 s, load average 28-78):

| setting | round 1 / 2 / 3 | versus default |
|---|---|---|
| default | 6.96 / 6.80 / 6.88, then 6.89 / 6.85 / 7.15 | |
| `GOGC=200` | 5.93 / 6.11 / 6.35 | -12 % |
| `GOGC=400` | 5.56 / 5.81 / 5.59 | -19 % |
| `GOGC=800` | 5.54 / 5.64 / 5.76 | -18 % |
| `GOGC=off GOMEMLIMIT=256MiB` | 5.27 / 5.48 / 5.40 | -22 % |
| `GOMAXPROCS=4` | 5.64 / 6.08 / 5.74 | -17 % |
| `GOMAXPROCS=4`, `GOGC=off GOMEMLIMIT=256MiB` | 5.54 / 5.60 / 5.61 | -19 % |

So, of golb's CPU per request before the change: about 43 % kernel socket
I/O, about 20 % garbage collection (`GODEBUG=gctrace=1` counted 1,233
collections in a 10 s run, heap goal 8 MiB), and the rest `net/http` server,
`http.Transport`, scheduler and allocation. `ReverseProxy`'s own work costs
2.2 to 4.4 µs per request in a benchmark with a stub transport (26 allocs),
about 4 % of the 69 µs; golb's own code and the balancer pick (about 50 ns)
are below 1 %. Allocation: 7.3 kB and 81 objects per request before, 6.9 kB
and 80 after (unchanged within sampling); the largest sites are
`textproto.readMIMEHeader` (13 %), `Header.Clone` (12 %), `Request.Clone`
(11.5 % cumulative).

### 6.3 Change kept

`cmd/golb/gc.go`: unless `GOGC` is set, golb lets the heap grow by about
16 MiB between collections while less than 16 MiB is alive (GOGC between 100
and 400, adjusted once a second from the live heap), and uses the stock 100
above that. Collections in a 10 s run went from 1,233 to 209. Nothing in
`internal/proxy` changed.

5 rounds of 10 s; load average 24 at the start, 33 at the end.

| 1 KiB, 64 connections | direct | before | after |
|---|---|---|---|
| ok req/s, median (min - max) | 105,804 (91,313 - 123,243) | 45,918 (30,720 - 50,442) | 50,729 (34,626 - 54,996) |
| golb / direct (medians) | | 43 % | 48 % |
| golb CPU-s per 100k, median (min - max) | | 6.96 (6.93 - 7.07) | 5.89 (5.79 - 5.99) |
| closed-loop p50, median of rounds | 424 µs | 1.02 ms | 918 µs |
| closed-loop p99, median of rounds | 3.59 ms | 6.62 ms | 6.52 ms |
| golb peak RSS (MiB) | | 26 - 28 | 43 - 62, median 46 |

The req/s ranges overlap, so by this document's rule the throughput gain is
not established; round by round "after" was ahead in 4 of 5 (+52 %, +12 %,
-17 %, +13 %, +1 %). The CPU ranges do not overlap: -15 %. An earlier
3-round baseline of the same "before" binary (load average 25-56) gave
39,380 (35,612 - 42,615) against direct 119,153, 33 %, and 6.74 CPU-s per
100k.

Other workloads, 3 rounds of 10 s each (load average 27-49):

| workload | | before | after |
|---|---|---|---|
| 256 connections, 1 KiB | ok req/s | 44,736 (38,812 - 49,835) | 46,895 (35,677 - 53,469) |
| | CPU-s per 100k | 6.51 (6.43 - 6.72) | 6.24 (6.09 - 6.50) |
| | peak RSS (MiB) | 50 | 77 - 97 |
| 64 connections, 10 KiB | ok req/s | 33,801 (29,935 - 35,261) | 35,918 (34,971 - 36,881) |
| | CPU-s per 100k | 9.52 (9.42 - 9.55) | 8.37 (8.35 - 8.39) |
| | peak RSS (MiB) | 28 - 32 | 43 - 45 |
| fixed 20,000 req/s, 1 KiB | p50 (direct 68 / 91 / 103 µs) | 283 / 232 / 243 µs | 211 / 239 / 228 µs |
| | added p50, per round | +215 / +141 / +139 µs | +143 / +147 / +125 µs |
| | p99 (direct 41.4 / 3.0 / 46.7 ms) | 65.8 / 6.7 / 44.3 ms | 42.5 / 98.0 / 9.2 ms |
| | CPU-s per 100k | 9.02 / 9.58 / 10.32 | 7.08 / 8.99 / 9.93 |

At 256 connections more memory is alive, the stock setting already collects
less often, and the gain is small (ranges overlap) for 27-47 MiB more
memory; with `GOGC=400` one 3-round trial gave 5.60 / 5.91 / 5.65 against
6.59 / 6.26 / 6.66 at about 105 MiB. Added p50 at a fixed rate is unchanged
(median +141 µs before, +143 µs after) and p99 remains unmeasurable on this
machine. Upstream connections stayed at 83-86 (64 connections) and 290-296
(256 connections) through a run, so the default idle pool causes no dial
churn.

`go test -run '^$' -bench=Handler -benchmem -count=5 ./internal/proxy/`
(code under test unchanged; load average 29 before the first run, 17 before
the second):

| | before, median (min - max) | after, median (min - max) | allocs/op |
|---|---|---|---|
| `BenchmarkHandler` ns/op | 19,792 (11,817 - 30,062) | 16,218 (12,478 - 16,679) | 83 / 83 |
| `BenchmarkHandlerAccessLog` ns/op | 17,241 (13,810 - 22,009) | 16,316 (14,823 - 26,636) | 84 / 84 |

### 6.4 Tried and not kept

| candidate | measurement | outcome |
|---|---|---|
| Replace `ReverseProxy` with a direct `RoundTrip` and copy | stub-transport benchmark, 1 proc: `ReverseProxy` 2,179 - 4,383 ns, 26 allocs, 2,288 B; a bare replacement without 1xx, trailer or `Connection`-token handling 832 - 1,044 ns, 16 allocs, 1,600 B | at most about 1.4 µs of 59 µs per request (2 %), inside the noise, for re-implementing the upgrade, 1xx and trailer paths; not built |
| Transport `ReadBufferSize`/`WriteBufferSize` 32 KiB and 64 KiB | 10 KiB: 8.38 / 8.57 / 8.54 CPU-s per 100k at the default 4 KiB, 9.05 / 9.10 / 9.34 at 32 KiB, 8.82 / 9.11 / 9.28 at 64 KiB; 1 KiB: 5.91 / 5.73 / 5.57 against 5.72 / 5.69 / 5.71 | worse or no change; reverted |
| 32 MiB floor (GOGC up to 800) instead of 16 MiB | 5.57 / 5.72 / 5.47 / 5.48 against 5.72 / 6.12 / 5.82 / 5.77; peak RSS 62 - 108 MiB against 39 - 42 | about 3 % more for twice the memory and one overshoot; not kept |
| `GOMAXPROCS` 4, 6, 8 with the change in | CPU-s per 100k medians 5.47, 5.60, 5.77 against 5.72; req/s medians 46,547, 52,326, 40,671 against 47,518 (5 rounds) | no consistent effect; no default, no recommendation |
| `MaxIdleConnsPerHost`, `MaxConnsPerHost` | connection counts above | no churn to fix |
| Per-goroutine start for `round_robin`/`least_conn`, fewer clock reads, timer reuse, in-place header mutation | about 50 ns of 59,000 ns for the pick; the rest are part of the 4 % above | not attempted |

### 6.5 Targets after the change

| Target | Measured | Verdict |
|---|---|---|
| Throughput at least 40,000 req/s | 50,729 (34,626 - 54,996) | met on the median, one round of five below |
| Throughput at least 50 % of direct | 48 % (medians) | **missed**; see 6.1 |
| Added p50 at 50 % load at most 150 µs | +143 µs (+125 to +147) | met, by a small margin |
| Added p99 at most 1 ms | not separable from noise | no evidence that it is met |
| Handler at most +5 allocs/op over bare `ReverseProxy` | 83, unchanged | met |

What remains is the cost of `net/http` on both sides of the proxy plus the
kernel: about 2.8 CPU-s per 100k in socket system calls and about 3.1 in
user space, of which golb's own layer is a few percent. The Go profiler
cannot rank that user-space remainder on this platform; profiling on Linux
is the next step before any further change.
