# golb benchmarks (v0.1) — PRELIMINARY, NOISY

**Status: preliminary.** These numbers were taken on a machine under heavy
unrelated load and with benchmark tools that have known defects (section
3.5). They are recorded so the first measurement is not lost; all end-to-end
figures are to be re-measured after the next fix round, on a quiet machine.
Do not quote them.

Measured by QA on 2026-11-21 against commit `6414ef7`. Every number below was
produced by the commands shown, on the machine described, and is reported as
measured. Targets come from `docs/PRD.md` section 3.2 and table 1.1.

**Read section 1 before using any number.** The machine was shared and
heavily loaded during every run, so absolute values are pessimistic and the
run-to-run spread is wide. Ratios between rows of the same table are more
trustworthy than the rows themselves, and a target missed by a small margin
here may be met on a quiet machine (and the reverse).

## 1. Conditions

| | |
|---|---|
| Machine | Apple M3 Pro, 11 logical CPUs (5 performance + 6 efficiency cores), 18 GB RAM |
| OS | macOS 15.5 (Darwin 24.5.0), arm64 |
| Go | go1.25.6 darwin/arm64, `GOMAXPROCS` unset (11) |
| Commit | `6414ef7` (local baseline, uncommitted work: `test/` and this file only) |
| Network | loopback only; load generator, golb and backends share the machine |
| Limits | `ulimit -n` 1048576; ephemeral ports 49152-65535; `kern.ipc.somaxconn` 128 |

Background load: the 1-minute load average was between 18
and 132 on 11 CPUs throughout, and rose over the session (sampled with
`uptime` before and after each block; the values are quoted with each
table). Other work on the machine at the time included two
concurrent code reviews, two QEMU virtual machines and an endpoint-security
agent that alone used about 3 cores. None of that could be stopped. The
efficiency cores are also slower than the performance cores, and macOS
decides where each process runs.

## 2. Micro-benchmarks

Command (PRD section 6), run once per package, five samples each:

```sh
go test -run '^$' -bench=. -benchmem -count=5 ./internal/balancer
go test -run '^$' -bench=. -benchmem -count=5 ./internal/metrics
go test -run '^$' -bench=. -benchmem -count=5 ./internal/proxy
```

`benchstat` is not installed; minimum, median and maximum of the five samples
are given instead. Load average 42.8 before, 55.5 after.

### 2.1 Balancer: pick plus in-flight bookkeeping

| Benchmark (11 procs) | ns/op min | median | max | allocs/op |
|---|---|---|---|---|
| `BenchmarkPickDone/round_robin/16` | 12.26 | 16.29 | 26.09 | 0 |
| `BenchmarkPickDone/round_robin_weighted/16` | 7.29 | 8.77 | 10.67 | 0 |
| `BenchmarkPickDone/least_conn/16` | 10.34 | 15.37 | 17.57 | 0 |
| `BenchmarkPickDone/p2c_ewma/16` | 32.58 | 38.00 | 45.90 | 0 |
| `BenchmarkPickDone/ring_hash/100` | 29.82 | 33.03 | 50.58 | 0 |
| `BenchmarkPickDoneDegraded/round_robin/16` | 11.38 | 12.85 | 16.22 | 0 |
| `BenchmarkPickDoneDegraded/round_robin_weighted/16` | 18.29 | 18.62 | 21.94 | 0 |
| `BenchmarkPickDoneDegraded/least_conn/16` | 13.59 | 15.83 | 22.40 | 0 |
| `BenchmarkPickDoneDegraded/p2c_ewma/16` | 39.48 | 50.08 | 67.89 | 0 |
| `BenchmarkPickDoneDegraded/ring_hash/100` | 30.38 | 36.17 | 44.05 | 0 |

"Degraded" is a retry: one backend excluded and a quarter of the pool
unavailable.

What these benchmarks measure: `Balancer.Pick` followed by
`Backend.Acquire`/`Release`. They do not call `Pool.Done`, which in the real
pool also records latency and feeds the outlier detector, so the name
overstates the coverage and the PRD's "`Pick`+`Done`" target is only partly
measured.

### 2.2 Balancer: scaling from 1 to 11 procs

```sh
go test -run '^$' -bench 'PickDoneParallel|IncParallel|ObserveParallel' -benchmem -cpu 1,11 -count=5 ./internal/balancer ./internal/metrics
```

Load average 30.7 before, 67.6 after. ns/op as min / median / max.

| Benchmark (`b.RunParallel`) | 1 proc | 11 procs | median ratio |
|---|---|---|---|
| `BenchmarkPickDoneParallel/round_robin/16` | 4.34 / 4.98 / 6.97 | 42.9 / 52.4 / 57.3 | 10.5x |
| `BenchmarkPickDoneParallel/round_robin_weighted/16` | 4.65 / 4.86 / 5.53 | 37.8 / 51.2 / 54.1 | 10.5x |
| `BenchmarkPickDoneParallel/least_conn/16` | 6.14 / 7.53 / 10.6 | 44.7 / 46.6 / 51.2 | 6.2x |
| `BenchmarkPickDoneParallel/p2c_ewma/16` | 27.9 / 30.4 / 33.7 | 22.3 / 22.6 / 27.4 | 0.7x |
| `BenchmarkPickDoneParallel/ring_hash/100` | 17.3 / 17.9 / 21.0 | 9.67 / 12.3 / 24.1 | 0.7x |
| `BenchmarkCounterIncParallel` | 2.03 / 2.19 / 2.55 | 32.6 / 33.3 / 34.4 | 15.2x |
| `BenchmarkHistogramObserveParallel` | 8.35 / 8.52 / 11.8 | 46.6 / 53.9 / 61.2 | 6.3x |

`round_robin` and `least_conn` slow down by more than 3x when every core does
nothing but pick: all goroutines write the same counter (the rotation
counter, and each backend's in-flight count). `p2c_ewma` and `ring_hash`
spread their writes and do not. A single shared metrics counter behaves the
same way. Whether this matters end to end is a separate question: in the
handler benchmark below one request costs about 16,000 ns, so 50 ns is 0.3 %
of it.

A single `-cpu 1,4,11` sweep (the PRD's command, one sample per cell, load
average 55 before and 35 after) gave the same picture and is not repeated
here.

### 2.3 Metrics

| Benchmark (11 procs) | ns/op min | median | max | B/op | allocs/op |
|---|---|---|---|---|---|
| `BenchmarkCounterInc` | 2.74 | 2.94 | 6.18 | 0 | 0 |
| `BenchmarkCounterIncParallel` | 11.80 | 16.04 | 16.41 | 0 | 0 |
| `BenchmarkGaugeAdd` | 2.65 | 2.74 | 2.98 | 0 | 0 |
| `BenchmarkHistogramObserve` | 6.88 | 7.77 | 8.94 | 0 | 0 |
| `BenchmarkHistogramObserveParallel` | 46.18 | 56.01 | 60.14 | 0 | 0 |
| `BenchmarkWriteTo` (one scrape) | 8,358 | 9,078 | 11,246 | 25,032 | 14 |

### 2.4 HTTP handler

| Benchmark (11 procs) | ns/op min | median | max | B/op | allocs/op |
|---|---|---|---|---|---|
| `BenchmarkHandler` (golb, 4 backends) | 14,292 | 16,099 | 19,229 | 7,023-7,345 | 83 |
| `BenchmarkHandlerAccessLog` | 18,679 | 21,500 | 28,755 | 7,133-7,777 | 84 |
| `BenchmarkBareReverseProxy` (1 backend) | 32,734 | 39,633 | 41,401 | 40,058-40,105 | 79-80 |
| `BenchmarkRouterMatch` | 60.55 | 69.99 | 88.69 | 0 | 0 |

Two things limit what the golb-versus-bare comparison shows:

- The PRD asks for "a stub transport ... in the same harness". Both
  benchmarks instead use real `httptest` backends over loopback, and golb is
  given four of them while the bare proxy is given one. Part of golb's lower
  time per request may come from having four backend accept loops to spread
  over, not from golb itself.
- The bare proxy has no `BufferPool`, so it allocates a 32 KiB copy buffer per
  request (the 40 kB/op). That is what a stock `ReverseProxy` does, so it is
  a fair description of the baseline, but most of the gap is this one
  setting.

The allocation counts are not affected by either point: golb adds 3 to 4
allocations per request over the bare proxy, and 1 more with the access log.

## 3. End-to-end

### 3.1 Is the load generator trustworthy?

`golb-bench load` was checked before any result was taken from it.

| Check | Command (after `go build -o bin/golb-bench ./cmd/golb-bench`) | Expected | Observed |
|---|---|---|---|
| Latency percentiles against a backend with a known 20 ms delay, 8 connections | `bin/golb-bench echo -addr 127.0.0.1:19005 -delay 20ms` and `bin/golb-bench load -url http://127.0.0.1:19005/ -c 8 -d 5s -warmup 1s` | p50 just above 20 ms; req/s = 8 / mean latency | p50 21.89 ms, p99 27.79 ms, mean 21.98 ms, 363.2 req/s (8 / 0.02198 = 364) |
| Same target with ApacheBench | `ab -k -c 8 -n 2000 http://127.0.0.1:19005/` | agreement with the row above | 364.2 req/s, p50 22 ms, p99 32 ms |
| Fixed rate below capacity | `... load ... -c 8 -rate 200 -d 5s -warmup 1s` | 200 req/s, p50 about 20 ms | 200.0 req/s, p50 20.45 ms |
| Fixed rate above capacity (1000 req/s offered, about 390 possible) | `... load ... -c 8 -rate 1000 -d 5s -warmup 1s -timeout 30s` | queueing delay visible in the percentiles | p50 6.01 s, p99 10.20 s: queueing is counted (no coordinated omission). **But "req/s" printed 1000.0**, see below |
| Request count against golb's own counter | `... load -url http://127.0.0.1:18080/ -c 64 -d 5s -warmup 0s`, then `curl -s 127.0.0.1:19901/metrics` | equal | 162,517 reported, `golb_requests_total{code_class="2xx"}` = 162,517 |
| Generator headroom, 64 connections, 1 KiB | CPU of the `load` process (`ps -o pcpu`) direct and through golb | generator not saturated when golb is in the path | direct: 89,655 req/s with the generator at about 1.5 cores; through golb: 39,231 req/s with the generator at about 0.9 cores |

Conclusions:

- Percentiles and request counts are computed correctly; an independent tool
  agrees on the simple case.
- The generator is not the bottleneck for any through-golb number: against
  the bare backend it sustains more than twice the rate golb reaches.
  The **direct** rows are a different matter: there the generator and the
  echo server together are the limit, so "direct" is the ceiling of this
  test setup, not the capacity of a backend.
- Flaw: in fixed-rate mode, "req/s" is the number of requests *scheduled* in
  the window divided by the window, not the rate achieved. When the target
  cannot keep up, the run overruns its window and still prints the offered
  rate. The fixed-rate tables below run at half of the measured maximum,
  where both are the same; do not use fixed-rate "req/s" as a throughput
  measurement.
- Closed-loop latencies (sections 3.2 and 3.4) are service times as seen by a
  client that waits for each response before sending the next. They
  understate what an open stream of arrivals would see under overload. The
  fixed-rate tables (3.3) do not have that problem.
- Latency values are rounded up to a histogram bucket at most 0.8 % wide.
- The generator speaks HTTP/1.1 keep-alive `GET` only. HTTP/2, TLS, request
  bodies and connection churn were not load-tested.

### 3.2 Closed loop: direct to a backend versus through golb

```sh
test/bench/e2e.sh OUT_DIR      # writes OUT_DIR/results.md and OUT_DIR/raw/*.json
```

Four echo backends in one process, golb with `round_robin`, access log off,
a fresh golb process per row, 3 runs of 15 s after 3 s warm-up. The median
run by req/s is shown with the min-max of the three. "Direct" is the load
generator against one echo listener. Load average 67.6 at the start of the
script, 98.4 at the end. No request failed in any run (errors 0, non-2xx 0).

1 KiB responses:

| connections | target | req/s median | req/s min - max | p50 | p99 | p99.9 | golb/direct | golb CPU (cores) | golb peak RSS (MiB) |
|---|---|---|---|---|---|---|---|---|---|
| 16 | direct | 74,099 | 73,126 - 74,381 | 160 µs | 1.13 ms | 4.55 ms | | | |
| 16 | golb | 17,874 | 17,553 - 24,798 | 473 µs | 6.78 ms | 40.11 ms | 24 % | 1.68 | 19 |
| 64 | direct | 92,675 | 63,164 - 107,212 | 465 µs | 4.15 ms | 12.45 ms | | | |
| 64 | golb | 42,768 | 38,560 - 43,634 | 1.08 ms | 7.24 ms | 16.71 ms | 46 % | 2.86 | 28 |
| 256 | direct | 121,401 | 110,549 - 128,550 | 1.67 ms | 9.76 ms | 23.33 ms | | | |
| 256 | golb | 46,999 | 39,598 - 49,991 | 3.93 ms | 28.18 ms | 61.87 ms | 39 % | 2.80 | 51 |

10 KiB (10,240 byte) responses:

| connections | target | req/s median | req/s min - max | p50 | p99 | p99.9 | golb/direct | golb CPU (cores) | golb peak RSS (MiB) |
|---|---|---|---|---|---|---|---|---|---|
| 16 | direct | 31,341 | 27,797 - 41,872 | 260 µs | 4.26 ms | 21.89 ms | | | |
| 16 | golb | 13,242 | 8,016 - 17,667 | 643 µs | 10.55 ms | 37.75 ms | 42 % | 1.57 | 20 |
| 64 | direct | 50,173 | 38,323 - 69,153 | 696 µs | 10.94 ms | 35.65 ms | | | |
| 64 | golb | 19,478 | 18,841 - 19,639 | 1.94 ms | 23.20 ms | 90.70 ms | 39 % | 1.87 | 27 |
| 256 | direct | 38,849 | 36,967 - 52,474 | 3.33 ms | 57.67 ms | 124.26 ms | | | |
| 256 | golb | 14,525 | 11,348 - 15,431 | 10.68 ms | 116.92 ms | 168.82 ms | 37 % | 1.38 | 54 |

Variance: the three runs of one row differ by up to a factor of 2.2 (10 KiB,
16 connections, golb) and the direct baseline alone moved between 63,000 and
107,000 req/s within one row. The rows were measured one after another while
the background load rose, so later rows (10 KiB) ran on a busier machine than
earlier ones. golb never used more than 2.9 of the 11 cores: on this machine
it was limited by what the other processes left it, not by its own CPU use.

### 3.3 Latency at a fixed rate (50 % of golb's closed-loop median)

Same script, 64 connections. "Added" is golb minus direct.

| response | rate | target | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|---|
| 1 KiB | 21,383 req/s | direct | 68 µs | 178 µs | 2.02 ms | 8.85 ms |
| 1 KiB | 21,383 req/s | golb | 311 µs | 755 ms | 1.10 s | 1.17 s |
| 10 KiB | 9,739 req/s | direct | 163 µs | 4.52 ms | 64.49 ms | 111.67 ms |
| 10 KiB | 9,739 req/s | golb | 8.19 s | 9.06 s | 9.46 s | 9.87 s |

Per run, through golb at 1 KiB: p50 8.45 ms / 311 µs / 259 µs and p99
2.53 s / 1.10 s / 1.37 s. At 10 KiB: p50 104 ms / 8.19 s / 255 ms.

**These fixed-rate results are not a valid measurement of golb's added
latency.** Delays of seconds mean golb could not sustain the offered rate
for part of each run, although the rate was half of what it had sustained
minutes earlier; during the 10 KiB row golb used 0.85 cores and the load
average was near 98. A follow-up at load average 104-132 (8 s runs, 3 runs
each, rate re-derived on the spot as 6,285 req/s) did not reproduce it for
golb (p99 28-109 ms with `round_robin`, 72-575 ms with `p2c_ewma`) and did
show it for the bare `ReverseProxy` driven close to its own limit (p99
0.5-5.9 s). The likeliest explanation is that capacity on this machine moved
by more than 2x between the closed-loop run and the fixed-rate run; a stall
inside golb is not ruled out. The only parts that held in every run: p50
through golb at half load was 0.26-0.86 ms against 0.04-0.55 ms direct.
This must be re-measured on a quiet machine.

### 3.4 `scripts/bench.sh`: policies, access log, reload, baseline, slow backend

```sh
scripts/bench.sh > bench-report.md      # defaults: CONNS=64 DURATION=15s WARMUP=3s RUNS=3 BODY=1024
```

Load average 95 at the start, 101 at the end: this block ran on a machine
roughly three times busier than section 3.2 (the same "direct" scenario
gave 26,091 req/s here against 92,675 there). Compare rows within this
table only. Errors 0 and non-2xx 0 in every row.

| scenario | req/s median | req/s min - max | p50 | p99 | p99.9 |
|---|---|---|---|---|---|
| direct to one backend | 26,091 | 19,513 - 36,495 | 721 µs | 38.01 ms | 140.51 ms |
| golb `round_robin` | 16,743 | 14,892 - 21,388 | 1.79 ms | 41.42 ms | 121.11 ms |
| golb `p2c_ewma` | 14,932 | 12,216 - 15,381 | 1.79 ms | 52.43 ms | 167.77 ms |
| golb `least_conn` | 13,986 | 11,779 - 14,122 | 2.06 ms | 50.86 ms | 126.88 ms |
| golb `ring_hash` | 17,284 | 14,267 - 17,426 | 1.84 ms | 31.72 ms | 88.08 ms |
| golb `round_robin`, access log on | 17,510 | 10,785 - 22,582 | 1.87 ms | 33.16 ms | 73.92 ms |
| golb `round_robin`, SIGHUP reload every 1 s | 10,538 | 9,582 - 17,518 | 2.56 ms | 61.34 ms | 171.97 ms |
| bare `ReverseProxy`, idle pool 64 per host | 9,273 | 8,116 - 10,432 | 4.19 ms | 51.64 ms | 130.02 ms |
| nginx, HAProxy, Caddy, Traefik, Envoy | not measured (not installed) | | | | |

One slow backend (+200 ms on 1 of 4), closed loop:

| scenario | req/s median | req/s min - max | p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|---|---|
| golb `round_robin` | 1,187 | 1,100 - 1,215 | 1.62 ms | 203.42 ms | 225.44 ms | 253.76 ms |
| golb `p2c_ewma` | 10,820 | 9,709 - 17,060 | 1.75 ms | 8.91 ms | 109.58 ms | 281.02 ms |

The script's fixed-rate table (rate 8,371 req/s) is not reproduced as a
result: its golb rows show the same overload pattern as section 3.3
(`round_robin` p99 3.84 s, `p2c_ewma` p99 188 ms, direct p99 44 ms), and its
"ReverseProxy sized pool, 8371 req/s" row is not a fixed-rate run at all
(next section).

### 3.5 Defects in the benchmark tooling

Found while checking the tools; none was fixed (QA does not own them).

1. `golb-bench load -rate`: "req/s" is the offered rate, not the achieved
   rate (section 3.1).
2. `golb-bench load`: only complete responses enter the latency histogram.
   Requests that fail or time out are counted as errors and left out of the
   percentiles, so a run with errors reports percentiles that are too good.
   Every run in this document had 0 errors.
3. `scripts/bench.sh`: `proxy_scenario` does not pass load flags on, so the
   row labelled "ReverseProxy sized pool, N req/s" is a closed-loop run (it
   reported 8,760 req/s for a nominal 8,371). The same would happen to any
   competitor row added to the fixed-rate table.
4. `scripts/bench.sh` runs "direct" against one echo listener and golb
   against four; all four are one process, so this probably matters little,
   but it is not the same thing.
5. Micro-benchmarks: see the caveats in sections 2.1 and 2.4.

## 4. Other measurements from the integration suite

From `go test -race -count=3 -v ./test/...` (race detector on, so slower
than a normal build; three runs):

| Measurement | Result |
|---|---|
| 128 MiB upload through golb, peak heap growth of the whole test process | at most 1 MiB |
| 128 MiB download, peak heap growth; time to first 64 KiB | at most 1 MiB; 0.7-11 ms |
| 16 clients, 4 backends, one killed under load (PRD B5) | 6,484 / 7,749 / 6,554 requests, 0 failed; no attempt on the dead backend later than 1 s after it died |
| Every backend answers 503, 800 client requests (PRD B6) | 960 upstream requests in each run (limit (1 + 20 %) x 800 = 960, plus the floor) |
| Weight change, backend added, backend removed, 20 alternating reloads, 7 rejected reloads under 16 clients (PRD B2) | 10,387-10,832 requests per run, 0 failed |

## 5. PRD targets versus measured

All "met"/"missed" verdicts below are **preliminary**: see section 1.

| Target (PRD 3.2, 1.1) | Measured | Verdict |
|---|---|---|
| `Pick`+`Done` `round_robin`, 16 backends: at most 25 ns, 0 allocs | median 16.3 ns (12.3-26.1), 0 allocs | met (one sample of five above 25 ns); `Done` not included |
| `Pick`+`Done` `least_conn`, 16 backends: at most 25 ns, 0 allocs | median 15.4 ns (10.3-17.6), 0 allocs | met; `Done` not included |
| `Pick`+`Done` `p2c_ewma`, 16 backends: at most 40 ns, 0 allocs | median 38.0 ns (32.6-45.9), 0 allocs | met on the median, two samples of five above 40 ns: inconclusive |
| `Pick`+`Done` `ring_hash`, 100 backends: at most 80 ns, 0 allocs | median 33.0 ns (29.8-50.6), 0 allocs | met; `Done` not included |
| Any policy under `RunParallel` at 11 procs at most 3x the 1-proc time | `round_robin` 10.5x, weighted 10.5x, `least_conn` 6.2x, `p2c_ewma` 0.7x, `ring_hash` 0.7x | **missed** for `round_robin` and `least_conn` |
| `Counter.Inc`, `Histogram.Observe`: 0 allocs | 0 allocs | met |
| Handler versus bare `ReverseProxy`: at most +5 allocs/op | 83 versus 79-80 | met |
| Handler versus bare `ReverseProxy`: at most +15 % ns/op | 16,099 versus 39,633 ns (median) | met as measured; comparison not like for like (section 2.4) |
| Throughput through golb at least 50 % of direct (1 KiB, 64 connections) | 46 % (42,768 / 92,675); 64 % in the bench.sh run under heavier load | **missed** in the cleaner run; noise is larger than the gap |
| Throughput through golb at least 40,000 req/s | median 42,768 (38,560-43,634) | met on the median, one run of three below |
| Added latency at 50 % load: p50 at most 150 µs | +243 µs (311 versus 68 µs) | **missed**, measurement unreliable (section 3.3) |
| Added latency at 50 % load: p99 at most 1 ms | +1.1 s | not validly measured (section 3.3) |
| Throughput at least equal to bare `ReverseProxy` | 16,743 versus 9,273 req/s, ranges do not overlap | met |
| Access log on: throughput loss at most 10 % | 17,510 versus 16,743 req/s (no loss visible; ranges overlap widely) | met within noise |
| Reload every second: p99 within 10 % of the no-reload run | 61.3 ms versus 41.4 ms (+48 %) | **missed**; one sample each on a very noisy machine |
| Reload every second: 0 errors | 0 errors in 3 x 15 s (about 15 reloads per run) | met |
| B4: p99 with `p2c_ewma` at most 0.5x `round_robin` with one slow backend | 109.6 ms versus 225.4 ms = 0.49x | met, by a margin smaller than the noise |
| Comparison with nginx, HAProxy, Caddy, Traefik, Envoy | | not measured |


## 6. Limitations

- **Shared, noisy machine.** Load average 18 to 132 on 11 CPUs during the
  runs, from work that could not be stopped. Numbers are pessimistic and
  their spread is wide; a ±20 % difference between two rows is within noise
  unless the min-max ranges are clearly apart.
- **Loopback only, one machine.** The load generator and the backends
  compete with golb for the same cores, there is no network latency or
  packet loss, and the kernel's loopback path is not a NIC. Throughput on
  separate hosts will differ, probably upwards; the "at least 40,000 req/s"
  target in particular depends on how many cores the generator and backends
  leave for golb.
- **Mixed core types.** 5 fast and 6 slow cores; the scheduler's placement
  changes between runs.
- **No competitor comparison.** nginx, HAProxy, Caddy, Traefik and Envoy are
  not installed on this machine, so none was measured and this document makes
  no statement about golb relative to any of them. The only other proxy
  measured is a bare Go `httputil.ReverseProxy` (`golb-bench baseline`).
- **Narrow workload.** HTTP/1.1 keep-alive `GET`, fixed-size responses of
  1 KiB and 10 KiB, 16 to 256 connections, four backends in one process.
  Not measured: HTTP/2, TLS termination, uploads, WebSocket, the TCP proxy,
  new-connection rate, thousands of idle connections, memory per connection,
  long runs (each scenario is 3 x 18 s), behaviour at `GOMAXPROCS` other
  than 11.
- **Three runs per scenario.** Enough to show a spread, not to give a
  confidence interval.
- **CPU and RSS** are read from `ps` (CPU time difference over the scenario's
  wall time including warm-up; highest RSS sampled every 2 s). They include
  the Go runtime and garbage collector.
- **Micro-benchmark caveats** are listed in section 2: `Pool.Done` is not in
  the pick benchmarks, and the handler comparison does not use a stub
  transport or equal backend counts.
- **Tool defects:** section 3.5. In particular the fixed-rate results are
  not usable (section 3.3).

### Reproducing against competitors

First fix defect 3 of section 3.5, or the competitors will not appear in the
fixed-rate table correctly. Then install any of nginx, HAProxy,
Caddy, Traefik or Envoy so that the binary is on `PATH`, then run:

```sh
scripts/bench.sh > bench-report.md
```

The script starts each proxy it finds, one at a time, on the same port and in
front of the same four echo backends, with round robin, keep-alive and access
log off, and prints one row per proxy next to the golb rows. Rows for
proxies it does not find say "not measured". The competitor configurations
inside the script were written without the binaries at hand and have never
been run: treat the first run of each as a test of that configuration, check
that the proxy reports no errors and really reuses upstream connections, and
record each proxy's version. Run on a quiet machine, ideally with the load
generator and backends on other hosts, and repeat the whole script several
times before drawing a conclusion.
