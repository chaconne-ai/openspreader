# openspreader

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Maven Central](https://img.shields.io/badge/maven--central-1.0.0--SNAPSHOT-blue.svg)](https://central.sonatype.com/)

**A complete set of multi-process programming components for Spring Boot:
distributed locks, semaphores, latches and barriers, a process pool, a
replicated cache, cluster-wide scheduling, RPC and MapReduce-style aggregation,
all on top of the embedded [`spreader`](../spreader) cluster.**

Each component keeps the shape of its `java.util.concurrent` counterpart and
spans every instance of the application instead of one JVM. A `ProcessingMutex`
is acquired and released like a `ReentrantLock`; a `ProcessingSemaphore` hands
out permits the same way; the process pool takes a method call on one instance
and runs it on another. The cache holds a full replica on every node, so reads
are local map lookups and writes replicate as operations rather than as data.

The cluster itself is `spreader`, running inside the same JVM as your beans.
There is no Redis, no ZooKeeper and no message broker in the picture. Two
properties turn it on, and every component is disabled by default, so adding the
starter to a running service changes nothing until you enable one.

```java
@Service
public class ReportService {

    private final ProcessingSyncService syncs;

    @Scheduled(cron = "0 0 2 * * *")
    @MultiProcessingScheduled          // one instance runs it, not all five
    public void nightlyReport() { ... }

    public void exportOnce(String id) {
        ProcessingMutex mutex = syncs.applicationMutex("export:" + id);
        if (mutex.tryAcquire()) {
            try { export(id); } finally { mutex.release(); }
        }
    }
}
```

## What you get

| Component | Injected as | Replaces |
|---|---|---|
| Distributed lock | `ProcessingSyncService.applicationMutex(key)` | `ReentrantLock` |
| Semaphore | `ProcessingSyncService.applicationSemaphore(key, permits)` | `Semaphore` |
| Latch / barrier | `ProcessingSyncService.applicationLatch/Barrier` | `CountDownLatch`, `CyclicBarrier` |
| Replicated cache | `ProcessingCache` | a small Redis |
| Process pool | `ProcessingPool`, or `@MultiProcessingCall` on any bean method | a work queue |
| Cluster scheduling | `@MultiProcessingScheduled` | ShedLock |
| RPC | `@RpcClient` | Feign for internal calls |
| MapReduce | `ProcessingMapReduce`, or `@MultiProcessingMapReduce` | a batch framework, for jobs of this size |

## What to expect in practice

| | |
|---|---|
| **Reads never leave the process** | Every node holds a full replica of the cache, so a read is a local map lookup. Measured at over ten million bit tests per second against roughly two thousand cross-node writes per second. That gap is the design, and it says which workloads fit: read-heavy, tolerant of a few milliseconds of staleness. |
| **The cache replicates operations, not data** | `setbit` on a 100MB bitmap ships one datagram, not the bitmap. That is what makes a cluster-wide Bloom filter practical rather than theoretical. |
| **Two scopes for every component** | `application*` confines a lock, a semaphore or a latch to instances of the *same* application; `cluster*` spans everything sharing the cluster port. The choice is per call, not per deployment. |
| **The process pool tells you which side you are on** | A call runs locally when no peer is available and behaves exactly as before, so a single instance is a valid deployment. `spreader_pool_remote_ratio` says whether work is genuinely being spread or you have a local thread pool with extra steps. |
| **Failures arrive as one exception type** | Everything surfaces as `ProcessingException`, split by cause rather than summed into a single rate, so a timeout and a serialization error never look alike on a dashboard. Business exceptions from your own remote code are passed through unwrapped. |
| **Observability without extra work** | Metrics register with Micrometer and reach `/actuator/prometheus`; cluster health joins the actuator endpoints and carries each member's HTTP address, so one node's health response is enough to reach any other. |
| **Inherited limits** | Leadership is not consensus, and the cache is memory only. Under a network partition two sides can each hold a leader, and a full cluster restart starts from empty. Both are stated in [Limits](#limits) rather than discovered later. |

## Is this the right tool?

| Your situation | |
|---|---|
| Several instances of a Spring Boot service that need a lock, a shared counter, or a job that runs once | **Yes. This is the case it was built for** |
| You want a read-heavy shared cache and can tolerate a few milliseconds of staleness | Yes. Reads are local and roughly a thousand times cheaper than writes |
| You need the cache to survive a full cluster restart | No. It is memory only, by decision. Use Redis if the data must outlive the processes |
| "This must never run twice, **ever**": money moves, or a ledger is written | **No. Use Raft**, or a database transaction |
| You are not on Spring Boot | Use [`spreader`](../spreader) directly, the library underneath this one |
| You already operate Redis and ZooKeeper for other reasons | Probably not worth the swap. The operational cost you would save is already being paid |


## Table of contents

- [What you get](#what-you-get)
- [What to expect in practice](#what-to-expect-in-practice)
- [Is this the right tool?](#is-this-the-right-tool)
- [Why](#why)
- [Installation](#installation)
- [Quick start](#quick-start)
  - [The three annotations](#the-three-annotations)
  - [The cache](#the-cache)
- [Performance](#performance-and-what-it-tells-you-about-the-design)
- [Recommended configuration](#recommended-configuration)
- [Observability](#observability)
- [How this is verified](#how-this-is-verified)
- [Examples](#examples)
- [Limits](#limits)
- [Contributing](#contributing)
- [License](#license)

## Why

`ReentrantLock` stops working the moment you run a second instance. So does
`Semaphore`, `CountDownLatch`, `CyclicBarrier`, and `@Scheduled`. The usual
replacements are Redis, ZooKeeper, or a message broker, each a service to
install, monitor, and be woken up by.

openspreader gives you the same shapes, cluster-wide, and the coordination
happens between your own instances: there is **no external service** in the
picture at all. Membership and leader election come from
[`spreader`](../spreader), which runs inside the same JVM as your beans, so a
lock acquired on the leader costs a message rather than a network service call.


## Installation

**Maven**

```xml
<dependency>
    <groupId>com.chaconne-ai</groupId>
    <artifactId>openspreader</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Gradle**

```groovy
implementation 'com.chaconne-ai:openspreader:1.0.0-SNAPSHOT'
```

### Requirements

| | |
|---|---|
| **Java** | 17 or later |
| **Spring Boot** | 4.1, built and tested against it; see the note below |
| **Runtime dependencies** | [`spreader`](../spreader), plus Spring Boot itself |
| **Optional** | Micrometer for Prometheus; Kryo for faster serialisation; Netty, MINA or Grizzly for an alternative transport |
| **Ports** | one cluster port, identical on every node (22000 by default), plus one work port per node |

> **On Spring Boot versions.** Earlier Boot lines are not tested. Boot 4
> relocated the actuator auto-configuration packages, so the observability
> wiring is version-sensitive in a way that fails **silently**: metrics simply
> do not register, with no error. If you need Boot 3.x, verify that
> `/actuator/prometheus` actually contains `spreader_*` entries before relying
> on it.


## Quick start

```properties
spring.spreader.name=order-cluster
spring.spreader.ip-addresses=10.0.1.10,10.0.1.11,10.0.1.12
spring.spreader.multiprocessing.mutex.enabled=true
spring.spreader.multiprocessing.cache.enabled=true
```

```java
@Service
public class ReportService {

    private final ProcessingSyncService syncs;
    private final ProcessingCache cache;

    // Only one instance in the cluster runs this, whoever gets there first
    public void nightlyReport() {
        ProcessingMutex mutex = syncs.applicationMutex("nightly-report");
        if (!mutex.tryAcquire()) {
            return;
        }
        try {
            generate();
        } finally {
            mutex.release();
        }
    }

    // Written on one node, readable on all of them
    public void publish(String id, byte[] doc) {
        cache.set("report:" + id, doc, 1, TimeUnit.HOURS);
    }
}
```

### The three annotations

**`@MultiProcessingCall`**: spread the work across instances of the same
application:

```java
@MultiProcessingCall
public String renderReport(String month) { ... }
```

The call may execute in another process. Arguments and the return value are
serialized, so both must be `Serializable`. With no peers available it simply
runs locally, so a single instance behaves exactly as before.

**`@RpcClient`**: call a method on a *different* application:

```java
@RpcClient(name = "inventory-service", fallback = InventoryFallback.class)
public interface InventoryClient {
    @RpcMethod
    int available(String sku);
}
```

Routing is by application name, not URL. Nodes come and go; there is nothing to
update.

**`@MultiProcessingScheduled`**: the job fires on every instance, but only one runs it:

```java
@MultiProcessingScheduled(lockAtLeastMs = 30_000)
@Scheduled(cron = "0 0 3 * * *")
public void syncOrders() { ... }
```

`lockAtLeastMs` matters more than it looks: a job that finishes in 50ms releases
the lock immediately, and a second instance can then win the same tick. Holding
the lock for a minimum span prevents that.

### The cache

47 commands over four data structures (strings, hashes, lists, sorted sets),
plus bitmaps and Bloom filters on top.

```java
cache.set("k", bytes, 10, TimeUnit.MINUTES);
cache.hset("user:1", "email", bytes);
cache.zadd("leaderboard", member, 99.5);
cache.setbit("seen", offset, true);

ProcessingBloomFilter filter = ProcessingBloomFilter.create(cache, "sms:sent", 1_000_000L, 0.01);
filter.put(phone);
filter.mightContain(phone);
```

**It replicates operations, not data.** `setbit` on a 100MB bitmap ships one
datagram. This is what makes a cluster-wide Bloom filter practical.

## Performance, and what it tells you about the design

Single JVM, loopback. **Absolute numbers will not transfer to your hardware**;
the ratios will, and they are what the design decisions are visible in.

### Reads never leave the process

| Operation | QPS | P50 |
|---|---:|---:|
| `getbit` (one bit test) | **10,724,759** | 0.0002 ms |
| `exists` | 6,655,804 | 0.0003 ms |
| `hget` (single field) | 2,394,463 | 0.0007 ms |
| lock acquire+release, **node is leader** | 943,174 | 0.0009 ms |
| lock acquire+release, **cross-node** | 875 | 1.13 ms |
| `hset` / `zadd` / `incr` (all cross-node writes) | ~2,100 | 0.43 ms |

Two ratios carry the whole design.

**Local read vs cross-node write: about 1,000×.** Every process holds a complete
replica and reads never touch the network. That is what the cache is for, and it
sets the shape of workloads that suit it: read-heavy, tolerant of a few
milliseconds of staleness.

**Leader vs follower: also about 1,000×** for the very same operation
(943,174 vs 875 QPS for a lock). Whether *this* node happens to be the leader
decides three orders of magnitude, which is why `spreader_cache_leader` is
worth graphing next to your latency panels.

### Reads span five orders of magnitude by how much they touch

| Read | QPS |
|---|---:|
| `getbit` | 10,724,759 |
| `exists` | 6,655,804 |
| `size` | 4,118,846 |
| `hget` / `zscore` | 1.8M – 2.4M |
| `lrange` / `zrange` (50 elements) | ~420,000 |
| `hgetAll` (whole hash) | **59,539** |

`hgetAll` is **180× slower than `getbit`** and has no upper bound: it copies
the entire hash. Point reads are cheap enough to put anywhere; range reads are
not, and `hgetAll` on a large hash belongs nowhere near a hot path.

### Task distribution: know which side you are on

| Mode | QPS |
|---|---:|
| local execution (no peers) | 253,013 |
| dispatched to another process | 11,581 |

**22× apart**, so `spreader_pool_remote_ratio` is the metric that decides your
throughput. Near zero means work is not actually being spread: you have a local
thread pool with extra steps. Near one means it is, and the cost is the expected
trade: throughput for other machines' CPUs.

### Bloom filter

| Operation | QPS |
|---|---:|
| `mightContain` (local bit test) | 495,272 |
| `put` (node is leader) | 166,924 |
| `put` (cross-node, 7 setbits) | **320** |

**1,500× between local read and cross-node write.** Each `put` is seven `setbit`
operations, each a round trip. The practical consequence is unambiguous: **load
on the leader, query anywhere.**

### Serialization

| | Encode/decode only | Through RPC |
|---|---:|---:|
| JDK | 91,788 QPS | 4,462 QPS |
| Kryo | **204,362 QPS** | 4,226 QPS |

Kryo is **2.2× faster** at pure encode/decode, and the advantage vanishes once
the network is in the path. Serialization is not the bottleneck in RPC; the
round trip is. Switch to Kryo for large objects held in the cache, not to speed
up remote calls.

> Measured inside a 4-core container, single JVM, loopback. Absolute values will
> not transfer to your hardware: every cross-node call here costs a loopback hop
> instead of a real network one. The ratios will.

## Recommended configuration

```properties
# --- cluster ---
spring.spreader.name=order-cluster
spring.spreader.ip-addresses=10.0.1.10,10.0.1.11,10.0.1.12
spring.spreader.advertise-host=10.0.1.10          # in containers: what peers can dial

# --- turn on only what you use ---
spring.spreader.multiprocessing.mutex.enabled=true
spring.spreader.multiprocessing.cache.enabled=true
spring.spreader.multiprocessing.scheduling.enabled=true

# --- cache ---
spring.spreader.multiprocessing.cache.max-keys=1000000
spring.spreader.multiprocessing.cache.eviction-policy=LRU
spring.spreader.multiprocessing.cache.request-timeout-ms=5000
```

### Sizing the cache

`max-keys` and `max-bytes` are per process, not per cluster: every node holds a
full replica. Budget for one node's heap, and remember eviction is **local**: a
key dropped here still exists elsewhere, so a subsequent read may be a miss on
one node and a hit on another.

Eviction samples 5 keys and evicts the least recently used among them (Redis
does the same). Exact LRU would require a global lock on every read, which would
cost the 1.3M reads/s that make the cache worth having. Raise `eviction-samples`
to 10 for a closer approximation.

### Timeouts

```properties
spring.spreader.multiprocessing.cache.request-timeout-ms=5000
spring.spreader.multiprocessing.mutex.request-timeout-ms=3000
```

Set the write timeout **longer than a leader election takes**. During an
election, writes retry rather than fail; a timeout shorter than the election
window turns a recoverable pause into an error.

### Detecting a stuck cluster

```properties
spring.spreader.metrics.leaderless-down-after=30s   # default
```

Health reports DOWN once this node has been unable to see a leader for longer
than this. It exists because 200 ms without a leader is a normal handover and
5 minutes without one is a dead cluster, and until this check was added, the two
looked **identical** in `/actuator/health`.

What makes that dangerous is that every component behaves *correctly* in the
meantime: writes retry until timeout, locks return "not acquired", scheduled
tasks skip the tick. Three deliberate degradations, none of them a bug. Together
they mean the cluster is doing nothing at all, while health says UP, so
Kubernetes will not restart it and the load balancer will not remove it.

### What happens during an election

Not "unavailable". It depends on the operation, deliberately:

| Operation | Behaviour |
|---|---|
| cache read | unaffected, may be slightly stale |
| cache write | retries until `request-timeout-ms` |
| lock / semaphore | returns "not acquired" |
| scheduled task | skips this tick |

You do not need to gate traffic at the application layer. Each path handles the
gap in the way that matches its own semantics, and gating would not work
anyway, since `isLeader()` is itself indeterminate during the transition.

### Serialization

```properties
spring.spreader.multiprocessing.serialization=JDK   # or KRYO
```

JDK is the default and needs nothing extra. Kryo encodes and decodes **2.2×
faster** (204,362 vs 91,788 QPS on ~300 byte payloads), so it is worth adding
when serialization is genuinely on your hot path: large objects, high volume.

It will not speed up remote calls, though: through RPC the two land within 5%
of each other, because the round trip dominates and the codec does not. Add Kryo
for what the cache serializes, not for what the network carries.

### Every failure comes out as a `ProcessingException`

All components throw subclasses of a single unchecked superclass, so one
handler covers the whole multi-process path instead of a list that goes stale
the moment a component is added:

```java
@ExceptionHandler(ProcessingException.class)
ResponseEntity<?> degrade(ProcessingException e) { ... }
```

| Subclass | Thrown when |
|---|---|
| `ProcessingCacheException` | cache read/write failed or timed out |
| `ProcessingPoolException` | task rejected, method not exposed, remote execution failed |
| `ProcessingMutexException` | lock could not be acquired or was lost |
| `ProcessingSemaphoreException` | permit accounting is inconsistent |
| `ProcessingSchedulingException` | a cluster-scheduled job could not run |
| `SerializationException` | payload could not be encoded or decoded |
| `RpcException` | remote call failed **at the transport level** |
| `RpcOverloadException` | request was **rejected by the rate limiter**, never sent |

The subclasses are siblings, not a chain. That matters for the last two:
`RpcOverloadException` is deliberately **not** a subclass of `RpcException`,
so narrowing `retryableExceptions()` to `RpcException` excludes exactly the
case where retrying makes things worse. Do not "tidy" it under `RpcException`.

Business exceptions thrown by your own remote code are **not** wrapped; they
propagate as themselves, so `catch` on your own types still works.

## Observability

Four read endpoints, four audiences:

```
/actuator/prometheus        Grafana : 212 spreader_* metrics
/actuator/spreader-report   humans  : one page, problems first
/actuator/spreader          your UI : structured JSON
/actuator/health            probes  : plus splitBrainOccurrences, leaderlessMillis,
                                      and otherMembers: who else this node can see
```

`otherMembers` is there because `memberCount` tells you *one is missing* but not
*which one*. It also makes the worst failure visible: A can see B while B cannot
see A, and **both counts look right**. Only the lists disagree.

Every entry carries that node's `metadata`, and in a web application the
framework puts this node's web address in there once the server has bound:
`server.port`, `management.port`, and, where they are not at the root, the
context path, the servlet path and the actuator's base path. Gossip only ever
knew `host:22000`, so until now nothing in the cluster could say *where a node
answers HTTP*. Now one curl at any member's health gives you every other
member's actuator URL:

```
http://<host>:<management.port><server.servlet.context-path><management.endpoints.web.base-path>/health
```

And one endpoint that writes:

```
GET/POST/DELETE /actuator/spreader-break   take this node out of service, or bring it back
```

It calls `takeBreak()`: the node stops sending and receiving *business* messages
but stays a full member: it keeps gossiping, keeps answering probes, and if it
was the leader it **stays** the leader. This is not a graceful shutdown.

Actuator exposes only `health` over the web by default, so this endpoint is
opt-in. If you do name it in `management.endpoints.web.exposure.include`, put
authentication on the management port first: without it anyone can retire your
nodes one by one while the cluster still looks perfectly healthy, with a full member
count, no alerts, nobody doing any work.

The report starts with a health check rather than a metric dump, because these
conditions **produce no log line** and will not be found unless looked for:

| Signal | Meaning |
|---|---|
| `spreader_cache_outbox_overflow` | broadcast queue overflowed; replicas fall back to full sync |
| `spreader_mutex_acquire_timeouts` | requests timed out waiting for a lock |
| `spreader_mutex_leaked` | `acquired − released`, climbing means a missing `finally` |
| `spreader_semaphore_held` | same idea for permits, and permit exhaustion shows up as *everyone stalls*, not as an error |
| `spreader_latch_await_timeouts` | a latch never reached zero; usually a participant that never called `countDown` |
| `spreader_barrier_broken` | *another* participant timed out, was interrupted, or left |
| `spreader_rpc_rejected` | inbound queue full: this node is dropping requests |
| `spreader_pool_remote_fallbacks` | dispatched work came back unanswered |
| `spreader_buffer_dropped` | messages discarded under load, silently |
| `splitBrainOccurrences` | this node found another cluster-port holder |

### Failures are split by cause, not summed into one rate

A single failure rate cannot be acted on. These pairs can:

| Component | Two counters | Point to |
|---|---|---|
| RPC | `call_timeouts` / `remote_errors` | the network and the peer's load / the peer's business code |
| Semaphore | `local_blocked` / `remote_denied` | this process is at its own quota / the whole cluster is tight |
| Barrier | `await_timeouts` / `broken` | you timed out / somebody else did |

The semaphore pair matters most: raising the local gate fixes `local_blocked`
and does **nothing** for `remote_denied`.

Averages (`*_avg_millis`) count only the calls that succeeded. Folding timeouts
in makes the average converge on the configured timeout, a function of your
config rather than of the peer's real speed.

Each endpoint reports **its own node only**. A UI showing the cluster must fetch
from every node and aggregate, and cumulative counters add up, while rates
(`contentionRate`, `remoteRatio`) do not.

### Reading the numbers yourself

Every component service implements one interface, so your own dashboard does not
need a per-component adapter:

```java
public interface MultiProcessingService extends GossipListener, AutoCloseable {
    Map<String, Object> stats();
}
```

```java
// Inject them all and render whatever you like
public DiagnosticsController(List<MultiProcessingService> services) { ... }
```

`MutexService`, `LatchService`, `BarrierService`, `SemaphoreService`,
`PoolService`, `RpcService` and `CacheService` all implement it. The three
endpoints above read the same `stats()`, so there is no second set of numbers
that could disagree with what you see.

## How this is verified

Three layers, because each catches what the others cannot.

**In-JVM suite: 642 cases per cell, 16 cells.** Multi-node clusters inside a single JVM, run
against the full matrix of 4 transport implementations × 2 protocols × 2
serializations. Fast enough to run on every change, which is what makes it
useful; blind to anything involving a real network interface.

**Cross-container clusters.** Three separate containers on a fixed-IP subnet,
started simultaneously so they race for the cluster port. This layer exists
because of what it found: on one machine, holding the cluster port *is*
mutual exclusion, because the OS guarantees it. Across machines every host binds its
own port with no conflict, and the code had inherited the single-machine
assumption. Three isolated nodes each believed they were the sole leader and
never merged. No in-JVM test could have shown this.

**Benchmarks.** Not for leaderboards, but for the ratios in the section above.
Local-vs-remote, leader-vs-follower, point-read-vs-range-read. Those ratios are
what tells you where to put work.

Example code is covered too, and deliberately so. Examples fail in a particular
way: they sit off the main path, so a change that alters their *behaviour* still
compiles and nobody notices. One of them shipped an `await()` without the
matching `countDown()`: correct-looking, and it hung forever. The test found it
in one run.

## Examples

Runnable, and covered by tests, in `com.chaconneai.openspreader.example`:

| Class | Shows |
|---|---|
| `QuickStartBestPractice` | the shortest working form of each component |
| `MutexBestPractice` | exclusive run, wait-then-run, once-per-period, sharded import |
| `SemaphoreBestPractice` | degradation, legacy-system throttling, multi-resource |
| `CacheBestPractice` | cache-aside, stampede protection, counters, TTL discipline |
| `RateLimitBestPractice` | a quota shared across the cluster, not per instance |
| `LeaderboardBestPractice` | sorted sets |
| `DelayedQueueBestPractice` | delayed jobs on a sorted set |
| `BloomFilterBestPractice` | sizing, rebuilding, when not to use one |
| `PhoneDedupBestPractice` | deduplication at scale |
| `MethodDispatchBestPractice` | `@MultiProcessingCall` and recursive tasks |
| `ScheduledTaskBestPractice` | `@MultiProcessingScheduled` patterns |

These are tested, not illustrative. The tests exist because example code fails
in a particular way: it is off the main path, so a change that alters its
*behaviour* still compiles, and nobody notices until someone copies it.

## Limits

- **Leadership is not consensus.** Under a partition both sides can elect a
  leader; it heals when the partition does. If a job must *never* run twice, use
  a system with a consensus protocol.
- **The cache is a cache.** Every node holds a full replica in heap. It is not a
  database and not a durable store.
- **Eviction is local.** Nodes can disagree about which keys are resident.
- **Everything is in-process.** No persistence: a full cluster restart starts
  from empty.
- **Cross-machine performance is unmeasured.** Every number above is one JVM
  over loopback.

## Contributing

Issues and pull requests are welcome at
[github.com/chaconne-ai/openspreader](https://github.com/chaconne-ai/openspreader).
For anything that does not belong in a public issue, write to
hello@chaconne-ai.com.

A few things worth knowing before you open one:

- **Every fix needs a regression test.** The test should fail before the fix and
  pass after it. Coverage as a number is not a goal; a bug recurring is the line
  that must not be crossed.
- **Run the full suite, not the single case.** "Green alone, red in the full
  run" has been a real defect every time it has come up in this project, never
  an environment quirk. Load changes the timing, and the timing is where these
  bugs live.
- **Comments are written in English**, and they explain *why* rather than
  restating *what*. The reasoning behind a non-obvious decision, and the
  failure that motivated it, is the part worth writing down.
- **A component that is off must cost nothing.** No thread pool, no bean, no
  line on a dashboard. That property is what makes the starter safe to add.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
Copyright 2026 [ChaconneAI](https://github.com/chaconne-ai).

```
Copyright 2026 ChaconneAI

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
