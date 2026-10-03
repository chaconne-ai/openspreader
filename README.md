# openspreader

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-green.svg)](https://spring.io/projects/spring-boot)
[![Maven Central](https://img.shields.io/badge/maven--central-1.0.0--SNAPSHOT-blue.svg)](https://central.sonatype.com/)

### The missing piece of Java concurrency: `java.util.concurrent`, scoped to the cluster.

**Thirteen coordination primitives as one Spring Boot starter. The APIs you already know,
spanning every instance of your application instead of one JVM. Zero lines of configuration to
start.**

```java
// A lock across every replica of this application
ProcessingMutex mutex = syncs.applicationMutex("daily-report");
if (mutex.tryAcquire()) {
    try { generateReport(); } finally { mutex.release(); }
}

// A scheduled task that runs on one instance per round. One annotation.
@Scheduled(cron = "0 0 2 * * *")
@MultiProcessingScheduled
public void syncOrdersDaily() { ... }
```

---

## Features

| Component | What it solves | Instead of |
|---|---|---|
| `ProcessingMutex` | One party at a time, cluster-wide | Redis SETNX, ZooKeeper |
| `ProcessingSemaphore` | At most N concurrent, whatever the replica count | A local `Semaphore` that multiplies by replica count |
| `ProcessingCountDownLatch` | Wait for N things to finish, once | Polling a database flag |
| `ProcessingCyclicBarrier` | N parties align, round after round | Hand-rolled rendezvous |
| `ProcessingExchanger` | Two processes swap an item | A queue you have to operate |
| `@MultiProcessingScheduled` | A scheduled task runs on one instance per round | ShedLock |
| `ProcessingPool` / `@MultiProcessingCall` | Hand computation to another replica | A work queue |
| `ProcessingCache` | 50 commands, Redis-shaped, reads from local memory | A small Redis |
| `ProcessingBloomFilter` | De-duplicate millions without the memory | A `Set` costing hundreds of MB |
| `ProcessingMapReduce` | Shard a dataset, compute, aggregate | A batch framework, for jobs of this size |
| `@RpcClient` | Call another application like a local method | Feign for internal calls |
| Cross-process events | `publishEvent` reaches every instance, **no code change** | A message broker |
| `ProcessingDag` | Multi-step flows with parallelism, branching, joins | A workflow engine, for graphs of this size |

Two properties cut across all of them:

| | |
|---|---|
| **Reads never leave the process** | Every node holds a full cache replica, so a read is a local map lookup. Measured over 4 million reads per second against roughly 2,000 cross-node writes. That gap says which workloads fit |
| **Two scopes, chosen per call** | `application*` confines a lock or permit to replicas of the *same* application; `cluster*` spans everything on the cluster port. Nine times out of ten you want the first |

## Architecture

Everything rests on [spreader](https://github.com/chaconne-ai/spreader): the cluster runs
**inside the same JVM** as your beans, so there is no broker and no registry.

```
  ┌─────────────────── your application ───────────────────┐
  │                                                        │
  │   @Scheduled   mutex.tryAcquire()   cache.get(k)       │
  │        │              │                  │             │
  │   ┌────┴──────────────┴──────────────────┴─────────┐   │
  │   │            openspreader components             │   │
  │   │  lock  semaphore  latch  cache  pool  rpc  dag │   │
  │   └────────────────────┬───────────────────────────┘   │
  │                        │                               │
  │   ┌────────────────────┴───────────────────────────┐   │
  │   │      spreader: gossip, members, leader         │   │
  │   └────────────────────┬───────────────────────────┘   │
  └────────────────────────┼───────────────────────────────┘
                           │ TCP/UDP
              ┌────────────┼────────────┐
         other replica           other replica
```

**Writes go through the leader, reads stay local.** The registers for locks, permits, latches
and barriers live in the leader's memory; the cache replicates operations rather than data.

```
cache.incr("views", 1) on a follower
    │
    ├─ forwarded to the leader
    │
    ├─ leader applies it, assigns version 1007
    │
    └─ broadcasts {v1007, INCR, "views", 1} to every replica
                      │
         each replica applies it to its OWN copy
```

That is why `setbit` on a 100MB bitmap ships one datagram rather than the bitmap, and why a
cluster-wide Bloom filter is practical at all.

## Requirements

| | |
|---|---|
| **Java** | 17 or later |
| **Spring Boot** | 4.1, built and tested against it |
| **Optional** | Micrometer for Prometheus; Kryo for faster serialisation; `spring-data-redis` for cache overflow; Netty, MINA or Grizzly for an alternative transport |
| **Ports** | one cluster port, identical on every node (22000 by default), plus one work port per node |

> **On Spring Boot versions.** Earlier lines are not tested. Boot 4 relocated the actuator
> auto-configuration packages, so the observability wiring is version-sensitive in a way that
> fails **silently**: metrics simply do not register. On Boot 3.x, verify that
> `/actuator/prometheus` really contains `spreader_*` entries before relying on it.

## Quick Start

**Install**

```xml
<dependency>
    <groupId>com.chaconne-ai</groupId>
    <artifactId>openspreader</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```xml
<repositories>
    <repository>
        <id>central-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>
```

**Configure nothing.** Discovery defaults to 127.0.0.1, so two instances on one machine with
different `server.port` values form a cluster by themselves.

One line has to change for production:

```properties
spring.spreader.ip-addresses=192.168.0.111,192.168.0.63,192.168.0.77
spring.spreader.name=order-cluster
spring.spreader.multiprocessing.mutex.enabled=true
```

**Minimal example**

```java
@Service
public class ReportService {

    private final ProcessingSyncService syncs;

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
}
```

**Expected output**, with three instances started:

```
Node started: cluster=order-cluster, name=order-service, clusterPort=22000
Cluster cache enabled: replication=order-service, maxKeys=1000000
Mutex service started

[instance 1] acquired nightly-report, generating
[instance 2] tryAcquire returned false, skipping this round
[instance 3] tryAcquire returned false, skipping this round
```

Every component is **off by default**, so adding the starter to a running service changes
nothing until one is asked for:

```properties
spring.spreader.multiprocessing.mutex.enabled=true
spring.spreader.multiprocessing.semaphore.enabled=true
spring.spreader.multiprocessing.cache.enabled=true
spring.spreader.multiprocessing.pooling.enabled=true
spring.spreader.multiprocessing.scheduling.enabled=true
spring.spreader.multiprocessing.aggregation.enabled=true
spring.spreader.multiprocessing.rpc.enabled=true
spring.spreader.multiprocessing.dag.enabled=true
```

> The DAG engine and the task pool need to be on for **every** instance, since any one of them
> may be handed work to run.

## Examples

Five representative components. Every component has a runnable example under
`com.chaconneai.openspreader.example`.

### Lock

**Input**

```java
ProcessingMutex mutex = syncs.applicationMutex("bulk-export");
```

**Execution**: three ways to take it, matching three attitudes.

```java
mutex.tryAcquire();                    // skip if taken, do not wait
mutex.acquire(3, TimeUnit.SECONDS);    // wait three seconds, then skip
mutex.acquire();                       // wait indefinitely
```

**Output**

```java
mutex.isHeld();              // is anyone holding it
mutex.currentOccupied();     // WHO holds it, which answers "why did my task not run"
mutex.release(5_000);        // release, but keep others out for five seconds
```

That cooldown solves a real problem: a task finishes, another replica immediately takes the
lock and runs it again. A cooldown window removes the duplicate.

### Semaphore

**Input**: protect a third party API at five concurrent calls.

```java
ProcessingSemaphore sem = syncs.applicationSemaphore("third-party-api", 5);
```

**Execution**

```java
if (sem.tryAcquire(3, TimeUnit.SECONDS)) {
    try { callThirdParty(); } finally { sem.release(); }
}
```

**Output**: **the five permits are shared cluster-wide**, whatever the replica count. That is
exactly what a single process `Semaphore` cannot do: three replicas each holding a local
semaphore of 5 gives you an actual concurrency of 15.

### Cache

**Input**: 50 commands over four data structures, plus bitmaps and Bloom filters.

```java
cache.set("k", bytes, 10, TimeUnit.MINUTES);
cache.hset("user:1", "email", bytes);
cache.zadd("leaderboard", member, 99.5);
cache.setbit("seen", offset, true);
cache.zrangeByScore("series", from, to, 0, 500);     // a page, not a million members
cache.zremrangeByScore("series", 0, cutoff);         // a retention policy in one operation
```

**Execution**: reads come from local memory; writes are forwarded to the leader and broadcast
back incrementally.

**Output**

```
reads     2,900,000 to 5,700,000 per second, never touching the network
writes    about 22,500 per second, cluster-wide, whatever the operation
```

So it suits shared state that is **read far more than written**: configuration, allow-lists,
rate-limit counters, sessions. A write becomes visible elsewhere after milliseconds. It is
**eventually consistent**, not a database.

**When memory is not enough**: register a `CacheStore` bean and eviction becomes a *move*
rather than a loss.

```properties
spring.spreader.multiprocessing.cache.external.enabled=true
spring.spreader.multiprocessing.cache.external.key-prefix=spreader:cache:
```

| | |
|---|---|
| A key lives in exactly one half | Memory, or outside. Reads ask memory first; writes go where the key already is |
| Eviction moves instead of deleting | Same sampling, same policy, but the key is written across before the memory copy goes |
| Only the leader writes outside | A follower replaying the replication stream touches memory only |
| Bitmaps stay in memory | A Bloom filter lookup does seven bit tests, which outside would be seven round trips |
| Nothing is enabled by default | Without the bean, `store` **is** the local store: same object, no wrapper, no extra call |

### MapReduce

**Input**: implement a job, register it as a bean.

```java
@Component("wordCount")
public class WordCountJob implements MapReduceJob<String, String, Integer, Integer> {

    public List<String> split(String text, int suggestedShards) {
        return splitByLines(text, suggestedShards);
    }

    public void map(String shard, Emitter<String, Integer> emitter) {
        for (String word : shard.split("\\W+")) {
            if (!word.isBlank()) {
                emitter.emit(word.toLowerCase(), 1);
            }
        }
    }

    public Integer reduce(String word, List<Integer> counts) {
        return counts.stream().mapToInt(Integer::intValue).sum();
    }
}
```

**Execution**: where each phase runs.

```
split     the submitter only, once
map       every node, the submitter included, once per shard
reduce    wherever the key's hash sends it, once per distinct key
```

**Output**

```java
MapReduceResult<String, Integer> result = mapReduce
        .<String, String, Integer, Integer>submit("wordCount", input)
        .get(5, TimeUnit.MINUTES);
```

Combining the partial results is not yours to write: one key is reduced on exactly one node.

> **Every node needs this bean, at the same version.** Half the nodes on a new `map` and half
> on the old one produce **a wrong answer rather than an error**. Do not submit jobs during a
> rolling deployment.

### RPC

**Input**: enable the scan, declare an interface pointing at another application's name.

```java
@SpringBootApplication
@EnableRpcClients(basePackages = "com.example.client")
public class Application { }

@RpcClient(serviceId = "inventory-service",
           timeout = 3000,
           maxConcurrent = 50,                       // in-flight ceiling, fail fast when full
           fallback = InventoryFallback.class)
public interface InventoryApi {

    int stockOf(String sku);
}
```

**Execution**: inject and call it like any bean. The server side needs a bean with a matching
method carrying `@MultiProcessingCall`.

```java
int stock = inventory.stockOf("widget-1");
```

**Output**: routing is by application name, not URL, and the target is chosen afresh on every
call, so scaling on the other side takes effect immediately.

| Setting | Why it matters |
|---|---|
| `maxConcurrent` | Without it, requests pile up locally when the downstream slows, and what falls over is your own process |
| `maxRetries` | A failure is retried **on a different instance each time**, so the called method should be idempotent. Retrying after a timeout is ambiguous: the request may have completed with only the reply lost |

## Configuration

### Cluster

| Property | Default | Description |
|---|---|---|
| `spring.spreader.name` | `default` | Cluster name, the only means of isolation between dev, staging and prod |
| `spring.spreader.ip-addresses` | `127.0.0.1` | Where to look for peers |
| `spring.spreader.advertise-host` | auto | **Set this in containers** |
| `spring.spreader.leader-eligible` | `true` | `false`: joins and works, never contends for leadership |

### Cache

| Property | Default | Description |
|---|---|---|
| `...cache.max-keys` | `1000000` | **Per process, not per cluster**: every node holds a full replica |
| `...cache.max-bytes` | `0` | 0 means no byte limit |
| `...cache.eviction-policy` | `LRU` | Samples 5 keys and evicts the least recently used, as Redis does. Exact LRU would need a global lock on every read |
| `...cache.eviction-samples` | `5` | Raise to 10 for a closer approximation |
| `...cache.external.enabled` | `false` | Build the Redis-backed overflow store |
| `...cache.external.key-prefix` | `spreader:cache:` | Keeps `keys`, `keyCount` and a clear from reaching data that is none of this cache's business |
| `...cache.request-timeout-ms` | `5000` | |
| `...cache.max-batch-size` | `512` | Most updates packed into one broadcast frame |

### Timeouts and leases

| Property | Default | Description |
|---|---|---|
| `...mutex.request-timeout-ms` | `3000` | Shorter than the cache's: a lock request that is not answered quickly is better retried |
| `...mutex.lease-ms` | `15000` | Renewed while held, so a task may run as long as it likes. Only a crashed holder loses it |
| `...scheduling.default-scope` | `APPLICATION` | |

### Health

| Property | Default | Description |
|---|---|---|
| `spring.spreader.metrics.split-brain-down-after` | `5m` | Grace before a split turns health DOWN |
| `spring.spreader.metrics.leaderless-down-after` | `30s` | |

## Performance

**Conditions**: 3-node cluster in one JVM over loopback, 4-core container, TCP/NIO with JDK
serialization, 8 concurrent threads. Measured 2026-10-03 by `CacheWriteStressTest`. Absolute
values do not transfer; the ratios do. Earlier revisions reported writes several times lower,
measured single-threaded, which gives one write's latency rather than throughput.

### Reads never leave the process

| Operation | QPS |
|---|---:|
| `getbit` | 5,700,036 |
| `stats` (aggregate, four numbers) | 5,125,359 |
| `exists` / `ttl` / `type` / `size` | 3.3M to 3.7M |
| `get` | 2,928,187 |
| `hget` (one field of 500) | 2,454,941 |
| `zrange` (50 of 500) | 544,151 |
| `hgetAll` (all 500 fields) | 96,197 |
| `keys` (wildcard) | 21,836 |

`hget` against `hgetAll` is **25x**, and the easiest accidental mistake in this API.

### Writes cost one round trip, whatever the operation

All fourteen write operations, each measured in its own fresh cluster, land between 17,600 and
24,100 per second. The leader executing the same operations on itself reaches 1.3M to 2.5M, so
what a write costs is the hop, not the work. Controls ruled out the inbound thread pool (4
threads against 16 moves the peak by 11 percent), the operation type, and the read-your-write
wait.

| | |
|---|---|
| Read scaling | Linear with node count. Reads are local |
| Write scaling | **Flat.** One, two and three instances writing at once give 13,966, 12,544 and 19,729 combined |
| Cluster-wide write ceiling | **~22,500/s**, reached at four concurrent writers |

Past the ceiling the leader discards replication messages rather than slowing down, and
followers catch up by full snapshot, so `outboxOverflow` in `stats()` is the metric to alert on.
The question to ask is how far your write rate is from 22,500. Far means no problem. Close means
that data may not belong here.

## How This Is Verified

| Layer | What it covers |
|---|---|
| **Mainline, 8 cells** | TCP/UDP x NIO/NETTY x JDK/KRYO, 652 cases per cell, green as of 2026-10-01 |
| **Full matrix, 16 cells** | Adds MINA and Grizzly, the transport layer and process lifecycle suites |
| **Cross-container** | Three containers on a fixed-IP subnet, started simultaneously so they race for the cluster port |
| **Differential testing** | The external cache store is verified by running the same operations against both implementations and comparing return values, which is how a leaked internal field was caught |

## Limitations & Trade-offs

Every coordination capability rests on one thing: **whoever holds the cluster port is the
leader**, and the write path for locks, permits, latches, barriers and the cache is all there.

| Premise | Consequence |
|---|---|
| **Leader uniqueness rests on timing, not consensus** | Under partition each side may have a leader, and two parties hold the same lock. Fine for work where a repeat is wasteful: scheduled tasks, cache warming, batch de-duplication. **Not** for a transfer or a debit, which belongs in a database transaction or behind an idempotence key |
| **A change of leader leaves a gap** | Takeover measured at 3.3 to 4.4 seconds, during which lock acquisitions and cache writes retry. Leave business timeouts headroom, or you will see "it failed once and was fine a few seconds later" |

| Limit | Detail |
|---|---|
| **The cache is a cache** | Every node holds a full replica in heap. Not a database, not durable |
| **Eviction is local** | Nodes can disagree about which keys are resident |
| **Everything is in-process** | No persistence by default: a full cluster restart starts from empty |
| **Resume is at-least-once** | A DAG node that ran and reported back, whose record was not yet written when the coordinator died, is a step that happened and is not in the record. An effect outside the process cannot be made atomic with a row inside it |
| **Writes have a cluster-wide ceiling** | About 22,500 per second in the measured setup, and it does not grow with instance count. This is a design limit, not a defect: writes funnel through the leader because a monotonic version is what lets every replica replay in the same order |
| **Cross-machine performance is unmeasured** | Every number above is one JVM over loopback |

## Documentation

| | |
|---|---|
| **Examples** | `com.chaconneai.openspreader.example`, one package per component. `QuickStartBestPractice` is the shortest code covering all of them on one page |
| **Underlying cluster** | [spreader](https://github.com/chaconne-ai/spreader): gossip, membership, pre-emptive election, four transports |
| **Observability** | `/actuator/prometheus` for 212 `spreader_*` metrics, `/actuator/spreader-report` for one page with problems first, `/actuator/spreader` for structured JSON |

Two numbers deserve alerts because **neither produces a log line**:

```
spreader_cache_outbox_overflow   broadcast queue overflowed; replicas fall back to full sync
spreader_cache_spill_failures    keys dropped because the external store refused them
```

## Contributing

| Rule | Why |
|---|---|
| **Every fix needs a regression test** | Coverage is not a target; a bug coming back is the line |
| **Run the full suite, not the single case** | "Green alone, red in the full run" has been a real defect every time it has come up here |
| **Comments are written in English**, explaining *why* rather than restating *what* | The reasoning behind a non-obvious decision is the part worth writing down |

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
Copyright 2026 [ChaconneAI](https://github.com/chaconne-ai).
