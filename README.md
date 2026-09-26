# openspreader

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Maven Central](https://img.shields.io/badge/maven--central-1.0.0--SNAPSHOT-blue.svg)](https://central.sonatype.com/)

**A complete set of multi-process programming components for Spring Boot:
distributed locks, semaphores, latches, barriers and exchangers, a process pool,
a replicated cache, cluster-wide scheduling, RPC and MapReduce-style aggregation,
all on top of the embedded [`spreader`](https://github.com/chaconne-ai/spreader) cluster.**

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
| Exchanger | `ProcessingSyncService.applicationExchanger(key)` | `Exchanger` |
| Replicated cache | `ProcessingCache` | a small Redis |
| Process pool | `ProcessingPool`, or `@MultiProcessingCall` on any bean method | a work queue |
| Cluster scheduling | `@MultiProcessingScheduled` | ShedLock |
| RPC | `@RpcClient` | Feign for internal calls |
| MapReduce | `ProcessingMapReduce`, with a `MapReduceJob` bean | a batch framework, for jobs of this size |
| DAG workflows | `ProcessingDag`, building a `StateGraph` | a workflow engine, for graphs of this size |

## What to expect in practice

| | |
|---|---|
| **Reads never leave the process** | Every node holds a full replica of the cache, so a read is a local map lookup. Measured at over ten million bit tests per second against roughly two thousand cross-node writes per second. That gap is the design, and it says which workloads fit: read-heavy, tolerant of a few milliseconds of staleness. |
| **The cache replicates operations, not data** | `setbit` on a 100MB bitmap ships one datagram, not the bitmap. That is what makes a cluster-wide Bloom filter practical rather than theoretical. |
| **Two scopes for every component** | `application*` confines a lock, a semaphore or a latch to instances of the *same* application; `cluster*` spans everything sharing the cluster port. The choice is per call, not per deployment. |
| **A rendezvous is not a transaction** | `ProcessingExchanger` pairs two parties and moves an item between them, but the item crosses the network through the leader. A change of leader mid-pairing loses it, which is why timing out returns the item rather than dropping it. Where an item must never be lost, use a queue with storage behind it. |
| **The process pool tells you which side you are on** | A call runs locally when no peer is available and behaves exactly as before, so a single instance is a valid deployment. `spreader_pool_remote_ratio` says whether work is genuinely being spread or you have a local thread pool with extra steps. |
| **Failures arrive as one exception type** | Everything surfaces as `ProcessingException`, split by cause rather than summed into a single rate, so a timeout and a serialization error never look alike on a dashboard. Business exceptions from your own remote code are passed through unwrapped. |
| **Observability without extra work** | Metrics register with Micrometer and reach `/actuator/prometheus`; cluster health joins the actuator endpoints and carries each member's HTTP address, so one node's health response is enough to reach any other. |
| **Inherited limits** | Election is pre-emptive rather than consensus-based, and the cache is memory only. Under a network partition two sides can each hold a leader, and a full cluster restart starts from empty. Both are stated in [Limits](#limits) rather than discovered later. |

## Is this the right tool?

| Your situation | |
|---|---|
| Several instances of a Spring Boot service that need a lock, a shared counter, or a job that runs once | **Yes. This is the case it was built for** |
| You want a read-heavy shared cache and can tolerate a few milliseconds of staleness | Yes. Reads are local and roughly a thousand times cheaper than writes |
| You need the cache to survive a full cluster restart | No. It is memory only, by decision. Use Redis if the data must outlive the processes |
| "This must never run twice, **ever**": money moves, or a ledger is written | **No.** That needs consensus-based election, or a database transaction |
| You are not on Spring Boot | Use [`spreader`](https://github.com/chaconne-ai/spreader) directly, the library underneath this one |
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
  - [Workflows: the DAG engine](#workflows-the-dag-engine)
- [Performance](#performance-and-what-it-tells-you-about-the-design)
- [Recommended configuration](#recommended-configuration)
  - [Which application may become the leader](#which-application-may-become-the-leader)
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
[`spreader`](https://github.com/chaconne-ai/spreader), which runs inside the same JVM as your beans, so a
lock acquired on the leader costs a message rather than a network service call.


## Installation

> **This is a snapshot release.** Snapshots do not live in the Maven Central
> release repository, so the repository below has to be declared as well or the
> dependency will not resolve.

**Maven**

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

**Gradle**

```groovy
implementation 'com.chaconne-ai:openspreader:1.0.0-SNAPSHOT'
```

```groovy
repositories {
    mavenCentral()
    maven { url 'https://central.sonatype.com/repository/maven-snapshots/' }
}
```

### Requirements

| | |
|---|---|
| **Java** | 17 or later |
| **Spring Boot** | 4.1, built and tested against it; see the note below |
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
@SpringBootApplication
@EnableRpcClients(basePackages = "com.example.client")   // switches on the scan
public class Application { }

@RpcClient(serviceId = "inventory-service",
           timeout = 3000,
           maxConcurrent = 50,                           // in-flight ceiling, fail fast when full
           fallback = InventoryFallback.class)
public interface InventoryClient {

    int available(String sku);
}
```

Routing is by application name, not URL. Nodes come and go; there is nothing to
update. The server side needs a bean with a matching method carrying
`@MultiProcessingCall`. Set `maxConcurrent`: without it, requests pile up locally
when the downstream slows, and what falls over is your own process.

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

### Workflows: the DAG engine

Several steps, some of which depend on each other and some of which do not. Written by hand
that becomes a tangle of futures, and the part that always goes wrong is not the happy path:
it is what a conditional's unchosen branch does to a join further down.

Turn it on with `spring.spreader.multiprocessing.dag.enabled=true`, on every instance, and
describe the graph:

```java
CompiledGraph flow = dag.bind(StateGraph.create("order-flow")
        .channel("trail", Reducers.concatList())     // how parallel writes merge
        .input("orderId")                            // required at invoke time

        .from(Validate.class).to(Charge.class)       // dependency
        .from(Charge.class).to(Ship.class, Notify.class)   // fan-out, in parallel
        .from(Ship.class, Notify.class).to(Settle.class)   // fan-in, waits for both

        .from(Score.class)                           // conditional
            .switchOn("#risk > 80 ? 'manual' : 'auto'")
            .caseOf("manual", HumanReview.class)
            .caseOf("auto", AutoApprove.class)

        .from(Charge.class).onFailure().to(Refund.class)   // compensation
        .retry(Charge.class, 2)                            // retried before that edge is taken

        .entry(Validate.class)
        .compile());

RunResult result = flow.invoke(Map.of("orderId", id));
```

A node is an ordinary Spring bean extending `GraphNode`, returning the channels it wrote.
Every node is dispatched through the process pool, so the steps of one workflow run on
different instances, and a single instance is still a valid deployment.

| | |
|---|---|
| **It builds nothing of its own** | Dispatch is the process pool, dynamic fan-out is `ProcessingMapReduce`, the reply cache that makes a retried dispatch safe is the pool's. The engine is the weaving |
| **The graph is checked at `compile()`** | Cycles, unreachable nodes, an unreachable quorum, and two parallel branches writing one channel with no reducer declared, which is the one that would otherwise lose a write in silence |
| **A skipped branch does not hang a join** | An unchosen branch is marked skipped and that propagates, so a fan-in downstream completes instead of waiting for something that will never arrive |
| **Parallel merges are ordered by node name** | Not by arrival, so the same graph gives the same answer on a different day |
| **A step can be a whole graph** | `SubGraph` is a node that is itself a graph, which is how a workflow stays readable past a dozen steps |
| **A step can be outside this application** | `ExternalNode` plus `local(...)` calls an HTTP API from the coordinating instance rather than shipping the call to a peer |
| **The definition can be stored, and drawn** | `JsonRenderer` and `YamlRenderer` write a graph out and read it back, with a `GraphCatalog` supplying the node classes and nothing resolved by `Class.forName`. `flow.toMermaid()` draws the graph, `result.toMermaid()` draws a finished run with each node coloured by what became of it |
| **Retries can back off** | `retry(Charge.class, 2, Backoff.exponential(Duration.ofMillis(200)))`, with jitter where fifty runs would otherwise retry in the same instant. The waiting costs no thread: the coordinator's loop simply wakes no later than the next attempt falls due |
| **A run can be stopped** | `dag.cancel(runId)` stops dispatching and stops waiting. It does not reach into another replica to interrupt work already running there, because that node may be halfway through a payment. What finished is still reported and still resumable |
| **It reports to Micrometer like everything else** | `spreader.dag.*`: runs started, succeeded, failed, cancelled, in flight, plus nodes run, failed, skipped, dispatched, local, and retries. Nothing per node name, which is how a cardinality problem starts |
| **Only failures worth repeating are repeated** | `retryWhen(Charge.class, classifier::classify)` takes a predicate, so the `BinaryExceptionClassifier` an application already configures for Spring Retry plugs straight in. "Insufficient funds" is a complete answer; asking it twice more costs two backoffs and changes nothing |
| **One run reads as one thing across replicas** | The run, graph and node names reach the log context on whichever instance runs the node, with no dependency at all. A real distributed trace is ten lines of your tracer through the `TracePropagation` seam, because which tracer to use is the application's decision |

#### `A.class` names a node, it does not mean "this class"

A node's name is its class's simple name unless you give it one. So `A.class` in an edge
means **the node called A**, and writing the same class twice does not produce two nodes, it
produces one:

```java
.from(Validate.class).to(Validate.class)
// DagException: graph order-flow is not acyclic: Validate -> Validate
```

That is refused at `compile()` rather than at run time, and the message says what it really
is: one node with an edge to itself.

**To use one class as two steps, name them.** A graph that validates on the way in and
validates again after enrichment wants one bean and two nodes:

```java
CompiledGraph flow = dag.bind(StateGraph.create("order-flow")
        .node("Validate", Check.class)          // one class
        .node("Recheck",  Check.class)          // two nodes
        .from("Validate").to(Enrich.class)
        .from(Enrich.class).to("Recheck")
        .retry("Recheck", 2)                    // each carries its own settings
        .entry("Validate")
        .compile());
```

Named nodes are referred to by string everywhere a class would do: `from`, `to`, `caseOf`,
`retry`, `local`, `entry`, and `RunResult.statusOf(...)`. Mixing the two is fine, and
`Check.class` still means the node called `Check`.

Two consequences worth knowing before you reach for this:

| | |
|---|---|
| **One name cannot be two classes** | `node("X", A.class)` then `node("X", B.class)` is refused at build time, naming both classes. A name is the node's identity, and quietly letting the second win would mean the graph you drew is not the graph that runs |
| **A node is not told its own name** | `execute(GraphState)` receives the state and nothing else, so two nodes of one class cannot behave differently by name. Let the difference come from a channel (`state.contains("enriched")`), which suits the state model, or write two small subclasses. Dispatch is by class name, so both nodes run on the **same bean instance**: like any Spring bean, it must be stateless |

#### The definition, stored and read back

`flow.render()` writes the graph out; `load(text, catalog)` reads it back. Below is
`SettlementFlowBestPractice` as Jackson writes it, which is the shape a workflow takes in a
database column. It is **abridged to one example of each shape**: two of its seven nodes and
two of its seven edges, chosen as the ones the table underneath refers to. Run the example to
see the whole thing.

```json
{
  "graph" : "daily-settlement",
  "entries" : [ "LoadMerchants" ],
  "inputs" : [ "day", "dryRun" ],
  "channels" : [ {
    "name" : "steps",
    "reducer" : "concatList"
  }, {
    "name" : "payouts",
    "reducer" : "writeOnce"
  } ],
  "nodes" : [ {
    "name" : "TransferFunds",
    "type" : "com.acme.settlement.TransferFunds",
    "kind" : "node",
    "entry" : false,
    "local" : true,
    "trigger" : "ALL",
    "retries" : 2
  }, {
    "name" : "Archive",
    "type" : "com.acme.settlement.Archive",
    "kind" : "node",
    "entry" : false,
    "local" : false,
    "trigger" : "ANY",
    "retries" : 0
  } ],
  "edges" : [ {
    "from" : "Total",
    "to" : "TransferFunds",
    "kind" : "conditional",
    "condition" : "ON_SUCCESS",
    "branch" : "0"
  }, {
    "from" : "TransferFunds",
    "to" : "FlagForOperator",
    "kind" : "plain",
    "condition" : "ON_FAILURE"
  } ],
  "conditionals" : [ {
    "sources" : [ "Total" ],
    "form" : "predicate",
    "expression" : null,
    "predicates" : [ "#total != null and #total > 0" ],
    "branches" : [ {
      "key" : "0",
      "targets" : [ "TransferFunds" ]
    } ],
    "else" : [ "ArchiveOnly" ]
  } ]
}
```

Four things in there are worth pointing out, because each answers a question people ask
about the format:

| | |
|---|---|
| **`retries`, `local` and `trigger` sit on the node** | They are properties of the step, not of an edge. `"retries": 2` is three attempts in all; `"local": true` is the bank call that runs on the coordinating instance; `"trigger": "ANY"` is the archive step firing on whichever branch got there first |
| **`condition` is what the edge waits for** | `ON_SUCCESS` for the ordinary path, `ON_FAILURE` for the compensation edge. That one field is the whole of failure routing |
| **The switches appear twice, deliberately** | Flattened into `edges` because drawing wants a flat list, and kept whole in `conditionals` because rebuilding wants the structure. A `when` chain stores its conditions as text and numbers its branches, hence `"branch": "0"` |
| **The code is not in here** | `type` is a name the `GraphCatalog` looks up, never a `Class.forName`. Listeners are left out on purpose: they are a run-time concern, attached where the graph is used. A reducer or a condition written as a lambda has no text to store, and loading says so by name rather than handing back a graph quietly missing a rule |

`RendererType.YAML` writes the same structure in a shape that diffs legibly in a pull
request. Both read back.

#### Persistence and resuming are the application's, and here is what it gets

There is **no store and no scheduler in here**, by decision. This is a component package: it
supplies the primitives, and where a half-finished run lives, how long it is kept and what
wakes it up are answered by the application, which is the only part of the system that knows.

What that decision obliges is the other half: the seams have to be real, or "the application
does it" is not something an application can actually do. They are these.

**Every run has an identity, and every callback carries it.** One listener instance sees
every run of the graph, and two can be in flight at once, so without this a record built
from callbacks would interleave them:

```java
RunResult r = flow.invoke(state, "settlement-2026-09-13");   // or let one be generated
r.runId();                                                    // the same value
```

**Write each node down as it finishes.** `onTransition` reports edges, and a terminal node
has none, so a record built from transitions alone is missing the last step of every branch.
`onNodeFinished` reports the node:

```java
.listener(new GraphListener() {
    @Override
    public void onStart(String runId, CompiledGraph graph, GraphState initial) {
        runs.open(runId, graph.render(), initial.asMap());     // the definition, as text
    }

    @Override
    public void onNodeFinished(String runId, NodeOutcome outcome) {
        runs.record(runId, outcome.node(), outcome.ok(), outcome.updates());
    }
})
```

**Hand it back to carry on.** Nodes named in `completed` are treated as done and are not
dispatched, which on a step that has already moved money is the whole point:

```java
RunResult again = flow.resume(runs.stateOf(runId), runs.completedIn(runId), runId);
```

`RunResult.state()` and `RunResult.completed()` are shaped to be stored and handed back
unchanged. Three things about `resume` are worth knowing before relying on it:

| | |
|---|---|
| **A skipped node is not in `completed()`** | A skip is worked out, not something that happened. Storing one and restoring it would freeze it: a step skipped because the run failed above it would stay skipped after the failure was fixed, and the resumed run would finish having done nothing while looking healthy. Left out, it is recomputed, and a conditional on a restored node is evaluated again and passes over the same branch |
| **A failed node is** | As FAILED, so resuming carries on down the compensation path exactly as the first run did. Removing it from the map first is how you say "try that step again" |
| **A node interrupted mid-flight runs again** | It never finished, so it is not in the record |
| **And so does one that finished without its row landing** | The record is written by `onNodeFinished`, which runs on the coordinating instance. A node that ran, reported back, and whose row was not yet written when that instance died is a step that **happened** and is **not in the record**. Resuming is therefore **at-least-once, not exactly-once**, and no arrangement of this engine changes that: an effect outside the process cannot be made atomic with a row inside it. A lookup pays nothing for that; a payment pays twice, which is what `NodeContext.idempotencyKey()` is for |

**Waiting, timers and human steps** are the same answer in a different shape, and they do
not need an API here: cut the graph in two, and let whatever knows when to continue (a
scheduler, a queue, a callback endpoint) resume the second half. A node that blocks waiting
for a person would hold a pool thread for as long as the person took.

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
# set on an application that should join but never lead, such as an API facade
spring.spreader.leader-eligible=false

# --- turn on only what you use ---
spring.spreader.multiprocessing.mutex.enabled=true
spring.spreader.multiprocessing.cache.enabled=true
spring.spreader.multiprocessing.scheduling.enabled=true
# the DAG engine needs this set on every instance, since every one of them
# has to be able to run a node it is handed
spring.spreader.multiprocessing.dag.enabled=true

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

### Which application may become the leader

Several applications often share one cluster name so that they can see each
other: the one doing the work, plus an API facade, a batch job, a console. They
are not equally suited to leading. The leader holds the lock and permit
registers and the authoritative cache copy, which wants a long lived, evenly
loaded instance with several replicas. A facade that scales to zero overnight and
restarts on every deploy will take the port just as readily, and what you get is
a change of leader on every deployment.

```properties
# per application, and it is the Spring Boot application that this is about,
# not the individual instance
spring.spreader.leader-eligible=false
```

Such a node joins normally, gossips normally and takes dispatched work normally.
It never claims the cluster port, it is skipped when the others work out whose
turn it is to take over, and it advertises this through member metadata so the
others do not have to guess. Older nodes that predate the flag read it as a key
they do not recognise, so a rolling upgrade is safe.

Setting it on **every** application is a misconfiguration, and one worth
understanding rather than guarding against: the leader is also the rendezvous
point for discovery, since discovery knocks on the cluster port and the holder of
that port is the leader. With nobody holding it the members cannot find each
other either, and each node sits alone with a member list of one. The engine
reports this as a periodic warning instead of promoting someone anyway, which
would override an explicit configuration. Starting one eligible application
restores the leader and the member views together, with no restart of the
followers.

Until then, components that need a leader fail rather than pretend: `acquire`
returns false instead of handing out a lock nobody else recognises. That is the
same set of deliberate degradations as [What happens during an
election](#what-happens-during-an-election), with one difference that matters:
an election ends, this does not until someone changes the configuration.

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
| `MapReduceBestPractice` | splitting a job, and jobs worth splitting |
| `ExchangerBestPractice` | handing an item between two parties |
| `OrderFlowBestPractice` | the DAG engine's five shapes in one order workflow |
| `SettlementFlowBestPractice` | a DAG with retries, compensation, a bank call and a sharded batch |

These are tested, not illustrative. The tests exist because example code fails
in a particular way: it is off the main path, so a change that alters its
*behaviour* still compiles, and nobody notices until someone copies it.

## Limits

- **Election is pre-emptive, not consensus-based.** Under a partition both sides
  can elect a leader; it heals when the partition does. The two are different
  routes rather than different grades: pre-emptive election suits coordination
  where a repeat is wasteful, and work where a repeat causes real harm belongs
  with a consensus protocol, or behind a fencing token, or in a database
  transaction.
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
