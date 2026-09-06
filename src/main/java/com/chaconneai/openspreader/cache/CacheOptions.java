package com.chaconneai.openspreader.cache;

/**
 * Every tunable parameter of the cache.
 *
 * <p>Gathered in one place rather than spread across a constructor's parameters, because all
 * of them are externalised to configuration
 * ({@code spring.spreader.multiprocessing.cache.*}) and more will be added. A long run of
 * {@code long}s side by side is something the compiler says nothing about when the order is
 * wrong.
 *
 * <p>Validation lives here rather than at the point of use: a misconfiguration should surface
 * at startup, not when a full synchronisation fails and reveals that the snapshot timeout was
 * set to a negative number.
 *
 * @param replicationName       the replication scope, an application name. null or blank
 *                              means the whole cluster
 * @param requestTimeoutMs      ceiling on one write's total time, retries included.
 *                              <b>It must exceed the cluster's takeover time</b>, or a write
 *                              in flight during a failover is certain to fail
 * @param retryIntervalMs       how long before retrying a failed write
 * @param gapTimeoutMs          how long to wait on a version gap before pulling a full snapshot
 * @param sweepIntervalMs       how often the leader sweeps expired keys
 * @param snapshotTimeoutMs     how long to wait for a full snapshot; proportional to the key
 *                              count
 * @param maintenanceIntervalMs the background maintenance interval; 0 derives it from
 *                              gapTimeout and sweepInterval
 * @param snapshotChunkBytes    ceiling on one snapshot chunk, which must stay within the
 *                              transport's per-frame limit
 * @param maxBatchSize          the most updates packed into one frame; it caps replication
 *                              throughput
 * @param maxBatchBytes         the most bytes in one frame; 0 takes it from the transport
 *                              protocol
 * @param outboxCapacity        capacity of the broadcast queue. When full, updates are
 *                              dropped and receivers pull a full snapshot
 * @param inboundThreads        threads handling inbound requests
 * @param maxKeys               ceiling on the key count; 0 is unlimited
 * @param maxBytes              approximate ceiling on content bytes. -1 is automatic, taking
 *                              25% of the maximum heap; 0 is unlimited
 * @param evictionPolicy        which entry goes first when full
 * @param evictionSamples       how many candidates are sampled per eviction
 * @param evictionBatch         the most evicted in one maintenance round
 * @param accessReportIntervalMs how often followers report read accesses; 0 disables it,
 *                               which leaves LRU blind to local reads and largely ineffective
 * @param accessReportSampleRate the sampling rate of those reports
 * @param accessReportMaxKeys   the most keys accumulated in one round
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record CacheOptions(
        String replicationName,
        long requestTimeoutMs,
        long retryIntervalMs,
        long gapTimeoutMs,
        long sweepIntervalMs,
        long snapshotTimeoutMs,
        long maintenanceIntervalMs,
        int snapshotChunkBytes,
        int maxBatchSize,
        int maxBatchBytes,
        int outboxCapacity,
        int inboundThreads,
        long maxKeys,
        long maxBytes,
        EvictionPolicy evictionPolicy,
        int evictionSamples,
        int evictionBatch,
        long accessReportIntervalMs,
        int accessReportSampleRate,
        int accessReportMaxKeys) {

    public CacheOptions {
        replicationName = replicationName == null || replicationName.isBlank()
                ? null : replicationName.trim();
        requestTimeoutMs = positive(requestTimeoutMs, "request-timeout-ms");
        retryIntervalMs = positive(retryIntervalMs, "retry-interval-ms");
        gapTimeoutMs = positive(gapTimeoutMs, "gap-timeout-ms");
        sweepIntervalMs = positive(sweepIntervalMs, "sweep-interval-ms");
        snapshotTimeoutMs = positive(snapshotTimeoutMs, "snapshot-timeout-ms");
        if (maintenanceIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "spring.spreader.multiprocessing.cache.maintenance-interval-ms must not be negative");
        }
        snapshotChunkBytes = (int) positive(snapshotChunkBytes, "snapshot-chunk-bytes");
        maxBatchSize = (int) positive(maxBatchSize, "max-batch-size");
        if (maxBatchBytes < 0) {
            throw new IllegalArgumentException(
                    "spring.spreader.multiprocessing.cache.max-batch-bytes must not be negative");
        }
        outboxCapacity = (int) positive(outboxCapacity, "outbox-capacity");
        inboundThreads = (int) positive(inboundThreads, "inbound-threads");
        if (maxKeys < 0 || accessReportIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "spring.spreader.multiprocessing.cache max-keys and access-report-interval-ms "
                            + "must not be negative");
        }
        if (maxBytes < -1) {
            throw new IllegalArgumentException("spring.spreader.multiprocessing.cache.max-bytes "
                    + "must be -1 (automatic), 0 (unlimited) or a positive number");
        }
        if (maxBytes == -1) {
            // A fixed absolute value would be wrong: one default would exhaust the heap in a
            // 512MB container and waste most of a 32GB machine. Only a proportion of the heap
            // makes a single default work everywhere.
            // A quarter rather than more, because the accounting itself is approximate --
            // object headers and bucket arrays are not counted -- so real usage is typically
            // 20% to 50% higher, and the application needs heap of its own.
            maxBytes = Runtime.getRuntime().maxMemory() / 4;
        }
        evictionPolicy = evictionPolicy == null ? EvictionPolicy.LRU : evictionPolicy;
        evictionSamples = (int) positive(evictionSamples, "eviction-samples");
        evictionBatch = (int) positive(evictionBatch, "eviction-batch");
        accessReportSampleRate = (int) positive(accessReportSampleRate, "access-report-sample-rate");
        accessReportMaxKeys = (int) positive(accessReportMaxKeys, "access-report-max-keys");
        if (evictionPolicy != EvictionPolicy.NONE && maxKeys == 0 && maxBytes == 0) {
            // A policy with no limit set is no policy at all, and a half-finished configuration
            // like this is exactly what leaves someone believing they are protected
            throw new IllegalArgumentException("spring.spreader.multiprocessing.cache.eviction-policy="
                    + evictionPolicy + " but both max-keys and max-bytes are 0, so no limit can "
                    + "ever trigger eviction. Either set a limit, or set eviction-policy to NONE");
        }
        if (outboxCapacity < maxBatchSize) {
            // A queue that cannot hold one batch means a batch never fills, so the setting
            // achieves nothing
            throw new IllegalArgumentException(
                    "spring.spreader.multiprocessing.cache.outbox-capacity(" + outboxCapacity
                            + ") must not be below max-batch-size(" + maxBatchSize + ")");
        }
    }

    /** Whether eviction needs access information. RANDOM and NONE do not, which saves the
     *  recording cost on the read path. */
    public boolean needsAccessTracking() {
        return evictionPolicy == EvictionPolicy.LRU || evictionPolicy == EvictionPolicy.LFU;
    }

    /** Whether any limit is set. Without one, eviction never triggers. */
    public boolean hasLimit() {
        return maxKeys > 0 || maxBytes > 0;
    }

    /** The effective maintenance interval: whatever was configured, or half of the tighter of
     *  the two intervals when nothing was. */
    public long effectiveMaintenanceIntervalMs() {
        if (maintenanceIntervalMs > 0) {
            return maintenanceIntervalMs;
        }
        return Math.max(100L, Math.min(gapTimeoutMs, sweepIntervalMs) / 2);
    }

    private static long positive(long v, String name) {
        if (v <= 0) {
            throw new IllegalArgumentException("spring.spreader.multiprocessing.cache." + name
                    + " must be greater than 0, but is " + v);
        }
        return v;
    }
}
