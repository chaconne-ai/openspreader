package com.chaconneai.openspreader.metrics;

import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.metrics.ChannelMetrics;
import com.chaconneai.spreader.metrics.LatencySnapshot;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;

/**
 * Exposes spreader's channel metrics through Micrometer, so Prometheus can scrape them at
 * {@code /actuator/prometheus}.
 *
 * <h2>There is one source of data</h2>
 * This <b>collects nothing</b>; it re-formats the data spreader already holds. Instrumenting
 * twice inevitably produces two sets of numbers that disagree, and nobody can say which to
 * believe.
 *
 * <h2>Why everything is a Gauge, with no Counter or Timer</h2>
 * Micrometer's {@code Counter} and {@code Timer} expect to do the accumulating and timing
 * themselves, whereas this data is <b>already computed</b> -- the layer below accumulates
 * lock-free on the hot path and this only reads. Forcing a Counter on it would mean either
 * counting twice, or moving the instrumentation up to this layer, which would make two sets
 * of it.
 *
 * <p>A Gauge reads the current value, which is exactly what a snapshot means. The price is
 * that cumulative quantities need {@code increase()} rather than {@code rate()} on the
 * Prometheus side -- but TPS is given directly anyway, so nothing needs computing there.
 *
 * <h2>Channels are dynamic</h2>
 * Channel names are not known in advance, since a user may start a new one at any time, so
 * this scans periodically and registers whatever is new. Anything already registered is not
 * registered again; {@link #registered} keeps track.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 20/08/2026
 */
public class SpreaderMeterBinder implements MeterBinder {

    /** The metric prefix. In Grafana, this prefix alone gathers every metric from this project. */
    private static final String PREFIX = "spreader.channel";

    private final MetricsService metrics;
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private volatile MeterRegistry registry;

    public SpreaderMeterBinder(MetricsService metrics) {
        this.metrics = metrics;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        this.registry = registry;
        bindKnownChannels();
    }

    /**
     * Registers every channel and buffer currently known.
     *
     * <p>Called periodically by the auto-configuration: channels and buffers both appear only
     * at runtime -- thread pools are created lazily -- and at the moment of startup there are
     * usually none at all.
     */
    public void bindKnownChannels() {
        MeterRegistry r = registry;
        if (r == null) {
            return;
        }
        for (String channel : metrics.channels().keySet()) {
            if (registered.add("channel:" + channel)) {
                bindChannel(r, channel);
            }
        }
        for (BufferMetrics buffer : metrics.buffers()) {
            if (registered.add("buffer:" + buffer.name())) {
                bindBuffer(r, buffer.name());
            }
        }
    }

    /**
     * Buffer fill levels.
     *
     * <h2>These deserve alerts more than latency does</h2>
     * Latency and error rate describe problems that have <b>already happened</b>; fill level
     * describes one that is <b>on its way</b>. And {@code dropped} is the most important of
     * them: when a buffer fills, messages are discarded outright, and neither the sender nor
     * the receiving application learns of it -- without watching this number, overload makes
     * no sound at all.
     *
     * <p>Suggested alerts: {@code spreader_buffer_dropped > 0} (alert outright, with no
     * threshold) and {@code spreader_buffer_usage > 0.7} sustained for five minutes.
     */
    private void bindBuffer(MeterRegistry registry, String name) {
        Tags tags = Tags.of("buffer", name);
        bufferGauge(registry, "size", tags, "how many are queued now", name, BufferMetrics::pending);
        bufferGauge(registry, "capacity", tags, "the ceiling", name, BufferMetrics::capacity);
        bufferGauge(registry, "usage", tags, "fill level, 0 to 1", name, BufferMetrics::usage);
        bufferGauge(registry, "dropped", tags,
                "total discarded; non-zero means messages really were lost",
                name, BufferMetrics::dropped);
        bufferGauge(registry, "drop.rate", tags, "drop rate, 0 to 1", name, BufferMetrics::dropRate);
        bufferGauge(registry, "handled", tags, "total processed", name, BufferMetrics::handled);
    }

    private void bufferGauge(MeterRegistry registry, String name, Tags tags, String desc,
                             String buffer, ToDoubleFunction<BufferMetrics> reader) {
        Gauge.builder("spreader.buffer." + name, buffer,
                        b -> metrics.buffers().stream()
                                .filter(m -> m.name().equals(b))
                                .mapToDouble(reader)
                                .findFirst()
                                .orElse(0d))
                .tags(tags)
                .description(desc)
                .register(registry);
    }

    private void bindChannel(MeterRegistry registry, String channel) {
        // The empty string, meaning the default channel, is awkward to query in Prometheus;
        // give it an explicit name
        Tags tags = Tags.of("channel", channel.isEmpty() ? "default" : channel);

        gauge(registry, "sent", tags, "messages sent successfully", ChannelMetrics::sent, channel);
        gauge(registry, "send.failures", tags, "failed sends",
                m -> m.sendFailures(), channel);
        gauge(registry, "retries", tags, "retransmissions", m -> m.retries(), channel);
        gauge(registry, "received", tags, "messages received and processed successfully",
                m -> m.received(), channel);
        gauge(registry, "receive.failures", tags, "messages whose handler failed",
                m -> m.receiveFailures(), channel);
        gauge(registry, "duplicates", tags, "duplicates turned away by de-duplication",
                m -> m.duplicates(), channel);

        gauge(registry, "concurrency", tags, "current concurrency: messages being handled",
                ChannelMetrics::inflight, channel);
        gauge(registry, "concurrency.peak", tags, "peak concurrency",
                ChannelMetrics::peakInflight, channel);

        gauge(registry, "tps.sent", tags, "outbound TPS: messages sent in the last complete second",
                ChannelMetrics::sentTps, channel);
        gauge(registry, "tps.received", tags, "inbound TPS: messages received in the last complete second",
                ChannelMetrics::receivedTps, channel);
        gauge(registry, "tps.peak", tags, "peak TPS", ChannelMetrics::peakTps, channel);

        gauge(registry, "error.rate", tags, "error rate, 0 to 1",
                ChannelMetrics::errorRate, channel);
        gauge(registry, "retry.rate", tags, "retry rate, 0 to 1",
                ChannelMetrics::retryRate, channel);

        // Latencies are in seconds throughout, which is the Prometheus convention -- it is
        // what makes Grafana recognise them as durations
        latency(registry, "outbound", tags,
                "outbound latency, from sending to the acknowledgement", channel, true);
        latency(registry, "inbound", tags, "inbound handler time", channel, false);
    }

    private void latency(MeterRegistry registry, String direction, Tags tags,
                         String desc, String channel, boolean outbound) {
        Tags withDir = tags.and("direction", direction);
        gauge(registry, "latency.min", withDir, desc + ", minimum",
                m -> nanosToSeconds(pick(m, outbound).minNanos()), channel);
        gauge(registry, "latency.avg", withDir, desc + ", mean",
                m -> pick(m, outbound).avgNanos() / 1_000_000_000d, channel);
        gauge(registry, "latency.max", withDir, desc + ", maximum",
                m -> nanosToSeconds(pick(m, outbound).maxNanos()), channel);
        gauge(registry, "latency.p50", withDir, desc + " P50", channel, outbound, 50);
        gauge(registry, "latency.p95", withDir, desc + " P95", channel, outbound, 95);
        gauge(registry, "latency.p99", withDir, desc + " P99", channel, outbound, 99);
    }

    private void gauge(MeterRegistry registry, String name, Tags tags, String desc,
                       String channel, boolean outbound, int quantile) {
        gauge(registry, name, tags, desc, m -> {
            var s = pick(m, outbound);
            long nanos = switch (quantile) {
                case 50 -> s.p50Nanos();
                case 95 -> s.p95Nanos();
                default -> s.p99Nanos();
            };
            return nanosToSeconds(nanos);
        }, channel);
    }

    private void gauge(MeterRegistry registry, String name, Tags tags, String desc,
                       ToDoubleFunction<ChannelMetrics> reader, String channel) {
        Gauge.builder(PREFIX + "." + name, channel, c -> reader.applyAsDouble(metrics.channel(c)))
                .tags(tags)
                .description(desc)
                .register(registry);
    }

    private static LatencySnapshot pick(ChannelMetrics m, boolean outbound) {
        return outbound ? m.outboundLatency() : m.inboundProcessing();
    }

    private static double nanosToSeconds(long nanos) {
        return nanos / 1_000_000_000d;
    }

    /** The channels registered so far, for diagnostics. */
    public Map<String, Boolean> registeredChannels() {
        Map<String, Boolean> out = new ConcurrentHashMap<>();
        registered.forEach(c -> out.put(c, Boolean.TRUE));
        return out;
    }
}
