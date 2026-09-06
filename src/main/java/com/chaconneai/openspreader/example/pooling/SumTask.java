package com.chaconneai.openspreader.example.pooling;

import com.chaconneai.openspreader.pooling.RecursiveTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The classic range sum, demonstrating fork/join across processes: 1 to 100 is split up,
 * sub-ranges may well be computed <b>in other processes</b>, and the results are merged.
 *
 * <p>It is written exactly as a {@code java.util.concurrent.RecursiveTask} would be. The
 * one additional requirement is that the task class and its fields must be serialisable,
 * since the whole object is sent to the peer.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class SumTask extends RecursiveTask<Long> {

    private static final long serialVersionUID = 1L;

    /** Below this size it is computed directly rather than split further. */
    private static final int THRESHOLD = 10;

    private final int from;
    private final int to;

    /** Records which process computed this range, so tests can observe the distribution. */
    private final List<String> trace = new ArrayList<>();

    public SumTask(int from, int to) {
        this.from = from;
        this.to = to;
    }

    @Override
    protected Long compute() {
        if (to - from + 1 <= THRESHOLD) {
            long sum = 0;
            for (int i = from; i <= to; i++) {
                sum += i;
            }
            ExecutionTrace.record(from, to, depth());
            return sum;
        }

        int mid = (from + to) >>> 1;
        // A forked subtask may be sent to another process; past the dispatch depth limit it
        // stays local automatically
        CompletableFuture<Long> left = fork(new SumTask(from, mid));
        CompletableFuture<Long> right = fork(new SumTask(mid + 1, to));
        return join(left) + join(right);
    }

    public int from() {
        return from;
    }

    public int to() {
        return to;
    }
}
