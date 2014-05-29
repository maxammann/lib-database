package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryKey;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;

import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Represents a BatchQueue
 */
public final class BatchQueue {

    public static final int CRITICAL_BATCH_SIZE = 100;
    public static final long DEAD_LINE = TimeUnit.MINUTES.toMillis(5);

    public static final long AUTO_FLUSH_INTERVAL = TimeUnit.SECONDS.toMillis(10);

    private final CopyOnWriteArrayList<BatchEntry> queuesList = new CopyOnWriteArrayList<BatchEntry>();
    private final LinkedList<BatchEntry> queue = new LinkedList<BatchEntry>();


    private long lastAutoFlush = System.currentTimeMillis();

    public void execute(DSLContext context) throws RuntimeException {
        BatchEntry entry;

        while ((entry = queue.poll()) != null) {
            try {
                BatchBindStep batch = context.batch(entry.getQuery());
                entry.execute(batch);
                batch.execute();
            } catch (RuntimeException e) {
                queue.offer(entry);
                throw e;
            }
        }
    }

    public boolean isReady() {
        return !queue.isEmpty();
    }

    public void flushReady() {
        for (BatchEntry entry : queuesList) {
            if (entry.isReady()) {
                queue.offer(entry);
            }
        }
    }

    public void flushAll() {
        for (BatchEntry entry : queuesList) {
            flush(entry);
        }
    }

    public void flush(BatchEntry entry) {
        queue.offer(entry);
    }

    public boolean isAutoFlushPending() {
        return (System.currentTimeMillis() - AUTO_FLUSH_INTERVAL) > lastAutoFlush;
    }

    public void offer(Data executor) {
        QueryKey key = executor.getQueryKey();
        BatchEntry entry = getBatchEntry(key);

        if (entry == null) {
            queuesList.add(entry = new BatchEntry(key, executor.getQueryProvider()));
        }

        entry.offer(executor);

        if (entry.reachedSize()) {
            flush(entry);
        }
    }

    public BatchEntry getBatchEntry(QueryKey query) {
        for (BatchEntry entry : queuesList) {
            if (entry.getQueryKey().hashCode() == query.hashCode()
                    && entry.getQueryKey().equals(query)) {
                return entry;
            }
        }

        return null;
    }

}
