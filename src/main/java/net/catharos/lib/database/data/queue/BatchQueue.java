package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Query;

import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Represents a BatchQueue
 */
public final class BatchQueue implements DataQueue<Data> {

    public static final int CRITICAL_BATCH_SIZE = 100;
    public static final long DEAD_LINE = TimeUnit.MINUTES.toMillis(5);

    public static final long AUTO_FLUSH_INTERVALL = TimeUnit.SECONDS.toMillis(10);

    private final CopyOnWriteArrayList<BatchEntry> queuesList = new CopyOnWriteArrayList<>();
    private final LinkedList<BatchEntry> queue = new LinkedList<>();
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

    @Override
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
        return (System.currentTimeMillis() - AUTO_FLUSH_INTERVALL) > lastAutoFlush;
    }

    @Override
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

    private static final class BatchEntry extends AbstractQueue<Data> {

        private final long created;
        private final QueryKey key;
        private final QueryProvider provider;

        public BatchEntry(QueryKey key, QueryProvider provider) {
            this.key = key;
            this.provider = provider;
            this.created = System.currentTimeMillis();
        }

        public QueryKey getQueryKey() {
            return key;
        }

        public Query getQuery() {
            return provider.getQuery(key);
        }

        @Override
        public void offer(Data executor) {
            super.offer(executor);
        }

        @Override
        public Data poll() {
            return super.poll();
        }

        public void execute(BatchBindStep query) throws RuntimeException {
            Data data;

            while ((data = poll()) != null) {
                try {
                    query.bind(data.execute());
                } catch (RuntimeException e) {
                    offer(data);
                    throw e;
                }
            }
        }

        private boolean reachedSize() {
            return getQueueSize() == CRITICAL_BATCH_SIZE || reachedDeadLine();
        }

        private boolean reachedDeadLine() {
            return (System.currentTimeMillis() - DEAD_LINE) > created;
        }

        @Override
        public boolean isReady() {
            return getQueueSize() >= CRITICAL_BATCH_SIZE || reachedDeadLine();
        }
    }
}
