package net.catharos.lib.database.data.queue;

import com.google.inject.name.Named;
import gnu.trove.map.hash.THashMap;
import gnu.trove.procedure.TObjectProcedure;
import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * Represents a BatchQueue
 */
public final class DefaultQueue implements Queue {

    private final long autoFlushInterval;
    private final BatchSettings batchSettings;

    private final THashMap<QueryKey, BatchEntry> queues = new THashMap<QueryKey, BatchEntry>();
    private long lastAutoFlush = System.currentTimeMillis();

    private final LinkedList<Entry> pending = new LinkedList<Entry>();


    public DefaultQueue(@Named("auto-flush-interval") long autoFlushInterval,
                        @Named("max-batch-idle") long maxIdle,
                        @Named("queue-time-unit") TimeUnit unit,
                        @Named("critical-batch-size") int criticalBatchSize) {
        this.autoFlushInterval = unit.toMillis(autoFlushInterval);
        this.batchSettings = new BatchSettings(criticalBatchSize, unit.toMillis(maxIdle));
    }

    @Override
    public void execute(DSLContext context) throws DataException {
        Entry entry;

        while ((entry = pending.poll()) != null) {
            try {
                entry.execute(context);
            } catch (RuntimeException e) {
                pending.offer(entry);
                throw e;
            }
        }
    }

    @Override
    public void publishBatch(Data data) {
        QueryKey key = data.getQueryKey();
        QueryProvider queryProvider = data.getQueryProvider();

        BatchEntry entry = getBatchEntry(key);

        if (entry == null) {
            queues.put(key, entry = new BatchEntry(batchSettings, key, queryProvider));
        }

        entry.offer(data);

        publishBatch(entry);
    }

    public void publishBatch(BatchEntry entry) {
        if (entry.isReady()) {
            flush(entry);
        }
    }

    @Override
    public void publishSingle(Data data) {
        flush(new SingleEntry(data));
    }

    @Override
    public boolean isReady() {
        return !pending.isEmpty();
    }

    @Override
    public void flushReady() {
        queues.forEachValue(new TObjectProcedure<BatchEntry>() {
            @Override
            public boolean execute(BatchEntry entry) {
                if (entry.isReady()) {
                    flush(entry);
                }
                return true;
            }
        });
    }

    @Override
    public void flushAll() {
        queues.forEachValue(new TObjectProcedure<BatchEntry>() {
            @Override
            public boolean execute(BatchEntry entry) {
                flush(entry);
                return true;
            }
        });
    }

    public void flush(Entry entry) {
        pending.offer(entry);
    }

    @Nullable
    public BatchEntry getBatchEntry(QueryKey key) {
        return queues.get(key);
    }

    @Override
    public boolean isFlushPending() {
        return (System.currentTimeMillis() - autoFlushInterval) > lastAutoFlush;
    }


    final static class BatchSettings {
        private final int criticalBatchSize;
        private final long maxIdle;

        private BatchSettings(int criticalBatchSize, long maxIdle) {
            this.criticalBatchSize = criticalBatchSize;
            this.maxIdle = maxIdle;
        }

        public long getMaxIdle() {
            return maxIdle;
        }

        public int getCriticalBatchSize() {
            return criticalBatchSize;
        }
    }
}
