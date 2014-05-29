package net.catharos.lib.database.data.queue;

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
public final class DefaultQueue {

    private final int criticalBatchSize;
    private final long maxIdle, autoFlushInterval;

    private final THashMap<QueryKey, BatchEntry> queues = new THashMap<QueryKey, BatchEntry>();
    private long lastAutoFlush = System.currentTimeMillis();

    private final LinkedList<Entry> pending = new LinkedList<Entry>();


    public DefaultQueue(long autoFlushInterval, long maxIdle, TimeUnit unit, int criticalBatchSize) {
        this.criticalBatchSize = criticalBatchSize;
        this.autoFlushInterval = unit.toMillis(autoFlushInterval);
        this.maxIdle = unit.toMillis(maxIdle);
    }

    public void execute(DSLContext context) throws RuntimeException {
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

    public void batchOffer(Data data) {
        QueryKey key = data.getQueryKey();
        QueryProvider provider = data.getQueryProvider();
        BatchEntry entry = getBatchEntry(key);

        if (entry == null) {
            queues.put(key, entry = new BatchEntry(key, provider));
        }

        entry.offer(data);

        batchOffer(entry);
    }

    public void batchOffer(BatchEntry entry) {
        if (entry.isReady()) {
            flush(entry);
        }
    }

    public void singleOffer(Data data) {
        flush(new SingleEntry(data));
    }

    public boolean isReady() {
        return !pending.isEmpty();
    }

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

    public void flushAll() {
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

    public void flush(Entry entry) {
        pending.offer(entry);
    }

    @Nullable
    public BatchEntry getBatchEntry(QueryKey key) {
        return queues.get(key);
    }

    public boolean isAutoFlushPending() {
        return (System.currentTimeMillis() - autoFlushInterval) > lastAutoFlush;
    }

}
