package org.societies.database.data.queue;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.societies.database.QueryKey;
import org.societies.database.QueryProvider;

import java.util.LinkedList;

/**
 * Represents a BatchEntry
 */
final class BatchEntry implements Entry {

    private final LinkedList<Data> queue = new LinkedList<Data>();
    private final long created;
    private final DefaultQueue.BatchSettings settings;

    private final QueryKey key;
    private final QueryProvider provider;

    public BatchEntry(DefaultQueue.BatchSettings settings, QueryKey key, QueryProvider provider) {
        this.settings = settings;
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

    public void offer(Data data) {
        queue.offer(data);
    }

    public Data poll() {
        return queue.poll();
    }

    @Override
    public void execute(DSLContext context) throws DataException {
        BatchBindStep batch = context.batch(getQuery());

        Data data;
        while ((data = poll()) != null) {
            Object[] execute;

            try {
                execute = data.execute();
            } catch (RuntimeException e) {
                offer(data);
                throw new DataException(e);
            }

            batch.bind(execute);
        }

        batch.execute();
    }

    private boolean reachedDeadLine() {
        long maxIdle = settings.getMaxIdle();
        return (System.currentTimeMillis() - maxIdle) > created;
    }

    @Override
    public boolean isReady() {
        int criticalBatchSize = settings.getCriticalBatchSize();
        return queue.size() >= criticalBatchSize || reachedDeadLine();
    }
}
