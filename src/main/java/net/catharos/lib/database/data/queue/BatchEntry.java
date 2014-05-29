package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;
import org.jooq.BatchBindStep;
import org.jooq.Query;

/**
* Represents a BatchEntry
*/
final class BatchEntry extends AbstractQueue<Data> {

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

    public boolean reachedSize() {
        return getQueueSize() == BatchQueue.CRITICAL_BATCH_SIZE || reachedDeadLine();
    }

    public boolean reachedDeadLine() {
        return (System.currentTimeMillis() - BatchQueue.DEAD_LINE) > created;
    }

    public boolean isReady() {
        return getQueueSize() >= BatchQueue.CRITICAL_BATCH_SIZE || reachedDeadLine();
    }
}
