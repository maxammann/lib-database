package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Query;

import java.util.concurrent.TimeUnit;

/**
 * Represents a BatchEntry
 */
final class BatchEntry extends AbstractQueue<Data> implements Entry {

    public static final int CRITICAL_BATCH_SIZE = 100;
    public static final long DEAD_LINE = TimeUnit.MINUTES.toMillis(5);

    public static final long AUTO_FLUSH_INTERVAL = TimeUnit.SECONDS.toMillis(10);


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
    public void offer(Data data) {
        super.offer(data);
    }

    @Override
    public Data poll() {
        return super.poll();
    }

    @Override
    public void execute(DSLContext context) throws RuntimeException {
        BatchBindStep batch = context.batch(getQuery());
        Data data;

        while ((data = poll()) != null) {
            try {
                batch.bind(data.execute());
            } catch (RuntimeException e) {
                offer(data);
                throw e;
            }
        }

        batch.execute();
    }

    private boolean reachedDeadLine() {
        return (System.currentTimeMillis() - DEAD_LINE) > created;
    }

    @Override
    public boolean isReady() {
        return getQueueSize() >= CRITICAL_BATCH_SIZE || reachedDeadLine();
    }
}
