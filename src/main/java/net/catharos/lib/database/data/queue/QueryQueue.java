package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryProvider;
import org.jooq.Query;

/**
 * Represents a QueryQueue
 */
public final class QueryQueue extends AbstractQueue<Data> {

    public void execute() throws RuntimeException {
        Data data;

        while ((data = poll()) != null) {
            QueryProvider provider = data.getQueryProvider();

            try {
                Object[] obj = data.execute();

                Query query = provider.getQuery(data.getQueryKey());

                for (int i = 0; i < obj.length; i++) {
                    query.bind(i, obj);
                }

                query.execute();
            } catch (RuntimeException e) {
                offer(data);
                throw e;
            }
        }
    }

    public boolean isReady() {
        return !isEmpty();
    }
}
