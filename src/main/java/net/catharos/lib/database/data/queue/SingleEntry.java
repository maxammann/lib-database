package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;
import org.jooq.DSLContext;
import org.jooq.Query;

/**
 * Represents a SingleEntry
 */
public class SingleEntry implements Entry {

    private final Data data;

    public SingleEntry(Data data) {this.data = data;}

    @Override
    public void execute(DSLContext context) throws DataException {

        Object[] obj;

        try {
            obj = data.execute();
        } catch (RuntimeException e) {
            throw new DataException(e);
        }

        QueryKey key = data.getQueryKey();
        QueryProvider provider = data.getQueryProvider();

        Query query = provider.getQuery(key);

        for (int i = 0; i < obj.length; i++) {
            query.bind(i, obj);
        }

        query.execute();
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
