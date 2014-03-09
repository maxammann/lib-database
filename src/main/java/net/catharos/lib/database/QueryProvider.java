package net.catharos.lib.database;

import gnu.trove.map.hash.THashMap;
import net.catharos.lib.core.lang.Closable;
import org.jooq.DSLContext;
import org.jooq.Query;

/**
 * Provides queries
 */
public abstract class QueryProvider implements Closable {

    private final DSLProvider provider;
    private THashMap<QueryKey, QueryBuilder> builders = new THashMap<>();

    protected QueryProvider(DSLProvider provider) {
        this.provider = provider;
    }

    public abstract void cache();

    /**
     * Caches one specific query builder.
     *
     * @param key    The key of the query
     * @param builder The query builder
     */
    public final <Q extends Query> void cache(QueryKey<Q> key, QueryBuilder<Q> builder) {
        builders.put(key, builder);
    }

    /**
     * Gets/Creates a query.
     *
     * @param key  The key of the query
     * @param <Q> The type of the query
     * @return The query from the cache
     */
    public <Q extends Query> Q getQuery(QueryKey<Q> key) {
        QueryBuilder query = builders.get(key);

        if (query == null) {
            throw new IllegalStateException("Query " + key + " not found!");
        }

        //todo cache results, but only for specific threads and thread unique
        return key.toQuery(query.create(provider.getDSLContext()));
    }

    @Override
    public boolean close() {
        builders.clear();
        return true;
    }

    protected static interface QueryBuilder<Q extends Query> {

        Q create(DSLContext context);

    }
}

