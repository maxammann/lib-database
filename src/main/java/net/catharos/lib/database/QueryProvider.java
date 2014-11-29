package net.catharos.lib.database;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import gnu.trove.map.hash.THashMap;
import net.catharos.lib.core.lang.Closable;
import org.jooq.*;
import org.jooq.types.UInteger;

import java.sql.Timestamp;
import java.util.concurrent.Callable;

/**
 * Provides queries
 */
public abstract class QueryProvider implements Closable {

    private final DSLProvider provider;
    private THashMap<QueryKey, QueryBuilder> builders = new THashMap<QueryKey, QueryBuilder>();

    public static final byte[] DEFAULT_BYTE_ARRAY = new byte[0];
    public static final byte[] DEFAULT_UUID = DEFAULT_BYTE_ARRAY;
    public static final String DEFAULT_STRING = "";
    public static final UInteger DEFAULT_UINTEGER = UInteger.valueOf(0);
    public static final Double DEFAULT_DOUBLE = 0d;
    public static final short DEFAULT_SHORT = 0;
    public static final Timestamp DEFAULT_TIMESTAMP = new Timestamp(System.currentTimeMillis());

    protected QueryProvider(DSLProvider provider) {
        this.provider = provider;
        build();
    }

    public abstract void build();

    /**
     * Caches one specific query builder.
     *
     * @param key     The key of the query
     * @param builder The query builder
     */
    public final <Q extends Query> void builder(QueryKey<Q> key, QueryBuilder<Q> builder) {
        builders.put(key, builder);
    }

    /**
     * Gets/Creates a query.
     *
     * @param key The key of the query
     * @param <Q> The type of the query
     * @return The query from the cache
     */
    public <Q extends Query> Q getQuery(QueryKey<Q> key) {
        QueryBuilder query = builders.get(key);

        if (query == null) {
            throw new IllegalStateException("Query " + key + " not found!");
        }

        //beautify cache results, but only for specific threads and thread unique
        if (provider.getDSLContext() == null) {
            throw new RuntimeException("Database not initialized!");
        }
        return key.toQuery(query.create(provider.getDSLContext()));
    }

    public <R extends Record> ListenableFuture<Result<R>> query(ListeningExecutorService service, final QueryKey<? extends Select<R>> key) {
        return query(service, getQuery(key));
    }

    public <R extends Record> ListenableFuture<Result<R>> query(ListeningExecutorService service, final Select<R> query) {
        return service.submit(new Callable<Result<R>>() {
            @Override
            public Result<R> call() throws Exception {
                return query.fetch();
            }
        });
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

