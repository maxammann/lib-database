package org.societies.database;

import gnu.trove.map.hash.THashMap;
import net.catharos.lib.core.lang.Closable;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;

import java.util.UUID;

/**
 * Provides queries
 */
public abstract class QueryProvider implements Closable {

    private final DSLProvider provider;
    private THashMap<QueryKey, QueryBuilder> builders = new THashMap<QueryKey, QueryBuilder>();

    public static final UUID DEFAULT_UUID = new UUID(0L, 0L);

    public static final byte[] DEFAULT_BYTE_ARRAY = new byte[0];


    public static final String DEFAULT_STRING = "";
    public static final UInteger DEFAULT_UINTEGER = UInteger.valueOf(0);
    public static final Double DEFAULT_DOUBLE = 0d;
    public static final short DEFAULT_SHORT = 0;
    public static final DateTime DEFAULT_TIMESTAMP = DateTime.now();

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

    protected static UUID id_param() {
        return new UUID(0L, 0L);
    }

    protected static Param<UUID> uuid_param() {
        return DSL.param("uuid", UUID.class);
    }

    protected static Param<UUID> uuid_param(String name) {
        return DSL.param(name, UUID.class);
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

    public <R extends Record> Result<R> query(final QueryKey<? extends Select<R>> key) {
        return query(getQuery(key));
    }

    public <R extends Record> Result<R> query(final Select<R> query) {
        return query.fetch();
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

