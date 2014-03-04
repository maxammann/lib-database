package net.catharos.lib.database;

import gnu.trove.set.hash.THashSet;

import java.util.Set;

/**
 * Represents a DatabaseContext
 */
public class DatabaseContext {
    private final Database database;

    /** A list of caches for queries */
    private final Set<QueryProvider> queryProviders;


    protected DatabaseContext(Database database) {
        this(database, new THashSet<QueryProvider>());
    }

    protected DatabaseContext(Database database, Set<QueryProvider> providers) {
        this.database = database;
        this.queryProviders = new THashSet<>(providers);
    }

    /**
     * Registers a new query cache class.
     *
     * @param cache The cache instance
     * @return True on success, otherwise false
     */
    public boolean addQueryProvider(QueryProvider cache) {
        cache.cache();

        return queryProviders.add(cache);
    }

    public Database getDatabase() {
        return database;
    }
}
