package net.catharos.lib.database.data.queue;

import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;

/**
 * Represents a Data
 */
public interface Data {

    QueryProvider getQueryProvider();

    QueryKey getQueryKey();

    Object[] execute();
}
