package org.societies.database.data.queue;

import org.societies.database.QueryKey;
import org.societies.database.QueryProvider;

/**
 * Represents a Data
 */
public interface Data {

    QueryProvider getQueryProvider();

    QueryKey getQueryKey();

    Object[] execute();
}
