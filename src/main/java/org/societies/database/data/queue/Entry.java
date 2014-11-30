package org.societies.database.data.queue;

import org.jooq.DSLContext;

/**
 *
 */
public interface Entry {

    public void execute(DSLContext context) throws DataException;

    boolean isReady();
}
