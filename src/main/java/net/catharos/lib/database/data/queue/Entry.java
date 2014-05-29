package net.catharos.lib.database.data.queue;

import org.jooq.DSLContext;

/**
 *
 */
public interface Entry {

    public void execute(DSLContext context) throws RuntimeException;

    boolean isReady();
}
