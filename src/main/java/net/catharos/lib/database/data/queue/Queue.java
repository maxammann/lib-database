package net.catharos.lib.database.data.queue;

import org.jooq.DSLContext;

/**
 *
 */
public interface Queue {

    void execute(DSLContext context) throws DataException;

    void publishBatch(Data data);

    void publishSingle(Data data);

    boolean isReady();

    void flushReady();

    void flushAll();

    boolean isFlushPending();
}
