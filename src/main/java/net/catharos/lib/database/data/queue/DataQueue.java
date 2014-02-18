package net.catharos.lib.database.data.queue;

/**
 * Represents a DataQueue
 */
public interface DataQueue<T extends Data> {

    boolean isReady();

    void offer(T executor);
}
