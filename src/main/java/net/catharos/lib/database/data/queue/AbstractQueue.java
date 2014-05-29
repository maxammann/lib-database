package net.catharos.lib.database.data.queue;

import java.util.LinkedList;

/**
 * Represents a AbstractQueue
 */
public abstract class AbstractQueue<T extends Data> {

    private final LinkedList<T> queue = new LinkedList<T>();

    public T poll() {
        return queue.poll();
    }

    public void offer(T executor) {
        queue.offer(executor);
    }

    protected boolean isEmpty() {
        return queue.isEmpty();
    }

    protected int getQueueSize() {
        return queue.size();
    }
}
