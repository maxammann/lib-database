package net.catharos.lib.database.data.queue;

import java.util.LinkedList;

/**
 * Represents a AbstractQueue
 */
public abstract class AbstractQueue<T extends Data> implements DataQueue<T> {

    private final LinkedList<T> queue = new LinkedList<>();

    public T poll() {
        return queue.poll();
    }

    @Override
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
