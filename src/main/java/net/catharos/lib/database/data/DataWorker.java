package net.catharos.lib.database.data;

import net.catharos.lib.database.data.queue.BatchQueue;
import net.catharos.lib.database.data.queue.Data;
import net.catharos.lib.database.data.queue.QueryQueue;
import org.jooq.DSLContext;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a DataWorker
 */
public final class DataWorker implements Runnable {
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final DSLContext context;

    private final QueryQueue queryQueue = new QueryQueue();
    private final BatchQueue batchQueue = new BatchQueue();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition idle = lock.newCondition();
    private final Condition ready = lock.newCondition();
    private volatile boolean running = true;

    public DataWorker(Thread.UncaughtExceptionHandler exceptionHandler, DSLContext context) {
        this.exceptionHandler = exceptionHandler;
        this.context = context;
    }

    @Override
    public void run() {
        while (running) {
            try {
                lock.lock();
                boolean batchReady = batchQueue.isReady();
                boolean queryReady = queryQueue.isReady();

                if (!batchReady && !queryReady) {
                    idle.signal();

                    try {
                        ready.await();
                    } catch (InterruptedException e) {
                        break;
                    }

                    batchReady = batchQueue.isReady();
                    queryReady = queryQueue.isReady();
                }

                if (batchReady) {
                    try {
                        if (batchQueue.isAutoFlushPending()) {
                            batchQueue.flushReady();
                        }
                        batchQueue.execute(context);
                    } catch (RuntimeException e) {
                        exceptionHandler.uncaughtException(Thread.currentThread(), e);
                    }
                }

                if (queryReady) {
                    try {
                        queryQueue.execute();
                    } catch (RuntimeException e) {
                        exceptionHandler.uncaughtException(Thread.currentThread(), e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void addQuery(Data data) {
        try {
            lock.lock();
            queryQueue.offer(data);
            ready.signal();
        } finally {
            lock.unlock();
        }
    }

    public void addBatch(Data data) {
        try {
            lock.lock();
            batchQueue.offer(data);
            ready.signal();
        } finally {
            lock.unlock();
        }
    }

    public void stop() throws InterruptedException {
        try {
            lock.lock();
            batchQueue.flushAll();
            ready.signal();
            idle.await();
        } finally {
            lock.unlock();
        }

        this.running = false;
    }

    public DSLContext getContext() {
        return context;
    }

    /**
     * @param exceptionHandler The exception handler
     * @param context          The dsl context
     * @return A default DataWorker
     */
    public static DataWorker createExecutor(Thread.UncaughtExceptionHandler exceptionHandler, DSLContext context) {
        return new DataWorker(exceptionHandler, context);
    }

    public static Thread createDefaultThread(DataWorker executor) {
        return new Thread(executor, executor.toString());
    }

    @Override
    public String toString() {
        return "DataWorker{" +
                "connection=" + "DataWorker["
                + (getContext() != null
                ? getContext().configuration().connectionProvider()
                : "")
                + "]" +
                ", running=" + running +
                '}';
    }
}
