package net.catharos.lib.database.data;

import net.catharos.lib.database.DSLProvider;
import net.catharos.lib.database.data.queue.Data;
import net.catharos.lib.database.data.queue.DataException;
import net.catharos.lib.database.data.queue.Queue;
import org.jooq.DSLContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a DataWorker
 */
@Singleton
public final class DataWorker implements Runnable {
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final DSLProvider dslProvider;

    private final Queue dataQueue;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition idle = lock.newCondition();
    private final Condition ready = lock.newCondition();

    private volatile boolean running = true;

    @Inject
    public DataWorker(Thread.UncaughtExceptionHandler exceptionHandler, DSLProvider dslProvider, Queue dataQueue) {
        this.exceptionHandler = exceptionHandler;
        this.dslProvider = dslProvider;
        this.dataQueue = dataQueue;
    }

    @Override
    public void run() {
        while (running) {
            try {
                lock.lock();
                boolean batchReady = dataQueue.isReady();

                if (!batchReady) {
                    idle.signal();

                    try {
                        ready.await();
                    } catch (InterruptedException e) {
                        break;
                    }

                    batchReady = dataQueue.isReady();
                }

                if (batchReady) {
                    if (dataQueue.isFlushPending()) {
                        dataQueue.flushReady();
                    }

                    dataQueue.execute(dslProvider.getDSLContext());
                }
            } catch (DataException e) {
                exceptionHandler.uncaughtException(Thread.currentThread(), e);
            } finally {
                lock.unlock();
            }
        }
    }

    public void publishBatch(Data data) {
        try {
            lock.lock();
            dataQueue.publishBatch(data);
            ready.signal();
        } finally {
            lock.unlock();
        }
    }


    public void publishSingle(Data data) {
        try {
            lock.lock();
            dataQueue.publishSingle(data);
            ready.signal();
        } finally {
            lock.unlock();
        }
    }

    public void stop() throws InterruptedException {
        try {
            lock.lock();
            dataQueue.flushAll();
            ready.signal();
            idle.await();
        } finally {
            lock.unlock();
        }

        this.running = false;
    }

    public static Thread createDefaultThread(DataWorker executor) {
        return new Thread(executor, executor.toString());
    }

    @Override
    public String toString() {
        DSLContext context = dslProvider.getDSLContext();
        return "DataWorker{" +
                "connection=" + "DataWorker["
                + (context != null
                ? context.configuration().connectionProvider()
                : "")
                + "]" +
                ", running=" + running +
                '}';
    }
}
