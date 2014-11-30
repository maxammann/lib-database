package org.societies.database.data;


import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.societies.database.DSLProvider;
import org.societies.database.DatabaseMock;
import org.societies.database.QueryKey;
import org.societies.database.QueryProvider;
import org.societies.database.data.queue.Data;
import org.societies.database.data.queue.DefaultQueue;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a DataWorkerTest
 */
@RunWith(JUnit4.class)
public class DataWorkerTest {

    public static final int TRIES = 1000;

    private AtomicInteger queries = new AtomicInteger();

    private DSLContext dslContext = DatabaseMock.mockedDSLContext(new MockDataProvider() {

        @Override
        public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
            queries.addAndGet(ctx.batchBindings().length);
            return new MockResult[0];
        }
    });

    private DSLProvider dslProvider = new DSLProvider() {
        @Override
        public DSLContext getDSLContext() {
            return dslContext;
        }
    };

    @Test
    public void testQueues() {
        long start = System.nanoTime();

        DataWorker controller = new DataWorker(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                throw new AssertionError(e);
            }
        }, dslProvider, new DefaultQueue(5, 5, TimeUnit.MINUTES, 50));

        Thread thread = DataWorker.createDefaultThread(controller);
        thread.start();

        final QueryKey<Query> TEST_KEY = new QueryKey<Query>();

        final QueryProvider provider = new QueryProvider(dslProvider) {
            @Override
            public void build() {
                builder(TEST_KEY, new QueryBuilder<Query>() {
                    @Override
                    public Query create(DSLContext context) {
                        return dslContext.query("SELECT 1");
                    }
                });
            }
        };

        for (int i = 0; i < TRIES; i++) {

            controller.publishBatch(new Data() {

                @Override
                public QueryProvider getQueryProvider() {
                    return provider;
                }

                @Override
                public QueryKey getQueryKey() {
                    return TEST_KEY;
                }

                @Override
                public Object[] execute() {
                    return new Object[0];
                }
            });

            controller.publishSingle(new Data() {

                @Override
                public QueryProvider getQueryProvider() {
                    return provider;
                }

                @Override
                public QueryKey getQueryKey() {
                    return TEST_KEY;
                }

                @Override
                public Object[] execute() {
                    return new Object[0];
                }
            });
        }

        try {
            controller.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        long finish = System.nanoTime();
        System.out.printf("Check took %s!%n", (double) (finish - start));

        Assert.assertEquals(TRIES * 2, queries.get());
    }
}
