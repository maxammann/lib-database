package net.catharos.lib.database.data;


import net.catharos.lib.database.DSLProvider;
import net.catharos.lib.database.DatabaseMock;
import net.catharos.lib.database.QueryKey;
import net.catharos.lib.database.QueryProvider;
import net.catharos.lib.database.data.queue.Data;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a DataWorkerTest
 */
@RunWith(JUnit4.class)
public class DataWorkerTest {

    public static final int TRIES = 1000000;
    private AtomicInteger queries = new AtomicInteger();

    @Test
    public void testQueues() {
        long start = System.nanoTime();

        final DSLContext dslContext = DatabaseMock.mockedDSLContext(new MockDataProvider() {

            @Override
            public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
                queries.addAndGet(ctx.batchBindings().length);
                return new MockResult[0];
            }
        });


        DataWorker controller = new DataWorker(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                throw new AssertionError(e);
            }
        }, dslContext);

        Thread thread = DataWorker.createDefaultThread(controller);
        thread.start();

        final QueryKey<Query> TEST_KEY = new QueryKey<>();

        final QueryProvider provider = new QueryProvider(new DSLProvider() {
            @Override
            public DSLContext getDSLContext() {
                return dslContext;
            }
        }) {
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

            controller.addQuery(new Data() {

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

            controller.addBatch(new Data() {

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
            e.printStackTrace();
        }

        long finish = System.nanoTime();
        System.out.printf("Check took %s!%n", (double) (finish - start) / (double) (TRIES * 2));

        Assert.assertEquals(TRIES * 2, queries.get());
    }
}
