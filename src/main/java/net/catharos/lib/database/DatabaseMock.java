package net.catharos.lib.database;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Represents a DatabaseMock
 */
public class DatabaseMock {

    public static DSLContext mockedDSLContext(MockDataProvider provider) {
        Connection connection = new MockConnection(provider);
        return DSL.using(connection, SQLDialect.MYSQL);
    }

    public static DSLContext mockedDSLContext() {
        return mockedDSLContext(new MockDataProvider() {
            @Override
            public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
                return new MockResult[0];
            }
        });
    }
}
