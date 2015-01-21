package org.societies.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Represents a AbstractDatabase
 */
public abstract class AbstractDatabase implements Database {

    public static final String DB_DATASOURCE_CLASS = "db-datasource-class";

    /** The number of maximum connections */
    public static final int MAX_CONNECTIONS = 10;

    /** The number of maximum idle connections */
    public static final long MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30);


    private final String dataSourceClass;

    private final SQLDialect dialect;

    /** Gets the data sources from teh connection pool */
    private HikariDataSource dataSource;

    /** The DSL context, use with jOOQ stuff */
    private DSLContext dslContext;

    public AbstractDatabase(String dataSourceClass,
                            SQLDialect dialect) {
        this.dataSourceClass = dataSourceClass;
        this.dialect = dialect;
    }

    public abstract HikariConfig createConfig();

    @Override
    public void initDatabase() {
        this.dataSource = new HikariDataSource(createConfig());

        // Create a MySQL DSL context
        Settings settings = new Settings();
        settings.setStatementType(StatementType.PREPARED_STATEMENT);
        dslContext = DSL.using(dataSource, dialect, settings);
    }

    /**
     * Returns a connection from the pool
     *
     * @return A connection
     */
    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Closes the connection/statement pool
     */
    @Override
    public boolean close() {

        if (dataSource == null) {
            return true;
        }

        try {
            // Try to close the connection
            dataSource.shutdown();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close the connection pool!", e);
        }

        return true;
    }

    public String getDataSourceClass() {
        return dataSourceClass;
    }

    public SQLDialect getDialect() {
        return dialect;
    }

    /**
     * Returns the DSL context used in this connection.
     *
     * @see org.jooq.DSLContext
     */
    @Override
    public DSLContext getDSLContext() {
        return dslContext;
    }
}
