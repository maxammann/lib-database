package net.catharos.lib.database;

import net.catharos.lib.core.lang.Closable;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The controller for the database connections.
 * Holds the connection pool and every else important.
 */
@Singleton
public class Database implements Closable, DSLProvider {
    /** The default MySQL database port */
    public static final int DEFAULT_PORT = 3306;

    /** The name of the MySQL driver class */
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    /** The query used for connection validation */
    private static final String VALIDATION_QUERY = "SELECT 1";

    /** The number of maximum active connections */
    private static final int MAX_ACTIVE = 10;

    /** The number of maximum idle connections */
    private static final int MAX_IDLE = 3;
    public static final int STMT_MAX_IDLE = 16;
    public static final int STMT_MAX_ACTIVE = 32;

    public static final String DB_HOST_KEY = "db-host";
    public static final String DB_PORT_KEY = "db-port";
    public static final String DB_DATABASE_KEY = "db-database";
    public static final String DB_USERNAME_KEY = "db-username";
    public static final String DB_PASSWORD_KEY = "db-password";

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    /** The pool of the database connections (idle and active) */
    private GenericObjectPool connectionPool;

    /** Gets the data sources from teh connection pool */
    private PoolingDataSource dataSource;

    /** The DSL context, use with jOOQ stuff */
    private DSLContext dslContext;

    /**
     * Creates a new connection controller and automatically tries to connect to the database.
     *
     * @param host     The host
     * @param port     The port
     * @param database The database
     * @param username The username
     * @param password The password
     */
    @Inject
    public Database(@Named(DB_HOST_KEY) String host,
                    @Named(DB_PORT_KEY) int port,
                    @Named(DB_DATABASE_KEY) String database,
                    @Named(DB_USERNAME_KEY) String username,
                    @Named(DB_PASSWORD_KEY) String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;

        initDatabase();
    }

    public void initDatabase() {
        // Load the MySQL driver
        try {
            Class.forName(MYSQL_DRIVER);

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not find mysql driver!");
        }

        // Create a connection pool and set up connection limits
        connectionPool = new GenericObjectPool();
        connectionPool.setMaxActive(MAX_ACTIVE);
        connectionPool.setMaxIdle(MAX_IDLE);

        // Construct connection factory
        ConnectionFactory factory = new DriverManagerConnectionFactory(
                String.format("jdbc:mysql://%s:%d/%s", host, port, database),
                username,
                password
        );

        GenericKeyedObjectPoolFactory stmtPoolFactory = new GenericKeyedObjectPoolFactory(
                null,
                STMT_MAX_ACTIVE,
                GenericKeyedObjectPool.DEFAULT_WHEN_EXHAUSTED_ACTION,
                GenericKeyedObjectPool.DEFAULT_MAX_WAIT,
                STMT_MAX_IDLE);

        // Create a poolable connection to the database
        PoolableConnectionFactory pcf = new PoolableConnectionFactory(
                factory,
                connectionPool,
                stmtPoolFactory,
                VALIDATION_QUERY,
                false,
                true
        );

        // Set factory for the connection pool and set the datasource to a pooling data source
        connectionPool.setFactory(pcf);
        dataSource = new PoolingDataSource(connectionPool);

        // Create a MySQL DSL context
        Settings settings = new Settings();
        settings.setStatementType(StatementType.PREPARED_STATEMENT);
        dslContext = DSL.using(dataSource, SQLDialect.MYSQL, settings);
    }

    /**
     * Returns a connection from the pool
     *
     * @return A connection
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }


    /**
     * Closes the connection/statement pool
     */
    @Override
    public boolean close() {

        try {
            // Try to close the connection
            connectionPool.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close the connection pool!", e);
        }

        return true;
    }

    /**
     * Returns the DSL context used in this connection.
     *
     * @see DSLContext
     */
    @Override
    public DSLContext getDSLContext() {
        return dslContext;
    }
}
