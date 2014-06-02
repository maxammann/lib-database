package net.catharos.lib.database.jbdc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.catharos.lib.database.Database;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.conf.StatementType;
import org.jooq.impl.DSL;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * The controller for the database connections.
 * Holds the connection pool and every else important.
 */
@Singleton
public class RemoteDatabase implements Database {

    /** The number of maximum connections */
    private static final int MAX_CONNECTIONS = 10;

    /** The number of maximum idle connections */
    private static final long MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30);

    public static final String DB_HOST_KEY = "db-host";
    public static final String DB_PORT_KEY = "db-port";
    public static final String DB_DATABASE_KEY = "db-database";
    public static final String DB_USERNAME_KEY = "db-username";
    public static final String DB_PASSWORD_KEY = "db-password";
    public static final String DB_DATASOURCE_CLASS = "db-datasource-class";

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String dataSourceClass;

    private final SQLDialect dialect;

    /** Gets the data sources from teh connection pool */
    private HikariDataSource dataSource;

    /** The DSL context, use with jOOQ stuff */
    private DSLContext dslContext;

    /**
     * Creates a new connection controller and automatically tries to connect to the database.
     * @param host     The host
     * @param port     The port
     * @param database The database
     * @param username The username
     * @param password The password
     * @param dataSourceClass The class of the datasource to use
     * @param dialect The sql dialect to use
     */
    @Inject
    public RemoteDatabase(@Named(DB_HOST_KEY) String host,
                          @Named(DB_PORT_KEY) int port,
                          @Named(DB_DATABASE_KEY) String database,
                          @Named(DB_USERNAME_KEY) String username,
                          @Named(DB_PASSWORD_KEY) String password,
                          @Named(DB_DATASOURCE_CLASS) String dataSourceClass,
                          SQLDialect dialect) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.dataSourceClass = dataSourceClass;
        this.dialect = dialect;

        initDatabase();
    }

    public void initDatabase() {
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(MAX_CONNECTIONS);
        config.setMaxLifetime(MAX_LIFETIME);
        config.setDataSourceClassName(dataSourceClass);
        config.addDataSourceProperty("serverName", host);
        config.addDataSourceProperty("port", port);
        config.addDataSourceProperty("databaseName", database);
        config.addDataSourceProperty("user", username);
        config.addDataSourceProperty("password", password);

        dataSource = new HikariDataSource(config);

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

        try {
            // Try to close the connection
            dataSource.shutdown();
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
