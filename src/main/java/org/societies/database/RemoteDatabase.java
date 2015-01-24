package org.societies.database;

import com.zaxxer.hikari.HikariConfig;
import org.jooq.SQLDialect;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * The controller for the database connections.
 * Holds the connection pool and every else important.
 */
@Singleton
public class RemoteDatabase extends AbstractDatabase {

    public static final String DB_HOST_KEY = "database.mysql.host";
    public static final String DB_PORT_KEY = "database.mysql.port";
    public static final String DB_DATABASE_KEY = "database.mysql.database";
    public static final String DB_USERNAME_KEY = "database.mysql.username";
    public static final String DB_PASSWORD_KEY = "database.mysql.password";

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    /**
     * Creates a new connection controller and automatically tries to connect to the database.
     *
     * @param host            The host
     * @param port            The port
     * @param database        The database
     * @param username        The username
     * @param password        The password
     * @param dataSourceClass The class of the datasource to use
     * @param dialect         The sql dialect to use
     */
    @Inject
    public RemoteDatabase(@Named(DB_HOST_KEY) String host,
                          @Named(DB_PORT_KEY) int port,
                          @Named(DB_DATABASE_KEY) String database,
                          @Named(DB_USERNAME_KEY) String username,
                          @Named(DB_PASSWORD_KEY) String password,
                          @Named(DB_DATASOURCE_CLASS) String dataSourceClass,
                          SQLDialect dialect) {
        super(dataSourceClass, dialect);
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }


    @Override
    public HikariConfig createConfig() {
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(MAX_CONNECTIONS);
        config.setMaxLifetime(MAX_LIFETIME);
        config.setDataSourceClassName(getDataSourceClass());
        config.addDataSourceProperty("serverName", host);
        config.addDataSourceProperty("port", port);
        config.addDataSourceProperty("databaseName", database);
        config.addDataSourceProperty("user", username);
        config.addDataSourceProperty("password", password);

        return config;

    }

}
