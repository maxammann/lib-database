package net.catharos.lib.database;

import javax.inject.Provider;

/**
 * Represents a DatabaseFactory
 */
public class DatabaseFactory implements Provider<Database> {

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    private Database lazy;

    public DatabaseFactory(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    @Override
    public Database get() {
        if (lazy == null) {
            lazy = new Database(host, port, database, username, password);
        }

        return lazy;
    }
}
