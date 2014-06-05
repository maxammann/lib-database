package net.catharos.lib.database;

import com.zaxxer.hikari.HikariConfig;
import org.jooq.SQLDialect;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class URLDatabase extends AbstractDatabase {

    private final String url;
    private final String driverClass;

    @Inject
    public URLDatabase(@Named("db-url") String url,
                       @Named(DB_DATASOURCE_CLASS) String dataSourceClass,
                       @Named("db-driver") String driverClass,
                       SQLDialect dialect) {
        super(dataSourceClass, dialect);
        this.url = url;
        this.driverClass = driverClass;

        initDatabase();
    }

    @Override
    public HikariConfig createConfig() {
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(MAX_CONNECTIONS);
        config.setMaxLifetime(MAX_LIFETIME);

        config.setJdbcUrl(url);
        config.setDriverClassName(driverClass);

        return config;
    }
}
