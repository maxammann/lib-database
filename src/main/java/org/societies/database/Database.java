package org.societies.database;

import net.catharos.lib.core.lang.Closable;
import org.jooq.DSLContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 */
public interface Database extends Closable, DSLProvider {

    Connection getConnection() throws SQLException;

    DataSource getDataSource();

    void initDatabase();

    @Override
    boolean close();

    @Override
    DSLContext getDSLContext();
}
