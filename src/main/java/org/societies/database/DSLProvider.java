package org.societies.database;

import org.jooq.DSLContext;

/**
 * Represents a DSLProvider
 */
public interface DSLProvider {

    DSLContext getDSLContext();
}
