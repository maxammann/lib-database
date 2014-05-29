package net.catharos.lib.database.data.queue;

import net.catharos.lib.core.lang.ArgumentException;

/**
 * Represents a DataException
 */
public class DataException extends ArgumentException {

    public DataException() {
    }

    public DataException(String message, Object... args) {
        super(message, args);
    }

    public DataException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public DataException(Throwable cause) {
        super(cause);
    }
}
