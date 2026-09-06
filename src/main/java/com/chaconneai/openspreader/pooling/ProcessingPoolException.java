package com.chaconneai.openspreader.pooling;

import com.chaconneai.openspreader.ProcessingException;

/**
 * Something went wrong dispatching a task: serialisation failed, the method is not
 * exposed, remote execution failed, or it timed out.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ProcessingPoolException extends ProcessingException {

    public ProcessingPoolException(String message) {
        super(message);
    }

    public ProcessingPoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
