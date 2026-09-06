package com.chaconneai.openspreader.cache;

import com.chaconneai.openspreader.ProcessingException;

/**
 * A cache operation failed.
 *
 * <p>Two kinds, wanting different treatment:
 * <ul>
 *   <li><b>Used wrongly</b> -- lpush on a string key, incr on something non-numeric.
 *       Retrying achieves nothing; fix the code.</li>
 *   <li><b>Temporarily unavailable</b> -- leadership is vacant, or the request timed out.
 *       Waiting a moment and retrying usually works.</li>
 * </ul>
 * Both arrive as this exception, distinguished by the message. They are not two classes
 * because in nine cases out of ten the caller only wants to know that it did not succeed
 * -- and where the distinction really matters, the log says it more directly.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ProcessingCacheException extends ProcessingException {

    public ProcessingCacheException(String message) {
        super(message);
    }

    public ProcessingCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
