package com.couchbase.client.core.endpoint;

/**
 * Describes the states of the decoding process.
 *
 * This is used internally by the handler implementations to signal their current decoding state. It has impact on
 * which actions need to be performed in each state.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public enum DecodingState {

    /**
     * Decoding is not currently taking place.
     */
    INITIAL,

    /**
     * Decoding has started and is currently taking place.
     */
    STARTED,

    /**
     * Decoding is finished.
     */
    FINISHED

}
