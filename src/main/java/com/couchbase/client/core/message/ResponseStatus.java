/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.core.message;

/**
 * Typesafe status code returned by {@link CouchbaseResponse}s.
 *
 * Note that the general rules apply and should be followed if further extended:
 *
 * - 100: Success.
 * - 101 - 199: General Non-Error codes that indicate some kind of special response.
 * - 200 - 299: Generic Errors tha can happen across services
 * - 300 - 399: Key/Value Specific Errors
 * - 400 - 499: View Specific Errors
 * - 500 - 599: Config Specific Errors
 * - 600 - 699: Query Specific Errors
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public enum ResponseStatus {


    /**
     * If the response is successful and finished.
     */
    SUCCESS(100, "Sucessful"),

    /**
     * If the request expected the document to not exist, but it existed already.
     */
    EXISTS(101, "The Resource already exists"),

    /**
     * If the request expected the document to exit, but it didn't exist already.
     */
    NOT_EXISTS(102, "The Resource does not exist"),

    /**
     * The underlying response indicates retry is in order.
     *
     * This is a internal response and should not bubble up to the user level.
     */
    RETRY(103, "The Resource needs to be retried"),

    /**
     * Generic failure status.
     */
    FAILURE(200, "Generic Failure"),

    /**
     * The server responded with a temporary failure.
     */
    FAILURE_TEMPORARY(201, "Temporary Failure"),

    /**
     * The requested resource is too big.
     */
    FAILURE_TOO_BIG(202, "Request Resource too big"),

    /**
     * An authentication error happened.
     */
    FAILURE_AUTH_ERROR(202, "Authentication Error"),

    /**
     * The request was invalid.
     */
    FAILURE_INVALID(203, "Invalid Request"),

    /**
     * The server reported that it is busy.
     */
    FAILURE_BUSY(204, "The server is busy"),

    /**
     * The request is not supported.
     */
    FAILURE_NOT_SUPPORTED(205, "The server does not support this request"),

    /**
     * Key/Value: the entity as part of the request was not stored.
     */
    FAILURE_KV_NOT_STORED(301, "Entity not stored");

    private final int code;
    private final String description;

    ResponseStatus(int code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * Check if the current {@link ResponseStatus} is success.
     *
     * @return true if it is a failure, false otherwise.
     */
    public boolean isSuccess() {
        return this == ResponseStatus.SUCCESS;
    }

    /**
     * If the current {@link ResponseStatus} is a failure.
     *
     * @return true if the code is a failure, false otherwise.
     */
    public boolean isFailure() {
        return code >= 200;
    }

    /**
     * The numeric code for this {@link ResponseStatus}.
     *
     * @return the numeric status code.
     */
    public int code() {
        return code;
    }

    /**
     * The textual description of this error.
     *
     * @return the error code.
     */
    public String description() {
        return description;
    }
}
