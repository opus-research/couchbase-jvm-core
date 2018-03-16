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
package com.couchbase.client.core.util;

/**
 * Helper class to centralize test properties that can be modified through system properties.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class TestProperties {

    private static String seedNode;
    private static String bucket;
    private static String password;

    /**
     * Initialize static the properties.
     */
    static {
        seedNode = System.getProperty("seedNode", "192.168.56.101");
        bucket = System.getProperty("bucket", "beer-sample");
        password = System.getProperty("password", "");
    }

    /**
     * The seed node to bootstrap from.
     *
     * @return the seed node.
     */
    public static String seedNode() {
        return seedNode;
    }

    /**
     * The bucket to work against.
     *
     * @return the name of the bucket.
     */
    public static String bucket() {
        return bucket;
    }

    /**
     * The password of the bucket.
     *
     * @return the password of the bucket.
     */
    public static String password() {
        return password;
    }
}
