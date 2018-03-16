/**
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.core.security.sasl;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * The SaslClientFactory supporting SCRAM-SHA512, SCRAM-SHA256 and SCRAM-SHA1
 * authentication methods
 *
 * @author Trond Norbye
 * @version 1.0
 */
public class ShaSaslClientFactory implements SaslClientFactory {
    private static final String[] supportedMechanisms =
            new String[]{"SCRAM-SHA512", "SCRAM-SHA256", "SCRAM-SHA1"};

    @Override
    public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol, String serverName,
        Map<String, ?> props, CallbackHandler cbh) throws SaslException {

        int sha = 0;

        for (String m : mechanisms) {
            if (m.equals("SCRAM-SHA512")) {
                sha = 512;
            } else if (m.equals("SCRAM-SHA256")) {
                sha = 256;
            } else if (m.equals("SCRAM-SHA1")) {
                sha = 1;
            }
        }

        if (sha == 0) {
            return null;
        }

        if (authorizationId != null) {
            throw new SaslException("authorizationId is not supported (yet)");
        }

        if (cbh == null) {
            throw new SaslException("Callback handler must be set");
        }

        // protocol, servername and props is currently being ignored...

        try {
            return new ShaSaslClient(cbh, sha);
        } catch (NoSuchAlgorithmException e) {
            // The JAVA runtime don't support all the algorithms we need
            return null;
        }
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
        // @todo look at the properties we should nuke off
        return supportedMechanisms;
    }
}