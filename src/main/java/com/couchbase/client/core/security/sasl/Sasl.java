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
import javax.security.sasl.SaslException;
import java.util.Map;

/**
 * A wrapper for {@link javax.security.sasl.Sasl} which first tries the mechanisms that are supported
 * out of the box and then tries our extended range (provided by {@link ShaSaslClient}.
 *
 * @author Trond Norbye
 * @version 1.2.5
 */
public class Sasl {

    public static SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
        String serverName, Map<String, ?> props, CallbackHandler cbh) throws SaslException {
        for (String mech : mechanisms) {
            String[] mechs = new String[]{ mech };

            SaslClient ret = javax.security.sasl.Sasl.createSaslClient(mechs, authorizationId, protocol, serverName,
                props, cbh);

            if (ret == null) {
                ShaSaslClientFactory factory = new ShaSaslClientFactory();
                ret = factory.createSaslClient(mechs, authorizationId, protocol, serverName, props, cbh);
            }

            if (ret != null) {
                return ret;
            }
        }

        return null;
    }
}
