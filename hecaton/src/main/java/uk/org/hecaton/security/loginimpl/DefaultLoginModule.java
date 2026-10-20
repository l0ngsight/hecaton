/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.org.hecaton.security.loginimpl;

import uk.org.hecaton.security.ClusterLoginModule;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.PasswordCredentials;
import uk.org.hecaton.security.TokenCredentials;
import uk.org.hecaton.security.impl.MemberIdentityCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Authentication of a realm without an authentication method: the connecting side must present
 * the same credentials as this member's own identity (its member realm's identity). If the
 * member has no identity, the connecting side must present none, and only the cluster name is
 * checked (before this module runs).
 */
public class DefaultLoginModule extends ClusterLoginModule {

    private String name;

    @Override
    protected boolean onLogin() throws LoginException {
        Credentials presented = getCredentials();
        Credentials expected = memberIdentity();
        if (expected == null) {
            if (presented != null && !isEmpty(presented)) {
                throw new FailedLoginException("Credentials were presented, but the member has no identity to check them "
                        + "against");
            }
            name = getClusterName();
            return true;
        }
        if (!matches(expected, presented)) {
            throw new FailedLoginException("Invalid credentials: expected " + describe(expected) + ", got "
                    + describe(presented));
        }
        name = presented.getName() == null ? getClusterName() : presented.getName();
        return true;
    }

    @Override
    protected String getName() {
        return name;
    }

    private Credentials memberIdentity() throws LoginException {
        MemberIdentityCallback callback = new MemberIdentityCallback();
        try {
            callbackHandler.handle(new Callback[]{callback});
        } catch (IOException | UnsupportedCallbackException e) {
            LoginException exception = new LoginException("The member identity is not available");
            exception.initCause(e);
            throw exception;
        }
        return callback.getCredentials();
    }

    /** Type and name, never the secret. */
    private static String describe(Credentials credentials) {
        return credentials == null ? "none" : credentials.getClass().getSimpleName() + "[" + credentials.getName() + "]";
    }

    private static boolean isEmpty(Credentials credentials) {
        return credentials instanceof PasswordCredentials password
                && password.getName() == null && password.getPassword() == null;
    }

    static boolean matches(Credentials expected, Credentials presented) {
        if (expected instanceof PasswordCredentials e && presented instanceof PasswordCredentials p) {
            return equal(e.getName(), p.getName()) && equal(e.getPassword(), p.getPassword());
        }
        if (expected instanceof TokenCredentials e && presented instanceof TokenCredentials p) {
            return e.getToken() != null && p.getToken() != null && MessageDigest.isEqual(e.getToken(), p.getToken());
        }
        return false;
    }

    /** Compares in constant time for equal lengths. */
    static boolean equal(String a, String b) {
        if (a == null || b == null) {
            return a == b;
        }
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
