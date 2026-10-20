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

import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.util.regex.Pattern;

/**
 * Authentication against users listed in the configuration ({@code simple-authentication}):
 * option {@code password.<user>} is the user's password, {@code roles.<user>} the user's roles
 * separated by {@code roleSeparator} (default {@code ,}).
 */
public class SimplePropertiesLoginModule extends ClusterLoginModule {

    private String name;

    @Override
    protected boolean onLogin() throws LoginException {
        Credentials credentials = getCredentials();
        if (!(credentials instanceof PasswordCredentials password) || password.getName() == null) {
            throw new FailedLoginException("A user name and password are required");
        }
        String expected = getStringOption("password." + password.getName(), null);
        if (expected == null || !DefaultLoginModule.equal(expected, password.getPassword())) {
            throw new FailedLoginException("Invalid user name or password");
        }
        name = password.getName();
        String roles = getStringOption("roles." + name, null);
        if (roles != null && !roles.isEmpty()) {
            for (String role : roles.split(Pattern.quote(getStringOption("roleSeparator", ",")))) {
                if (!role.isBlank()) {
                    addRole(role.trim());
                }
            }
        }
        return true;
    }

    @Override
    protected String getName() {
        return name;
    }
}
