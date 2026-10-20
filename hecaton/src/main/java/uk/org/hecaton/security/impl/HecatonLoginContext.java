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
package uk.org.hecaton.security.impl;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * A {@link LoginContext} that loads its login modules with the member's configuration class
 * loader, whichever thread logs in.
 */
final class HecatonLoginContext extends LoginContext {

    private final ClassLoader classLoader;

    HecatonLoginContext(Subject subject, CallbackHandler handler, Configuration configuration, ClassLoader classLoader)
            throws LoginException {
        super("hecaton", subject, handler, configuration);
        this.classLoader = classLoader;
    }

    @Override
    public void login() throws LoginException {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(classLoader);
        try {
            super.login();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
