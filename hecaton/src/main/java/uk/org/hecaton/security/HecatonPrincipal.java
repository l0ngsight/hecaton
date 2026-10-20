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
package uk.org.hecaton.security;

import java.io.Serializable;
import java.security.Principal;
import java.util.Objects;

/**
 * Base of the principals that Hecaton login modules add to an authenticated subject.
 */
public abstract class HecatonPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;

    protected HecatonPrincipal(String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass() == getClass() && name.equals(((HecatonPrincipal) o).name);
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode() + name.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + name + "]";
    }
}
