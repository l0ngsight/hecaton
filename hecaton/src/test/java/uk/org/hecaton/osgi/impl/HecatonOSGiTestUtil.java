/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.osgi.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.osgi.HecatonOSGiInstance;
import uk.org.hecaton.osgi.HecatonOSGiService;

import static org.mockito.Mockito.mock;

public final class HecatonOSGiTestUtil {

    private HecatonOSGiTestUtil() {
    }

    public static HecatonOSGiInstance createHecatonOSGiInstance(HecatonInstance instance, HecatonOSGiService service) {
        return new HecatonOSGiInstanceImpl(instance, service);
    }

    public static HecatonOSGiInstance createHecatonOSGiInstance(HecatonInstance instance) {
        return createHecatonOSGiInstance(instance, mock(HecatonOSGiService.class));
    }

    public static HecatonOSGiInstance createHecatonOSGiInstance(HecatonOSGiService service) {
        return createHecatonOSGiInstance(mock(HecatonInstance.class), service);
    }

    public static HecatonOSGiInstance createHecatonOSGiInstance() {
        return createHecatonOSGiInstance(mock(HecatonInstance.class), mock(HecatonOSGiService.class));
    }
}
