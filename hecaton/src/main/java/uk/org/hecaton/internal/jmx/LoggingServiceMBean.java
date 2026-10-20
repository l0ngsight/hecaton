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

package uk.org.hecaton.internal.jmx;

import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.logging.impl.LoggingServiceImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static uk.org.hecaton.internal.jmx.ManagementService.quote;

public class LoggingServiceMBean extends HecatonMBean<LoggingServiceImpl> {

    protected LoggingServiceMBean(HecatonInstanceImpl hecatonInstance, ManagementService service) {
        super((LoggingServiceImpl) hecatonInstance.getLoggingService(), service);

        Map<String, String> properties = new HashMap<>();
        properties.put("type", quote("HecatonInstance.LoggingService"));
        properties.put("name", quote(hecatonInstance.getName()));
        properties.put("instance", quote(hecatonInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("level")
    @ManagedDescription("Current level set")
    public String getLevel() {
        Level level = managedObject.getLevel();
        return level == null ? null : level.getName();
    }

    @ManagedAnnotation(value = "setLevel", operation = true)
    @ManagedDescription("Set level")
    public void setLevel(String level) {
        managedObject.setLevel(level);
    }

    @ManagedAnnotation(value = "resetLevel", operation = true)
    @ManagedDescription("Reset level")
    public void resetLevel() {
        managedObject.resetLevel();
    }

}
