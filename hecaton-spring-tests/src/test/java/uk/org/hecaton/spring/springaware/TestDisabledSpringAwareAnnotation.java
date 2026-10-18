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

package uk.org.hecaton.spring.springaware;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spring.CustomSpringExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertNull;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"springAware-disabled-applicationContext-hecaton.xml"})
class TestDisabledSpringAwareAnnotation {

    @BeforeAll
    @AfterAll
    static void cleanup() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Autowired
    private ApplicationContext context;

    @Test
    void testDisabledSpringManagedContext() {
        HecatonInstance instance = (HecatonInstance) context.getBean("instance");
        assertNull(instance.getConfig().getManagedContext());

        HecatonClientProxy client = (HecatonClientProxy) context.getBean("client");
        assertNull(client.getClientConfig().getManagedContext());
    }
}
