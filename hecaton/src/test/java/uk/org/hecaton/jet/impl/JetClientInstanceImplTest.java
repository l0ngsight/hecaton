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

package uk.org.hecaton.jet.impl;

import uk.org.hecaton.client.impl.client.DistributedObjectInfo;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetClientInstanceImplTest extends JetTestSupport {

    @Test
    public void given_singleMapOnMember_when_getDistributedObjectsCalled_then_ReturnedObjectInfo() {
        // Given
        HecatonInstance member = createHecatonInstance();
        JetClientInstanceImpl client = (JetClientInstanceImpl) createHecatonClient().getJet();
        String mapName = randomMapName();
        member.getMap(mapName);

        // When
        List<DistributedObjectInfo> objects = client.getDistributedObjects();

        // Then
        assertFalse(objects.isEmpty());
        DistributedObjectInfo info = objects.stream()
                .filter(i -> mapName.equals(i.getName()))
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(MapService.SERVICE_NAME, info.getServiceName());
    }
}
