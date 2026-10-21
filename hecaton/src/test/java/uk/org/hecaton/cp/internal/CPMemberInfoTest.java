/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMemberInfoTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Test
    public void testDeserialization() throws UnknownHostException {
        InetAddress localHost = InetAddress.getLocalHost();
        Address address = new Address(localHost.getHostName(), 0);
        CPMemberInfo cpMemberInfo = new CPMemberInfo(UUID.randomUUID(), address, false);

        // need a list of CPMemberInfo to trigger the default java serializer
        Data data = SERIALIZATION_SERVICE.toData(Arrays.asList(cpMemberInfo));
        List<CPMemberInfo> cpMemberInfoList = SERIALIZATION_SERVICE.toObject(data);
        CPMemberInfo restoredCPMemberInfo = cpMemberInfoList.get(0);
        Address memberInfoAddress = restoredCPMemberInfo.getAddress();

        assertEquals(cpMemberInfo, restoredCPMemberInfo);
        assertTrue(memberInfoAddress.isIPv4() ||  memberInfoAddress.isIPv6());
    }

    @Test
    public void testDeserializationWithUnresolvableHost() {
        Address unresolvedAddress = Address.createUnresolvedAddress("unresolved-host", 0);
        CPMemberInfo cpMemberInfo = new CPMemberInfo(UUID.randomUUID(), unresolvedAddress, false);

        // need a list of CPMemberInfo to trigger the default java serializer
        Data data = SERIALIZATION_SERVICE.toData(Arrays.asList(cpMemberInfo));
        List<CPMemberInfo> cpMemberInfoList = SERIALIZATION_SERVICE.toObject(data);
        CPMemberInfo restoredCPMemberInfo = cpMemberInfoList.get(0);
        Address memberInfoAddress = restoredCPMemberInfo.getAddress();

        assertEquals(cpMemberInfo, restoredCPMemberInfo);
        assertFalse(memberInfoAddress.isIPv4() ||  memberInfoAddress.isIPv6());
    }
}
