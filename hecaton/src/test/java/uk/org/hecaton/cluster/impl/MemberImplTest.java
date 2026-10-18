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

package uk.org.hecaton.cluster.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.version.MemberVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static uk.org.hecaton.internal.util.UuidUtil.newUnsecureUUID;
import static uk.org.hecaton.test.Accessors.getHecatonInstanceImpl;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberImplTest extends HecatonTestSupport {

    private static HecatonInstanceImpl hecatonInstance;
    private static Address address;

    @BeforeClass
    public static void setUp() throws Exception {
        HecatonInstance instance = new TestHecatonInstanceFactory().newHecatonInstance();
        hecatonInstance = getHecatonInstanceImpl(instance);
        address = new Address("127.0.0.1", 5701);
    }

    @AfterClass
    public static void tearDown() {
        hecatonInstance.shutdown();
    }

    @Test
    public void testConstructor_withLocalMember_isTrue() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
    }

    @Test
    public void testConstructor_withLocalMember_isFalse() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), false);

        assertBasicMemberImplFields(member);
        assertFalse(member.localMember());
    }

    @Test
    public void testConstructor_withLiteMember_isTrue() {
        MemberImpl member = new MemberImpl.Builder(address)
                .version(MemberVersion.of("3.8.0"))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .liteMember(true)
                .build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertTrue(member.isLiteMember());
    }

    @Test
    public void testConstructor_withLiteMember_isFalse() {
        MemberImpl member = new MemberImpl.Builder(address)
                .version(MemberVersion.of("3.8.0"))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withHecatonInstance() {
        UUID uuid = UuidUtil.newUnsecureUUID();
        MemberImpl member = new MemberImpl.Builder(address).version(MemberVersion.of("3.8.0"))
                .localMember(true).uuid(uuid).instance(hecatonInstance).build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertEquals(uuid, member.getUuid());
    }

    @Test
    public void testConstructor_withAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key1", "value");
        attributes.put("key2", "12345");

        UUID uuid = UuidUtil.newUnsecureUUID();
        MemberImpl member = new MemberImpl.Builder(address).version(MemberVersion.of("3.8.0")).localMember(true)
                .uuid(uuid).attributes(attributes).instance(hecatonInstance).build();

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
        assertEquals(uuid, member.getUuid());
        assertEquals("value", member.getAttribute("key1"));
        assertEquals("12345", member.getAttribute("key2"));
        assertFalse(member.isLiteMember());
    }

    @Test
    public void testConstructor_withMemberImpl() {
        MemberImpl member = new MemberImpl(new MemberImpl(address, MemberVersion.of("3.8.0"), true));

        assertBasicMemberImplFields(member);
        assertTrue(member.localMember());
    }

    @Test
    public void testSetHecatonInstance() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getLogger());

        member.setHecatonInstance(hecatonInstance);

        assertNotNull(member.getLogger());
    }

    @Test
    public void testStringAttribute() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), true);
        assertNull(member.getAttribute("stringKey"));

        member.setAttribute("stringKey", "stringValue");
        assertEquals("stringValue", member.getAttribute("stringKey"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAttribute_withHecatonInstance() {
        MemberImpl member = new MemberImpl.Builder(address)
                .version(MemberVersion.of("3.8.0"))
                .localMember(true)
                .uuid(newUnsecureUUID())
                .instance(hecatonInstance).build();

        member.setAttribute("setKeyWithInstance", "setValueWithInstance");
        assertEquals("setValueWithInstance", member.getAttribute("setKeyWithInstance"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAttribute_onRemoteMember() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.8.0"), false);
        member.setAttribute("remoteMemberSet", "wontWork");
    }

    @Test
    public void testSerialization_whenSingleAddress() {
        MemberImpl member = new MemberImpl(address, MemberVersion.of("3.12.0"), false);
        testSerialization(member);
    }

    @Test
    public void testSerialization_whenMultiAddress() throws Exception {
        Map<EndpointQualifier, Address> addressMap = new HashMap<>();
        addressMap.put(EndpointQualifier.MEMBER, address);
        addressMap.put(EndpointQualifier.REST, new Address("127.0.0.1", 8080));
        MemberImpl member = new MemberImpl.Builder(addressMap).version(MemberVersion.of("3.12.0")).build();
        testSerialization(member);
    }

    private void testSerialization(Member member) {
        SerializationService serializationService = getSerializationService(hecatonInstance);
        Data serialized = serializationService.toData(member);
        Member deserialized = serializationService.toObject(serialized);
        assertEquals(member, deserialized);
    }

    private static void assertBasicMemberImplFields(MemberImpl member) {
        assertEquals(address, member.getAddress());
        assertEquals(5701, member.getPort());
        assertEquals("127.0.0.1", member.getInetAddress().getHostAddress());
        assertTrue(member.getFactoryId() > -1);
        assertTrue(member.getClassId() > -1);
        assertNotNull(member.getVersion());
        assertEquals(3, member.getVersion().getMajor());
        assertEquals(8, member.getVersion().getMinor());
        assertEquals(0, member.getVersion().getPatch());
    }
}
