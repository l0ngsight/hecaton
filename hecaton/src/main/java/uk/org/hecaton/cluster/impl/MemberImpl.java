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
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.util.Preconditions;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.version.MemberVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static uk.org.hecaton.internal.util.Preconditions.isNotNull;

public final class MemberImpl
        extends AbstractMember
        implements Member, HecatonInstanceAware, IdentifiedDataSerializable {

    /**
     * Denotes that member list join version of a member is not known yet.
     */
    public static final int NA_MEMBER_LIST_JOIN_VERSION = -1;

    private boolean localMember;

    private volatile int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
    private volatile HecatonInstanceImpl instance;
    private volatile ILogger logger;

    public MemberImpl() {
    }

    public MemberImpl(Address address, MemberVersion version, boolean localMember) {
        this(newHashMap(MEMBER, address), address, version, localMember, null, null,
                false, NA_MEMBER_LIST_JOIN_VERSION, null);
    }

    public MemberImpl(Address address, MemberVersion version, boolean localMember, UUID uuid) {
        this(newHashMap(MEMBER, address), address, version, localMember, uuid, null, false,
                NA_MEMBER_LIST_JOIN_VERSION, null);
    }

    protected MemberImpl(Map<EndpointQualifier, Address> addresses, MemberVersion version, boolean localMember,
                       UUID uuid, Map<String, String> attributes, boolean liteMember, int memberListJoinVersion,
                       HecatonInstanceImpl instance) {
        this(addresses, addresses.get(MEMBER), version, localMember, uuid, attributes, liteMember, memberListJoinVersion,
                instance);
    }

    public MemberImpl(MemberImpl member) {
        super(member);
        this.localMember = member.localMember;
        this.memberListJoinVersion = member.memberListJoinVersion;
        this.instance = member.instance;
    }

    private MemberImpl(Map<EndpointQualifier, Address> addresses, Address address, MemberVersion version,
                       boolean localMember, UUID uuid, Map<String, String> attributes, boolean liteMember,
                       int memberListJoinVersion, HecatonInstanceImpl instance) {
        super(addresses, address, version, uuid, attributes, liteMember);
        this.memberListJoinVersion = memberListJoinVersion;
        this.localMember = localMember;
        this.instance = instance;
    }

    @Override
    protected ILogger getLogger() {
        return logger;
    }

    @Override
    public void setHecatonInstance(HecatonInstance hecatonInstance) {
        if (hecatonInstance instanceof HecatonInstanceImpl impl) {
            instance = impl;
            localMember = instance.node.address.equals(address);
            logger = instance.node.getLogger(this.getClass().getName());
        }
    }

    @Override
    public boolean localMember() {
        return localMember;
    }

    @Override
    public String getAttribute(String key) {
        return attributes.get(key);
    }

    public void setMemberListJoinVersion(int memberListJoinVersion) {
        this.memberListJoinVersion = memberListJoinVersion;
    }

    public int getMemberListJoinVersion() {
        return memberListJoinVersion;
    }

    private void ensureLocalMember() {
        if (!localMember) {
            throw new UnsupportedOperationException("Attributes on remote members must not be changed");
        }
    }

    public void setAttribute(String key, String value) {
        ensureLocalMember();
        if (instance != null && instance.node.clusterService.isJoined()) {
            throw new UnsupportedOperationException("Attributes can not be changed after instance has started");
        }

        isNotNull(key, "key");
        isNotNull(value, "value");

        attributes.put(key, value);
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER;
    }

    public static class Builder {
        private Address address;
        private  Map<EndpointQualifier, Address> addressMap;

        private Map<String, String> attributes;
        private boolean localMember;
        private UUID uuid;
        private boolean liteMember;
        private MemberVersion version;
        private int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
        private HecatonInstanceImpl instance;

        public Builder(Address address) {
            Preconditions.isNotNull(address, "address");
            this.address = address;
        }

        public Builder(Map<EndpointQualifier, Address> addresses) {
            Preconditions.isNotNull(addresses, "addresses");
            Preconditions.isNotNull(addresses.get(MEMBER), "addresses.get(MEMBER)");
            this.addressMap = addresses;
        }

        public Builder address(Address address) {
            this.address = Preconditions.isNotNull(address, "address");
            return this;
        }

        public Builder localMember(boolean localMember) {
            this.localMember = localMember;
            return this;
        }

        public Builder version(MemberVersion memberVersion) {
            this.version = memberVersion;
            return this;
        }

        public Builder uuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder attributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder memberListJoinVersion(int memberListJoinVersion) {
            this.memberListJoinVersion = memberListJoinVersion;
            return this;
        }

        public Builder liteMember(boolean liteMember) {
            this.liteMember = liteMember;
            return this;
        }

        public Builder instance(HecatonInstanceImpl hecatonInstanceImpl) {
            this.instance = hecatonInstanceImpl;
            return this;
        }

        public MemberImpl build() {
            if (addressMap == null) {
                addressMap = newHashMap(MEMBER, address);
            }
            if (address == null) {
                address = addressMap.get(MEMBER);
            }
            return new MemberImpl(addressMap, address, version, localMember, uuid,
                    attributes, liteMember, memberListJoinVersion, instance);
        }
    }

    private static Map<EndpointQualifier, Address> newHashMap(EndpointQualifier member, Address address) {
        Map<EndpointQualifier, Address> result = new HashMap<>();
        result.put(member, address);
        return result;
    }
}
