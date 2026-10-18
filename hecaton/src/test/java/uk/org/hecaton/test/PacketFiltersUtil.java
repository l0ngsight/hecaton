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

package uk.org.hecaton.test;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.server.FirewallingServer.FirewallingServerConnectionManager;
import uk.org.hecaton.internal.server.OperationPacketFilter;
import uk.org.hecaton.internal.server.PacketFilter;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.internal.util.Preconditions;
import uk.org.hecaton.internal.util.collection.IntHashSet;
import uk.org.hecaton.logging.ILogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static java.util.Collections.singleton;

@SuppressWarnings("unused")
public final class PacketFiltersUtil {

    private PacketFiltersUtil() {
    }

    public static ServerConnectionManager getConnectionManager(HecatonInstance instance) {
        Node node = getNode(instance);
        return node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
    }

    public static void resetPacketFiltersFrom(HecatonInstance instance) {
        ServerConnectionManager cm = getConnectionManager(instance);
        if (cm instanceof FirewallingServerConnectionManager manager) {
            manager.removePacketFilter();
        } else {
            ILogger logger = instance.getLoggingService().getLogger(PacketFiltersUtil.class);
            logger.warning("Trying to reset packet filters when using real network");
        }
    }

    public static void delayOperationsFrom(HecatonInstance instance, int factory, List<Integer> opTypes) {
       filterOperationsFrom(instance, factory, opTypes, PacketFilter.Action.DELAY);
    }

    public static void delayOperationsBetween(HecatonInstance from, HecatonInstance to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, getAddresses(singleton(to)), factory, opTypes, PacketFilter.Action.DELAY);
    }

    public static void delayOperationsBetween(HecatonInstance from, Collection<HecatonInstance> to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, getAddresses(to), factory, opTypes, PacketFilter.Action.DELAY);
    }

    public static void dropOperationsFrom(HecatonInstance instance, int factory, List<Integer> opTypes) {
        filterOperationsFrom(instance, factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void dropOperationsBetween(HecatonInstance from, HecatonInstance to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, getAddresses(singleton(to)), factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void dropOperationsBetween(HecatonInstance from, Collection<HecatonInstance> to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, getAddresses(to), factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void dropOperationsToAddresses(HecatonInstance instance, Collection<Address> addresses, int factory, List<Integer> opTypes) {
        filterOperationsBetween(instance, addresses, factory, opTypes, PacketFilter.Action.DROP);
    }

    public static void rejectOperationsFrom(HecatonInstance instance, int factory, List<Integer> opTypes) {
        filterOperationsFrom(instance, factory, opTypes, PacketFilter.Action.REJECT);
    }

    public static void rejectOperationsBetween(HecatonInstance from, HecatonInstance to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, getAddresses(singleton(to)), factory, opTypes, PacketFilter.Action.REJECT);
    }

    public static void rejectOperationsBetween(HecatonInstance from, Collection<HecatonInstance> to, int factory, List<Integer> opTypes) {
        filterOperationsBetween(from, getAddresses(to), factory, opTypes, PacketFilter.Action.REJECT);
    }

    private static void filterOperationsFrom(HecatonInstance instance, int factory, List<Integer> opTypes,
            PacketFilter.Action action) {
        PacketFilter packetFilter = new EndpointAgnosticPacketFilter(getNode(instance).getSerializationService(), factory, opTypes, action);
        setCustomFilter(instance, packetFilter);
    }

    private static void filterOperationsBetween(HecatonInstance from, Collection<Address> addresses, int factory, List<Integer> opTypes,
                                                PacketFilter.Action action) {
        PacketFilter packetFilter = new EndpointAwarePacketFilter(getNode(from).getSerializationService(), addresses, factory,
                opTypes, action);
        setCustomFilter(from, packetFilter);
    }

    private static Collection<Address> getAddresses(Collection<HecatonInstance> instances) {
        List<Address> addresses = new ArrayList<>();
        for (HecatonInstance instance : instances) {
            addresses.add(getAddress(instance));
        }

        return addresses;
    }

    public static void setCustomFilter(HecatonInstance from, PacketFilter packetFilter) {
        Node node = getNode(from);
        FirewallingServerConnectionManager cm = (FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        cm.setPacketFilter(packetFilter);
    }

    public static void wrapCustomerFilter(HecatonInstance from, UnaryOperator<PacketFilter> packetFilterWrapper) {
        Node node = getNode(from);
        FirewallingServerConnectionManager cm = (FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        PacketFilter original = cm.getPacketFilter();
        assert original != null;
        var wrapped = packetFilterWrapper.apply(original);
        cm.setPacketFilter(wrapped);
    }

    private static class EndpointAgnosticPacketFilter extends OperationPacketFilter {

        final int factory;
        final Action action;

        // Integer.MIN_VALUE is used for missing value
        final IntHashSet types = new IntHashSet(1024, Integer.MIN_VALUE);

        EndpointAgnosticPacketFilter(InternalSerializationService serializationService, int factory,
                List<Integer> typeIds, Action action) {
            super(serializationService);
            this.action = Preconditions.checkNotNull(action);
            assert !typeIds.isEmpty() : "At least one operation type must be defined!";
            this.factory = factory;
            types.addAll(typeIds);
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            return (this.factory == factory && types.contains(type)) ? action : Action.ALLOW;
        }
    }

    private static class EndpointAwarePacketFilter extends EndpointAgnosticPacketFilter {

        final Set<Address> blacklist = new HashSet<>();

        EndpointAwarePacketFilter(InternalSerializationService serializationService, Collection<Address> blacklist, int factory,
                                  List<Integer> typeIds, Action action) {
            super(serializationService, factory, typeIds, action);
            this.blacklist.addAll(blacklist);
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            if (blacklist.contains(endpoint)) {
                return super.filterOperation(endpoint, factory, type);
            }
            return Action.ALLOW;
        }
    }
}
