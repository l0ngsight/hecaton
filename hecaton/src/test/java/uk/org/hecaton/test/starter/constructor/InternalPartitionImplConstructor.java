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

package uk.org.hecaton.test.starter.constructor;

import uk.org.hecaton.test.starter.HecatonStarterConstructor;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;

import static uk.org.hecaton.test.starter.HecatonProxyFactory.proxyArgumentsIfNeeded;
import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;

@HecatonStarterConstructor(classNames = {"uk.org.hecaton.internal.partition.impl.InternalPartitionImpl"})
public class InternalPartitionImplConstructor extends AbstractStarterObjectConstructor {

    public InternalPartitionImplConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> partitionInterceptorClass = classloader.loadClass("uk.org.hecaton.internal.partition.PartitionReplicaInterceptor");
        Class<?> replicaClass = classloader.loadClass("uk.org.hecaton.internal.partition.PartitionReplica");
        Class<?> replicaArrayClass = Array.newInstance(replicaClass, 0).getClass();
        // obtain reference to constructor InternalPartitionImpl(int partitionId, PartitionReplica localReplica,
        //                      PartitionReplica[] replicas, int version, PartitionReplicaInterceptor interceptor)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Integer.TYPE, replicaClass, replicaArrayClass,
                Integer.TYPE, partitionInterceptorClass);

        Integer partitionId = getFieldValueReflectively(delegate, "partitionId");
        Object interceptor = getFieldValueReflectively(delegate, "interceptor");
        Object localReplica = getFieldValueReflectively(delegate, "localReplica");
        Integer version = getFieldValueReflectively(delegate, "version");
        Object replicas = getFieldValueReflectively(delegate, "replicas");

        Object[] args = new Object[] {partitionId, localReplica, replicas, version, interceptor};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, classloader);

        return constructor.newInstance(proxiedArgs);
    }
}
