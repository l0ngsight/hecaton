/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.impl.processor.TransformP;
import uk.org.hecaton.jet.sql.impl.ExpressionUtil;
import uk.org.hecaton.jet.sql.impl.JetJoinInfo;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static uk.org.hecaton.jet.Traversers.empty;
import static uk.org.hecaton.jet.Traversers.singleton;
import static uk.org.hecaton.jet.Traversers.traverseIterable;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_CREATE;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;

//@SuppressFBWarnings(
//        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
//        justification = "the class is never java-serialized"
//)
final class JoinByEquiJoinProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private JetJoinInfo joinInfo;
    private String mapName;
    private int partitionCount;
    private int[] partitions;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient MapProxyImpl<Object, Object> map;
    private transient ExpressionEvalContext evalContext;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private JoinByEquiJoinProcessorSupplier() {
    }

    JoinByEquiJoinProcessorSupplier(
            @Nonnull JetJoinInfo joinInfo,
            @Nonnull String mapName,
            int partitionCount,
            @Nullable int[] partitions,
            @Nonnull KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        assert joinInfo.isEquiJoin() && (joinInfo.isInner() || joinInfo.isLeftOuter());

        this.joinInfo = joinInfo;
        this.mapName = mapName;
        this.partitionCount = partitionCount;
        this.partitions = partitions;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = (MapProxyImpl<Object, Object>) context.hecatonInstance().getMap(mapName);
        evalContext = ExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            PartitionIdSet partitions = this.partitions == null
                    ? null
                    : new PartitionIdSet(partitionCount, this.partitions);
            QueryPath[] rightPaths = rightRowProjectorSupplier.paths();
            KvRowProjector rightProjector = rightRowProjectorSupplier.get(evalContext, extractors);
            Processor processor = new TransformP<>(
                    joinFn(joinInfo, map, partitions, rightPaths, rightProjector, evalContext)
            ) {
                @Override
                public boolean isCooperative() {
                    return false;
                }
            };
            processors.add(processor);
        }
        return processors;
    }

    private static FunctionEx<JetSqlRow, Traverser<JetSqlRow>> joinFn(
            JetJoinInfo joinInfo,
            MapProxyImpl<Object, Object> map,
            PartitionIdSet partitions,
            QueryPath[] rightPaths,
            KvRowProjector rightRowProjector,
            ExpressionEvalContext evalContext
    ) {
        return left -> {
            Predicate<Object, Object> predicate = QueryUtil.toPredicate(
                    left,
                    joinInfo.leftEquiJoinIndices(),
                    joinInfo.rightEquiJoinIndices(),
                    rightPaths
            );
            if (predicate == null) {
                return joinInfo.isInner()
                        ? empty()
                        : singleton(left.extendedRow(rightRowProjector.getColumnCount()));
            }

            Set<Entry<Object, Object>> matchingRows = joinInfo.isInner()
                    ? map.entrySet(predicate, partitions.copy())
                    : map.entrySet(predicate);
            List<JetSqlRow> joined = join(left, matchingRows, rightRowProjector, joinInfo.nonEquiCondition(), evalContext);
            return joined.isEmpty() && joinInfo.isLeftOuter()
                    ? singleton(left.extendedRow(rightRowProjector.getColumnCount()))
                    : traverseIterable(joined);
        };
    }

    private static List<JetSqlRow> join(
            JetSqlRow left,
            Set<Entry<Object, Object>> entries,
            KvRowProjector rightRowProjector,
            Expression<Boolean> condition,
            ExpressionEvalContext evalContext
    ) {
        List<JetSqlRow> rows = new ArrayList<>();
        for (Entry<Object, Object> entry : entries) {
            JetSqlRow right = rightRowProjector.project(entry.getKey(), entry.getValue());
            if (right == null) {
                continue;
            }

            JetSqlRow joined = ExpressionUtil.join(left, right, condition, evalContext);
            if (joined != null) {
                rows.add(joined);
            }
        }
        return rows;
    }

    @Override
    public List<Permission> permissions() {
        return singletonList(new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(joinInfo);
        out.writeObject(mapName);
        out.writeInt(partitionCount);
        out.writeObject(partitions);
        out.writeObject(rightRowProjectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        joinInfo = in.readObject();
        mapName = in.readObject();
        partitionCount = in.readInt();
        partitions = in.readObject();
        rightRowProjectorSupplier = in.readObject();
    }

    static ProcessorMetaSupplier supplier(
            JetJoinInfo joinInfo,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        return new Supplier(joinInfo, mapName, rightRowProjectorSupplier);
    }

//    @SuppressFBWarnings(
//            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
//            justification = "the class is never java-serialized"
//    )
    private static final class Supplier implements ProcessorMetaSupplier, DataSerializable {

        private JetJoinInfo joinInfo;
        private String mapName;
        private KvRowProjector.Supplier rightRowProjectorSupplier;

        private transient Map<Address, int[]> partitionAssignment;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                JetJoinInfo joinInfo,
                String mapName,
                KvRowProjector.Supplier rightRowProjectorSupplier
        ) {
            assert joinInfo.isEquiJoin() && (joinInfo.isInner() || joinInfo.isLeftOuter());

            this.joinInfo = joinInfo;
            this.mapName = mapName;
            this.rightRowProjectorSupplier = rightRowProjectorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            partitionAssignment = context.partitionAssignment();
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (joinInfo.isInner()) {
                int partitionCount = partitionAssignment.values().stream().mapToInt(a -> a.length).sum();

                return address -> new JoinByEquiJoinProcessorSupplier(
                        joinInfo,
                        mapName,
                        partitionCount,
                        partitionAssignment.get(address),
                        rightRowProjectorSupplier
                );
            } else {
                return address -> new JoinByEquiJoinProcessorSupplier(
                        joinInfo,
                        mapName,
                        0,
                        null,
                        rightRowProjectorSupplier
                );
            }
        }

        @Override
        public boolean initIsCooperative() {
            return true;
        }

        @Override
        public boolean closeIsCooperative() {
            return true;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(joinInfo);
            out.writeObject(mapName);
            out.writeObject(rightRowProjectorSupplier);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            joinInfo = in.readObject();
            mapName = in.readObject();
            rightRowProjectorSupplier = in.readObject();
        }
    }
}
