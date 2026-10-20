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

package uk.org.hecaton.sql.impl.schema.map;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.sql.impl.CoreSqlTestSupport;
import uk.org.hecaton.sql.impl.extract.GenericQueryTargetDescriptor;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.extract.QueryTarget;
import uk.org.hecaton.sql.impl.extract.QueryTargetDescriptor;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.schema.map.PartitionedMapTable.PartitionedMapPlanObjectKey;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionedMapPlanObjectKeyTest extends CoreSqlTestSupport {
    @Test
    public void test_partitioned() {
        String schema1 = "schema1";
        String schema2 = "schema2";

        String tableName1 = "table1";
        String tableName2 = "table2";

        String mapName1 = "map1";
        String mapName2 = "map2";

        List<TableField> fields1 = singletonList(new MapTableField("field1", QueryDataType.INT, true, QueryPath.KEY_PATH));
        List<TableField> fields2 = singletonList(new MapTableField("field2", QueryDataType.INT, true, QueryPath.KEY_PATH));

        Set<String> conflictingSchemas1 = Collections.singleton("schema1");
        Set<String> conflictingSchemas2 = Collections.singleton("schema2");

        QueryTargetDescriptor keyDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor keyDescriptor2 = new TestTargetDescriptor();

        QueryTargetDescriptor valueDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor valueDescriptor2 = new TestTargetDescriptor();

        Object keyJetMetadata1 = new Object();
        Object valueJetMetadata1 = new Object();

        Object keyJetMetadata2 = new Object();
        Object valueJetMetadata2 = new Object();

        List<MapTableIndex> indexes1 = singletonList(new MapTableIndex("idx", IndexType.SORTED, 0, emptyList(), emptyList()));
        List<MapTableIndex> indexes2 = singletonList(new MapTableIndex("idx", IndexType.HASH, 0, emptyList(), emptyList()));

        boolean hd1 = false;
        boolean hd2 = true;

        List<String> partitioningAttributes1 = singletonList("attr1");
        List<String> partitioningAttributes2 = emptyList();

        boolean supportsPartitionPruning1 = true;
        boolean supportsPartitionPruning2 = false;

        PartitionedMapPlanObjectKey objectId = new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes1, supportsPartitionPruning1);

        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes1, supportsPartitionPruning1), true);

        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema2, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema2, tableName2, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName2, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields2, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas2, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor2, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor2, keyJetMetadata1, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor2, keyJetMetadata2, valueJetMetadata1, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor2, keyJetMetadata1, valueJetMetadata2, indexes1, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes2, hd1, partitioningAttributes2, supportsPartitionPruning2), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, tableName1, mapName1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, keyJetMetadata1, valueJetMetadata1, indexes1, hd2, partitioningAttributes2, supportsPartitionPruning2), false);
    }

    private static class TestTargetDescriptor implements QueryTargetDescriptor {
        @Override
        public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) {
            // No-op.
        }
    }
}
