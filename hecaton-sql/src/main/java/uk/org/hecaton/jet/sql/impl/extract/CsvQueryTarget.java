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

package uk.org.hecaton.jet.sql.impl.extract;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.sql.impl.extract.QueryExtractor;
import uk.org.hecaton.sql.impl.extract.QueryTarget;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.List;

@NotThreadSafe
public class CsvQueryTarget implements QueryTarget {

    private final List<String> fieldList;
    private String[] entry;

    public CsvQueryTarget(List<String> fieldList) {
        this.fieldList = fieldList;
        assert new HashSet<>(fieldList).size() == fieldList.size() : "duplicates in the fieldList";
    }

    @Override
    public void setTarget(Object target, Data targetData) {
        assert targetData == null;
        entry = (String[]) target;
        assert entry.length == fieldList.size();
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return path == null ? createExtractor() : createFieldExtractor(path, type);
    }

    private QueryExtractor createExtractor() {
        return () -> entry;
    }

    private QueryExtractor createFieldExtractor(String path, QueryDataType type) {
        int fieldIndex = fieldList.indexOf(path);
        return () -> type.convert(entry[fieldIndex]);
    }
}
