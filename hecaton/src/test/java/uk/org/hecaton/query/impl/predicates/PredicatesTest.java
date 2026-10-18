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

package uk.org.hecaton.query.impl.predicates;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.PredicateBuilder.EntryObject;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.QueryException;
import uk.org.hecaton.query.SampleTestObjects.Employee;
import uk.org.hecaton.query.SampleTestObjects.Value;
import uk.org.hecaton.query.impl.QueryContext;
import uk.org.hecaton.query.impl.QueryEntry;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static uk.org.hecaton.instance.impl.TestUtil.toData;
import static uk.org.hecaton.query.Predicates.and;
import static uk.org.hecaton.query.Predicates.between;
import static uk.org.hecaton.query.Predicates.equal;
import static uk.org.hecaton.query.Predicates.greaterEqual;
import static uk.org.hecaton.query.Predicates.greaterThan;
import static uk.org.hecaton.query.Predicates.ilike;
import static uk.org.hecaton.query.Predicates.in;
import static uk.org.hecaton.query.Predicates.instanceOf;
import static uk.org.hecaton.query.Predicates.lessEqual;
import static uk.org.hecaton.query.Predicates.lessThan;
import static uk.org.hecaton.query.Predicates.like;
import static uk.org.hecaton.query.Predicates.notEqual;
import static uk.org.hecaton.query.Predicates.or;
import static uk.org.hecaton.query.Predicates.regex;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class PredicatesTest extends HecatonTestSupport {

    private static final String ATTRIBUTE = "DUMMY_ATTRIBUTE_IGNORED";

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test
    @Ignore("now will execute partition number of times")
    public void testAndPredicate_whenFirstIndexAwarePredicateIsNotIndexed() {
        final HecatonInstance instance = createHecatonInstance();
        final IMap<Object, Object> map = instance.getMap("map");
        map.addIndex(IndexType.HASH, "name");
        String name = randomString();
        map.put("key", new Value(name));

        final ShouldExecuteOncePredicate<?, ?> indexAwareNotIndexedPredicate = new ShouldExecuteOncePredicate<>();
        final EqualPredicate equalPredicate = new EqualPredicate("name", name);
        final AndPredicate andPredicate = new AndPredicate(indexAwareNotIndexedPredicate, equalPredicate);
        map.values(andPredicate);
    }

    static class ShouldExecuteOncePredicate<K, V> implements IndexAwarePredicate<K, V> {

        boolean executed;

        @Override
        public boolean apply(Map.Entry<K, V> mapEntry) {
            if (!executed) {
                executed = true;
                return true;
            }
            throw new RuntimeException();
        }

        @Override
        public Set<QueryableEntry<K, V>> filter(final QueryContext queryContext) {
            return null;
        }

        @Override
        public boolean isIndexed(final QueryContext queryContext) {
            return false;
        }
    }

    @Test
    public void testEqual() {
        assertPredicateTrue(equal(ATTRIBUTE, "value"), "value");
        assertPredicateFalse(equal(ATTRIBUTE, "value1"), "value");
        assertPredicateTrue(equal(ATTRIBUTE, TRUE), true);
        assertPredicateTrue(equal(ATTRIBUTE, true), TRUE);
        assertPredicateFalse(equal(ATTRIBUTE, true), FALSE);
        assertPredicateFalse(equal(ATTRIBUTE, new BigDecimal("1.23E3")), new BigDecimal("1.23E2"));
        assertPredicateTrue(equal(ATTRIBUTE, new BigDecimal("1.23E3")), new BigDecimal("1.23E3"));
        assertPredicateFalse(equal(ATTRIBUTE, 15.22), 15.23);
        assertPredicateTrue(equal(ATTRIBUTE, 15.22), 15.22);
        assertPredicateFalse(equal(ATTRIBUTE, 16), 15);
    }

    @Test
    public void testAnd() {
        final Predicate and1 = and(greaterThan(ATTRIBUTE, 4), lessThan(ATTRIBUTE, 6));
        assertPredicateTrue(and1, 5);
        final Predicate and2 = and(greaterThan(ATTRIBUTE, 5), lessThan(ATTRIBUTE, 6));
        assertPredicateFalse(and2, 4);
        final Predicate and3 = and(greaterThan(ATTRIBUTE, 4), lessThan(ATTRIBUTE, 6), equal(ATTRIBUTE, 5));
        assertPredicateTrue(and3, 5);
        final Predicate and4 = Predicates.and(greaterThan(ATTRIBUTE, 3), lessThan(ATTRIBUTE, 6), equal(ATTRIBUTE, 4));
        assertPredicateFalse(and4, 5);
    }

    @Test
    public void testOr() {
        final Predicate or1 = or(equal(ATTRIBUTE, 3), equal(ATTRIBUTE, 4), equal(ATTRIBUTE, 5));
        assertPredicateTrue(or1, 4);
        assertPredicateFalse(or1, 6);
    }

    @Test
    public void testGreaterEqual() {
        assertPredicateTrue(greaterEqual(ATTRIBUTE, 5), 5);
    }

    @Test
    public void testLessThan() {
        assertPredicateTrue(lessThan(ATTRIBUTE, 7), 6);
        assertPredicateFalse(lessThan(ATTRIBUTE, 3), 4);
        assertPredicateFalse(lessThan(ATTRIBUTE, 4), 4);
        assertPredicateTrue(lessThan(ATTRIBUTE, "tc"), "bz");
        assertPredicateFalse(lessThan(ATTRIBUTE, "gx"), "h0");
    }

    @Test
    public void testGreaterThan() {
        assertPredicateTrue(greaterThan(ATTRIBUTE, 5), 6);
        assertPredicateFalse(greaterThan(ATTRIBUTE, 5), 4);
        assertPredicateFalse(greaterThan(ATTRIBUTE, 5), 5);
        assertPredicateTrue(greaterThan(ATTRIBUTE, "aa"), "xa");
        assertPredicateFalse(greaterThan(ATTRIBUTE, "da"), "cz");
        assertPredicateTrue(greaterThan(ATTRIBUTE, new BigDecimal("1.23E2")), new BigDecimal("1.23E3"));
    }

    @Test
    public void testLessEqual() {
        assertPredicateTrue(lessEqual(ATTRIBUTE, 4), 4);
    }

    @Test
    public void testPredicatesAgainstANullField() {
        assertFalse_withNullEntry(lessEqual("nullField", 1));

        assertFalse_withNullEntry(in("nullField", 1));
        assertFalse_withNullEntry(lessThan("nullField", 1));
        assertFalse_withNullEntry(greaterEqual("nullField", 1));
        assertFalse_withNullEntry(greaterThan("nullField", 1));
        assertFalse_withNullEntry(equal("nullField", 1));
        assertFalse_withNullEntry(notEqual("nullField", null));
        assertFalse_withNullEntry(between("nullField", 1, 1));
        assertTrue_withNullEntry(like("nullField", null));
        assertTrue_withNullEntry(ilike("nullField", null));
        assertTrue_withNullEntry(regex("nullField", null));
        assertTrue_withNullEntry(notEqual("nullField", 1));
    }

    @Test
    public void testBetween() {
        assertPredicateTrue(between(ATTRIBUTE, 4, 6), 5);
        assertPredicateTrue(between(ATTRIBUTE, 5, 6), 5);
        assertPredicateTrue(between(ATTRIBUTE, "abc", "xyz"), "prs");
        assertPredicateFalse(between(ATTRIBUTE, "klmn", "xyz"), "efgh");
        assertPredicateFalse(between(ATTRIBUTE, 6, 7), 5);
    }

    @Test
    public void testIn() {
        assertPredicateTrue(in(ATTRIBUTE, 4, 7, 8, 5), 5);
        assertPredicateTrue(in(ATTRIBUTE, 5, 7, 8), 5);
        assertPredicateFalse(in(ATTRIBUTE, 6, 7, 8), 5);
        assertPredicateFalse(in(ATTRIBUTE, 6, 7, 8), 9);
    }

    @Test
    public void testLike() {
        assertPredicateTrue(like(ATTRIBUTE, "J%"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "Ja%"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "J_v_"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "_av_"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "_a__"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "J%v_"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "J%_"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "java"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "j%"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "J_a"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "J_ava"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "J_a_a"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "J_av__"), "Java");
        assertPredicateFalse(like(ATTRIBUTE, "J_Va"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "Java World"), "Java World");
        assertPredicateTrue(like(ATTRIBUTE, "Java%ld"), "Java World");
        assertPredicateTrue(like(ATTRIBUTE, "%World"), "Java World");
        assertPredicateTrue(like(ATTRIBUTE, "Java_World"), "Java World");

        assertPredicateTrue(like(ATTRIBUTE, "J.-*.*\\%"), "J.-*.*%");
        assertPredicateTrue(like(ATTRIBUTE, "J\\_"), "J_");
        assertPredicateTrue(like(ATTRIBUTE, "J%"), "Java");
        assertPredicateTrue(like(ATTRIBUTE, "J%"), "Java\n");
    }

    @Test
    public void testILike() {
        assertPredicateFalse(like(ATTRIBUTE, "JavaWorld"), "Java World");
        assertPredicateTrue(ilike(ATTRIBUTE, "Java_World"), "java World");
        assertPredicateTrue(ilike(ATTRIBUTE, "java%ld"), "Java World");
        assertPredicateTrue(ilike(ATTRIBUTE, "%world"), "Java World");
        assertPredicateFalse(ilike(ATTRIBUTE, "Java_World"), "gava World");
        assertPredicateTrue(ilike(ATTRIBUTE, "J%"), "java\nworld");
    }

    @Test
    public void testILike_Id() {
        ILikePredicate predicate = (ILikePredicate) ilike(ATTRIBUTE, "Java_World");

        assertThat(predicate.getClassId())
                .isEqualTo(6)
                .isEqualTo(PredicateDataSerializerHook.ILIKE_PREDICATE);
    }

    @Test
    public void testIsInstanceOf() {
        assertTrue(instanceOf(Long.class).apply(new DummyEntry(1L)));
        assertFalse(instanceOf(Long.class).apply(new DummyEntry("Java")));
        assertTrue(instanceOf(Number.class).apply(new DummyEntry(4)));
    }

    @Test
    public void testCriteriaAPI() {
        Object value = new Employee(12, "abc-123-xvz", 34, true, 10D);
        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        EntryObject e2 = e.get("age");
        Predicate predicate = e2.greaterEqual(29).and(e2.lessEqual(36));
        assertTrue(predicate.apply(createEntry("1", value)));
        e = Predicates.newPredicateBuilder().getEntryObject();
        assertTrue(e.get("id").equal(12).apply(createEntry("1", value)));
    }

    @Test(expected = NullPointerException.class)
    public void testBetweenNull() {
        Predicates.between(ATTRIBUTE, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testLessThanNull() {
        Predicates.lessThan(ATTRIBUTE, null);
    }

    @Test(expected = NullPointerException.class)
    public void testLessEqualNull() {
        Predicates.lessEqual(ATTRIBUTE, null);
    }

    @Test(expected = NullPointerException.class)
    public void testGreaterThanNull() {
        Predicates.greaterThan(ATTRIBUTE, null);
    }

    @Test(expected = NullPointerException.class)
    public void testGreaterEqualNull() {
        Predicates.greaterEqual(ATTRIBUTE, null);
    }

    @Test(expected = NullPointerException.class)
    public void testInNullWithNullArray() {
        Predicates.in(ATTRIBUTE, (Comparable<?>[]) null);
    }

    private class DummyEntry extends QueryEntry {

        DummyEntry(Comparable attribute) {
            super(ss, toData("1"), attribute, Extractors.newBuilder(ss).build());
        }

        @Override
        public Comparable getAttributeValue(String attributeName) throws QueryException {
            return (Comparable) getValue();
        }
    }

    private final class NullDummyEntry extends QueryableEntry {

        private Integer nullField;

        private NullDummyEntry() {
        }

        public Integer getNullField() {
            return nullField;
        }

        public void setNullField(Integer nullField) {
            this.nullField = nullField;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public Object setValue(Object value) {
            return null;
        }

        @Override
        public Object getKey() {
            return 1;
        }

        @Override
        public Comparable getAttributeValue(String attributeName) throws QueryException {
            return null;
        }

        @Override
        protected Object getTargetObject(boolean key) {
            return null;
        }

        @Override
        public Data getKeyData() {
            return null;
        }

        @Override
        public Data getValueData() {
            return null;
        }

        @Override
        public Object getKeyIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getKeyDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Object getValueIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }

        @Override
        public Data getValueDataIfPresent() {
            throw new UnsupportedOperationException("Should not be called.");
        }
    }

    private Entry createEntry(final Object key, final Object value) {
        return new QueryEntry(ss, toData(key), value, Extractors.newBuilder(ss).build());
    }

    private void assertPredicateTrue(Predicate p, Comparable comparable) {
        assertTrue(p.apply(new DummyEntry(comparable)));
    }

    private void assertPredicateFalse(Predicate p, Comparable comparable) {
        assertFalse(p.apply(new DummyEntry(comparable)));
    }

    private void assertTrue_withNullEntry(Predicate p) {
        assertTrue(p.apply(new NullDummyEntry()));
    }

    private void assertFalse_withNullEntry(Predicate p) {
        assertFalse(p.apply(new NullDummyEntry()));
    }
}
