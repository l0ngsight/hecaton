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

package uk.org.hecaton.jet.sql.impl;

import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.ArrayDataSerializableFactory;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.jet.sql.impl.ExpressionUtil.SqlRowComparator;
import uk.org.hecaton.jet.sql.impl.connector.map.LazyDefiningSpecificMemberPms;
import uk.org.hecaton.jet.sql.impl.connector.map.RowProjectorProcessorSupplier;
import uk.org.hecaton.jet.sql.impl.expression.UdtObjectToJsonFunction;
import uk.org.hecaton.jet.sql.impl.expression.json.JsonArrayFunction;
import uk.org.hecaton.jet.sql.impl.expression.json.JsonObjectFunction;
import uk.org.hecaton.jet.sql.impl.expression.json.JsonParseFunction;
import uk.org.hecaton.jet.sql.impl.expression.json.JsonQueryFunction;
import uk.org.hecaton.jet.sql.impl.expression.json.JsonValueFunction;
import uk.org.hecaton.jet.sql.impl.opt.FieldCollation;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateAccumulateFunction;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateArrayAggSupplier;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateAvgSupplier;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateCountSupplier;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateCreateSupplier;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateObjectAggSupplier;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.AggregateSumSupplier;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.RowGetFn;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.RowGetMaybeSerializedFn;
import uk.org.hecaton.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule.RowIdentityFn;
import uk.org.hecaton.jet.sql.impl.validate.UpdateDataConnectionOperation;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.sql.impl.LazyTarget;
import uk.org.hecaton.sql.impl.SqlDataSerializerHook;
import uk.org.hecaton.sql.impl.exec.scan.MapIndexScanMetadata;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexCompositeFilter;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexEqualsFilter;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexFilterValue;
import uk.org.hecaton.sql.impl.exec.scan.index.IndexRangeFilter;
import uk.org.hecaton.sql.impl.expression.CaseExpression;
import uk.org.hecaton.sql.impl.expression.CastExpression;
import uk.org.hecaton.sql.impl.expression.ColumnExpression;
import uk.org.hecaton.sql.impl.expression.ConstantExpression;
import uk.org.hecaton.sql.impl.expression.FieldAccessExpression;
import uk.org.hecaton.sql.impl.expression.ParameterExpression;
import uk.org.hecaton.sql.impl.expression.RowExpression;
import uk.org.hecaton.sql.impl.expression.SargExpression;
import uk.org.hecaton.sql.impl.expression.datetime.ExtractFunction;
import uk.org.hecaton.sql.impl.expression.datetime.ToCharFunction;
import uk.org.hecaton.sql.impl.expression.datetime.ToEpochMillisFunction;
import uk.org.hecaton.sql.impl.expression.datetime.ToTimestampTzFunction;
import uk.org.hecaton.sql.impl.expression.math.AbsFunction;
import uk.org.hecaton.sql.impl.expression.math.DivideFunction;
import uk.org.hecaton.sql.impl.expression.math.DoubleBiFunction;
import uk.org.hecaton.sql.impl.expression.math.DoubleFunction;
import uk.org.hecaton.sql.impl.expression.math.FloorCeilFunction;
import uk.org.hecaton.sql.impl.expression.math.MinusFunction;
import uk.org.hecaton.sql.impl.expression.math.MultiplyFunction;
import uk.org.hecaton.sql.impl.expression.math.PlusFunction;
import uk.org.hecaton.sql.impl.expression.math.RandFunction;
import uk.org.hecaton.sql.impl.expression.math.RemainderFunction;
import uk.org.hecaton.sql.impl.expression.math.RoundTruncateFunction;
import uk.org.hecaton.sql.impl.expression.math.SignFunction;
import uk.org.hecaton.sql.impl.expression.math.UnaryMinusFunction;
import uk.org.hecaton.sql.impl.expression.predicate.AndPredicate;
import uk.org.hecaton.sql.impl.expression.predicate.ComparisonPredicate;
import uk.org.hecaton.sql.impl.expression.predicate.IsFalsePredicate;
import uk.org.hecaton.sql.impl.expression.predicate.IsNotFalsePredicate;
import uk.org.hecaton.sql.impl.expression.predicate.IsNotNullPredicate;
import uk.org.hecaton.sql.impl.expression.predicate.IsNotTruePredicate;
import uk.org.hecaton.sql.impl.expression.predicate.IsNullPredicate;
import uk.org.hecaton.sql.impl.expression.predicate.IsTruePredicate;
import uk.org.hecaton.sql.impl.expression.predicate.NotPredicate;
import uk.org.hecaton.sql.impl.expression.predicate.OrPredicate;
import uk.org.hecaton.sql.impl.expression.predicate.SearchPredicate;
import uk.org.hecaton.sql.impl.expression.service.GetDdlFunction;
import uk.org.hecaton.sql.impl.expression.string.AsciiFunction;
import uk.org.hecaton.sql.impl.expression.string.CharLengthFunction;
import uk.org.hecaton.sql.impl.expression.string.ConcatFunction;
import uk.org.hecaton.sql.impl.expression.string.ConcatWSFunction;
import uk.org.hecaton.sql.impl.expression.string.InitcapFunction;
import uk.org.hecaton.sql.impl.expression.string.LikeFunction;
import uk.org.hecaton.sql.impl.expression.string.LowerFunction;
import uk.org.hecaton.sql.impl.expression.string.PositionFunction;
import uk.org.hecaton.sql.impl.expression.string.ReplaceFunction;
import uk.org.hecaton.sql.impl.expression.string.SubstringFunction;
import uk.org.hecaton.sql.impl.expression.string.TrimFunction;
import uk.org.hecaton.sql.impl.expression.string.UpperFunction;
import uk.org.hecaton.sql.impl.extract.GenericQueryTargetDescriptor;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.EmptyRow;
import uk.org.hecaton.sql.impl.row.HeapRow;
import uk.org.hecaton.sql.impl.schema.Mapping;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import uk.org.hecaton.sql.impl.schema.type.Type;
import uk.org.hecaton.sql.impl.schema.type.Type.TypeField;
import uk.org.hecaton.sql.impl.schema.view.View;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.sql.impl.type.QueryDataType.QueryDataTypeField;
import uk.org.hecaton.sql.impl.type.SqlDaySecondInterval;
import uk.org.hecaton.sql.impl.type.SqlYearMonthInterval;

import java.util.Map;
import java.util.function.Supplier;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY_ID;

/**
 * Serialization hook for Jet SQL engine classes.
 */
public class JetSqlSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(JET_SQL_DS_FACTORY, JET_SQL_DS_FACTORY_ID);

    public static final int JSON_QUERY = 0;
    public static final int JSON_PARSE = 1;
    public static final int JSON_VALUE = 2;
    public static final int JSON_OBJECT = 3;
    public static final int JSON_ARRAY = 4;
    public static final int MAP_INDEX_SCAN_METADATA = 5;
    public static final int ROW_PROJECTOR_PROCESSOR_SUPPLIER = 6;
    public static final int KV_ROW_PROJECTOR_SUPPLIER = 7;
    public static final int ROOT_RESULT_CONSUMER_SINK_SUPPLIER = 8;
    public static final int SQL_ROW_COMPARATOR = 9;
    public static final int FIELD_COLLATION = 10;
    public static final int ROW_GET_MAYBE_SERIALIZED_FN = 11;
    public static final int NULL_FUNCTION = 12;
    public static final int ROW_GET_FN = 13;
    public static final int AGGREGATE_CREATE_SUPPLIER = 14;
    public static final int AGGREGATE_ACCUMULATE_FUNCTION = 15;
    public static final int AGGREGATE_COMBINE_FUNCTION = 16;
    public static final int AGGREGATE_FINISH_FUNCTION = 17;
    public static final int AGGREGATE_SUM_SUPPLIER = 18;
    public static final int AGGREGATE_AVG_SUPPLIER = 19;
    public static final int AGGREGATE_COUNT_SUPPLIER = 20;
    public static final int AGGREGATE_JSON_ARRAY_AGG_SUPPLIER = 21;
    public static final int ROW_IDENTITY_FN = 22;
    public static final int AGGREGATE_EXPORT_FUNCTION = 23;
    public static final int AGGREGATE_JSON_OBJECT_AGG_SUPPLIER = 24;
    public static final int UDT_OBJECT_TO_JSON = 26;
    public static final int INDEX_FILTER_VALUE = 27;
    public static final int INDEX_FILTER_EQUALS = 28;
    public static final int INDEX_FILTER_RANGE = 29;
    public static final int INDEX_FILTER_IN = 30;
    public static final int EXPRESSION_TO_CHAR = 31;
    public static final int EXPRESSION_COLUMN = 32;
    public static final int EXPRESSION_IS_NULL = 33;
    public static final int EXPRESSION_CONSTANT = 34;
    public static final int EXPRESSION_PARAMETER = 35;
    public static final int EXPRESSION_CAST = 36;
    public static final int EXPRESSION_DIVIDE = 37;
    public static final int EXPRESSION_MINUS = 38;
    public static final int EXPRESSION_MULTIPLY = 39;
    public static final int EXPRESSION_PLUS = 40;
    public static final int EXPRESSION_UNARY_MINUS = 41;
    public static final int EXPRESSION_AND = 42;
    public static final int EXPRESSION_OR = 43;
    public static final int EXPRESSION_NOT = 44;
    public static final int EXPRESSION_COMPARISON = 45;
    public static final int EXPRESSION_IS_TRUE = 46;
    public static final int EXPRESSION_IS_NOT_TRUE = 47;
    public static final int EXPRESSION_IS_FALSE = 48;
    public static final int EXPRESSION_IS_NOT_FALSE = 49;
    public static final int EXPRESSION_IS_NOT_NULL = 50;

    public static final int EXPRESSION_ABS = 51;
    public static final int EXPRESSION_SIGN = 52;
    public static final int EXPRESSION_RAND = 53;
    public static final int EXPRESSION_DOUBLE = 54;
    public static final int EXPRESSION_FLOOR_CEIL = 55;
    public static final int EXPRESSION_ROUND_TRUNCATE = 56;

    //region String expressions IDs
    public static final int EXPRESSION_ASCII = 57;
    public static final int EXPRESSION_CHAR_LENGTH = 58;
    public static final int EXPRESSION_INITCAP = 59;
    public static final int EXPRESSION_LOWER = 60;
    public static final int EXPRESSION_UPPER = 61;
    public static final int EXPRESSION_CONCAT = 62;
    public static final int EXPRESSION_LIKE = 63;
    public static final int EXPRESSION_SUBSTRING = 64;
    public static final int EXPRESSION_TRIM = 65;
    public static final int EXPRESSION_REMAINDER = 66;
    public static final int EXPRESSION_CONCAT_WS = 67;
    public static final int EXPRESSION_REPLACE = 68;
    public static final int EXPRESSION_POSITION = 69;
    public static final int EXPRESSION_CASE = 70;
    public static final int EXPRESSION_EXTRACT = 71;
    //endregion

    public static final int EXPRESSION_DOUBLE_DOUBLE = 72;
    public static final int EXPRESSION_TO_TIMESTAMP_TZ = 73;
    public static final int EXPRESSION_TO_EPOCH_MILLIS = 74;
    public static final int SARG_EXPRESSION = 75;
    public static final int SEARCH_PREDICATE = 76;
    public static final int EXPRESSION_FIELD_ACCESS = 77;
    public static final int EXPRESSION_ROW = 78;

    public static final int DATA_CONNECTION = 79;

    public static final int UPDATE_DATA_CONNECTION_OPERATION = 80;

    public static final int ROW_HEAP = 82;
    public static final int ROW_EMPTY = 83;

    public static final int LAZY_TARGET = 84;

    public static final int TARGET_DESCRIPTOR_GENERIC = 85;

    public static final int QUERY_PATH = 86;

    public static final int INTERVAL_YEAR_MONTH = 87;
    public static final int INTERVAL_DAY_SECOND = 88;

    public static final int EXPRESSION_GET_DDL = 90;

    public static final int LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER = 91;

    public static final int LEN = LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[JSON_QUERY] = JsonQueryFunction::new;
        constructors[JSON_PARSE] = JsonParseFunction::new;
        constructors[JSON_VALUE] = JsonValueFunction::new;
        constructors[JSON_OBJECT] = JsonObjectFunction::new;
        constructors[JSON_ARRAY] = JsonArrayFunction::new;
        constructors[MAP_INDEX_SCAN_METADATA] = MapIndexScanMetadata::new;
        constructors[ROW_PROJECTOR_PROCESSOR_SUPPLIER] = RowProjectorProcessorSupplier::new;
        constructors[KV_ROW_PROJECTOR_SUPPLIER] = uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector.Supplier::new;
        constructors[ROOT_RESULT_CONSUMER_SINK_SUPPLIER] =
                uk.org.hecaton.jet.sql.impl.processors.RootResultConsumerSink.Supplier::new;
        constructors[SQL_ROW_COMPARATOR] = SqlRowComparator::new;
        constructors[FIELD_COLLATION] = FieldCollation::new;
        constructors[ROW_GET_MAYBE_SERIALIZED_FN] = RowGetMaybeSerializedFn::new;
        constructors[NULL_FUNCTION] = () -> AggregateAbstractPhysicalRule.NullFunction.INSTANCE;
        constructors[ROW_GET_FN] = RowGetFn::new;
        constructors[AGGREGATE_CREATE_SUPPLIER] = AggregateCreateSupplier::new;
        constructors[AGGREGATE_ACCUMULATE_FUNCTION] = AggregateAccumulateFunction::new;
        constructors[AGGREGATE_COMBINE_FUNCTION] = () -> AggregateAbstractPhysicalRule.AggregateCombineFunction.INSTANCE;
        constructors[AGGREGATE_FINISH_FUNCTION] = () -> AggregateAbstractPhysicalRule.AggregateFinishFunction.INSTANCE;
        constructors[AGGREGATE_SUM_SUPPLIER] = AggregateSumSupplier::new;
        constructors[AGGREGATE_AVG_SUPPLIER] = AggregateAvgSupplier::new;
        constructors[AGGREGATE_COUNT_SUPPLIER] = AggregateCountSupplier::new;
        constructors[AGGREGATE_JSON_ARRAY_AGG_SUPPLIER] = AggregateArrayAggSupplier::new;
        constructors[ROW_IDENTITY_FN] = RowIdentityFn::new;
        constructors[AGGREGATE_EXPORT_FUNCTION] = () -> AggregateAbstractPhysicalRule.AggregateExportFunction.INSTANCE;
        constructors[AGGREGATE_JSON_OBJECT_AGG_SUPPLIER] = AggregateObjectAggSupplier::new;
        constructors[UDT_OBJECT_TO_JSON] = UdtObjectToJsonFunction::new;
        constructors[UPDATE_DATA_CONNECTION_OPERATION] = UpdateDataConnectionOperation::new;

        constructors[INDEX_FILTER_VALUE] = IndexFilterValue::new;
        constructors[INDEX_FILTER_EQUALS] = IndexEqualsFilter::new;
        constructors[INDEX_FILTER_RANGE] = IndexRangeFilter::new;
        constructors[INDEX_FILTER_IN] = IndexCompositeFilter::new;

        constructors[EXPRESSION_TO_CHAR] = ToCharFunction::new;
        constructors[EXPRESSION_COLUMN] = ColumnExpression::new;
        constructors[EXPRESSION_IS_NULL] = IsNullPredicate::new;

        constructors[EXPRESSION_CONSTANT] = ConstantExpression::new;
        constructors[EXPRESSION_PARAMETER] = ParameterExpression::new;
        constructors[EXPRESSION_CAST] = CastExpression::new;
        constructors[EXPRESSION_DIVIDE] = DivideFunction::new;
        constructors[EXPRESSION_MINUS] = MinusFunction::new;
        constructors[EXPRESSION_MULTIPLY] = MultiplyFunction::new;
        constructors[EXPRESSION_PLUS] = PlusFunction::new;
        constructors[EXPRESSION_UNARY_MINUS] = UnaryMinusFunction::new;
        constructors[EXPRESSION_AND] = AndPredicate::new;
        constructors[EXPRESSION_OR] = OrPredicate::new;
        constructors[EXPRESSION_NOT] = NotPredicate::new;
        constructors[EXPRESSION_COMPARISON] = ComparisonPredicate::new;
        constructors[EXPRESSION_IS_TRUE] = IsTruePredicate::new;
        constructors[EXPRESSION_IS_NOT_TRUE] = IsNotTruePredicate::new;
        constructors[EXPRESSION_IS_FALSE] = IsFalsePredicate::new;
        constructors[EXPRESSION_IS_NOT_FALSE] = IsNotFalsePredicate::new;
        constructors[EXPRESSION_IS_NOT_NULL] = IsNotNullPredicate::new;

        constructors[EXPRESSION_ABS] = AbsFunction::new;
        constructors[EXPRESSION_SIGN] = SignFunction::new;
        constructors[EXPRESSION_RAND] = RandFunction::new;
        constructors[EXPRESSION_DOUBLE] = DoubleFunction::new;
        constructors[EXPRESSION_FLOOR_CEIL] = FloorCeilFunction::new;
        constructors[EXPRESSION_ROUND_TRUNCATE] = RoundTruncateFunction::new;

        constructors[EXPRESSION_ASCII] = AsciiFunction::new;
        constructors[EXPRESSION_CHAR_LENGTH] = CharLengthFunction::new;
        constructors[EXPRESSION_INITCAP] = InitcapFunction::new;
        constructors[EXPRESSION_LOWER] = LowerFunction::new;
        constructors[EXPRESSION_UPPER] = UpperFunction::new;
        constructors[EXPRESSION_CONCAT] = ConcatFunction::new;
        constructors[EXPRESSION_LIKE] = LikeFunction::new;
        constructors[EXPRESSION_SUBSTRING] = SubstringFunction::new;
        constructors[EXPRESSION_TRIM] = TrimFunction::new;
        constructors[EXPRESSION_REPLACE] = ReplaceFunction::new;
        constructors[EXPRESSION_POSITION] = PositionFunction::new;
        constructors[EXPRESSION_REMAINDER] = RemainderFunction::new;
        constructors[EXPRESSION_CONCAT_WS] = ConcatWSFunction::new;
        constructors[EXPRESSION_CASE] = CaseExpression::new;
        constructors[EXPRESSION_EXTRACT] = ExtractFunction::new;

        constructors[EXPRESSION_DOUBLE_DOUBLE] = DoubleBiFunction::new;
        constructors[EXPRESSION_TO_TIMESTAMP_TZ] = ToTimestampTzFunction::new;
        constructors[EXPRESSION_TO_EPOCH_MILLIS] = ToEpochMillisFunction::new;

        constructors[SARG_EXPRESSION] = SargExpression::new;
        constructors[SEARCH_PREDICATE] = SearchPredicate::new;
        constructors[EXPRESSION_FIELD_ACCESS] = FieldAccessExpression::new;
        constructors[EXPRESSION_ROW] = RowExpression::new;

        constructors[DATA_CONNECTION] = DataConnectionCatalogEntry::new;

        constructors[ROW_HEAP] = HeapRow::new;
        constructors[ROW_EMPTY] = () -> EmptyRow.INSTANCE;

        constructors[LAZY_TARGET] = LazyTarget::new;

        constructors[TARGET_DESCRIPTOR_GENERIC] = () -> GenericQueryTargetDescriptor.DEFAULT;

        constructors[QUERY_PATH] = QueryPath::new;

        constructors[INTERVAL_YEAR_MONTH] = SqlYearMonthInterval::new;
        constructors[INTERVAL_DAY_SECOND] = SqlDaySecondInterval::new;

        constructors[EXPRESSION_GET_DDL] = GetDdlFunction::new;

        constructors[LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER] = LazyDefiningSpecificMemberPms::new;

        return new ArrayDataSerializableFactory(constructors);
    }

    @Override
    public void afterFactoriesCreated(Map<Integer, DataSerializableFactory> factories) {
        ArrayDataSerializableFactory mapDataFactory = (ArrayDataSerializableFactory) factories.get(SqlDataSerializerHook.F_ID);

        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[SqlDataSerializerHook.LEN];
        constructors[SqlDataSerializerHook.QUERY_DATA_TYPE] = QueryDataType::new;
        // SqlDataSerializerHook.QUERY_ID
        constructors[SqlDataSerializerHook.MAPPING] = Mapping::new;
        constructors[SqlDataSerializerHook.MAPPING_FIELD] = MappingField::new;
        constructors[SqlDataSerializerHook.VIEW] = View::new;
        constructors[SqlDataSerializerHook.TYPE] = Type::new;
        constructors[SqlDataSerializerHook.TYPE_FIELD] = TypeField::new;
        // SqlDataSerializerHook.ROW_VALUE
        constructors[SqlDataSerializerHook.QUERY_DATA_TYPE_FIELD] = QueryDataTypeField::new;
        for (QueryDataType type : QueryDataType.values()) {
            constructors[SqlDataSerializerHook.PREDEFINED_QUERY_DATA_TYPE_BASE + type.getConverter().getId()] = () -> type;
        }

        mapDataFactory.mergeConstructors(constructors);
    }
}
