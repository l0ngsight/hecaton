/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.jet.clickhouse.impl;

import uk.org.hecaton.nio.serialization.FieldKind;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import com.clickhouse.data.ClickHouseColumn;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.UUID;

/**
 * The Java type of a ClickHouse column as Hecaton sees it, with the conversions between the
 * values of the ClickHouse client and the fields of compact {@link GenericRecord}s.
 * <p>
 * Values read from ClickHouse are normalized to one Java type per column: {@code Boolean},
 * {@code Byte}, {@code Short}, {@code Integer}, {@code Long}, {@code Float}, {@code Double},
 * {@code BigDecimal}, {@code String}, {@code LocalDate} or {@code OffsetDateTime}.
 */
public final class ColumnType {

    /**
     * The kinds of values Hecaton supports in ClickHouse columns.
     */
    public enum Kind {
        BOOLEAN, INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, DECIMAL, STRING, DATE, TIMESTAMP_WITH_TIMEZONE
    }

    private static final DateTimeFormatter PARAMETER_TIME_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS");

    private final ClickHouseColumn column;
    private final Kind kind;

    private ColumnType(ClickHouseColumn column, Kind kind) {
        this.column = column;
        this.kind = kind;
    }

    /**
     * Returns the type of the column, or fails if Hecaton does not support its ClickHouse type.
     */
    public static ColumnType of(ClickHouseColumn column) {
        Kind kind = kindOf(column);
        if (kind == null) {
            throw new IllegalArgumentException("Column '" + column.getColumnName() + "' has the type "
                    + column.getOriginalTypeName() + ", which Hecaton does not support. Supported: Bool, "
                    + "(U)Int8 to (U)Int256, Float32, Float64, Decimal, String, FixedString, Enum, UUID, "
                    + "IPv4, IPv6, Date, Date32, DateTime, DateTime64, also as Nullable or LowCardinality");
        }
        return new ColumnType(column, kind);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static Kind kindOf(ClickHouseColumn column) {
        if (column.isArray() || column.getDataType() == null) {
            return null;
        }
        return switch (column.getDataType()) {
            case Bool -> Kind.BOOLEAN;
            case Int8 -> Kind.INT8;
            case UInt8, Int16 -> Kind.INT16;
            case UInt16, Int32 -> Kind.INT32;
            case UInt32, Int64 -> Kind.INT64;
            case UInt64, Int128, UInt128, Int256, UInt256, Decimal, Decimal32, Decimal64, Decimal128, Decimal256 ->
                    Kind.DECIMAL;
            case Float32 -> Kind.FLOAT32;
            case Float64 -> Kind.FLOAT64;
            case String, FixedString, Enum, Enum8, Enum16, UUID, IPv4, IPv6 -> Kind.STRING;
            case Date, Date32 -> Kind.DATE;
            case DateTime, DateTime32, DateTime64 -> Kind.TIMESTAMP_WITH_TIMEZONE;
            default -> null;
        };
    }

    public ClickHouseColumn column() {
        return column;
    }

    public String name() {
        return column.getColumnName();
    }

    public Kind kind() {
        return kind;
    }

    public boolean isNullable() {
        return column.isNullable();
    }

    /**
     * Returns the ClickHouse type of a query parameter for values of this column.
     */
    public String parameterType() {
        if (kind == Kind.TIMESTAMP_WITH_TIMEZONE) {
            // times are sent as instants, whatever the column's time zone
            return "DateTime64(9, 'UTC')";
        }
        String type = column.getOriginalTypeName();
        if (column.isLowCardinality() && type.startsWith("LowCardinality(")) {
            type = type.substring("LowCardinality(".length(), type.length() - 1);
        }
        return type;
    }

    /**
     * Returns the value of a query parameter of {@linkplain #parameterType() its type} for a
     * value {@linkplain #toClickHouse converted} for this column.
     */
    public Object toParameter(Object clickHouseValue) {
        if (clickHouseValue instanceof ZonedDateTime time) {
            return PARAMETER_TIME_FORMAT.format(time.withZoneSameInstant(ZoneOffset.UTC));
        } else if (clickHouseValue instanceof InetAddress address) {
            return address.getHostAddress();
        }
        return clickHouseValue;
    }

    /**
     * Converts a value read by the ClickHouse client to this column's Java type.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public Object fromClickHouse(Object value) {
        if (value == null) {
            return null;
        }
        return switch (kind) {
            case BOOLEAN -> value instanceof Boolean ? value : ((Number) value).intValue() != 0;
            case INT8 -> ((Number) value).byteValue();
            case INT16 -> ((Number) value).shortValue();
            case INT32 -> ((Number) value).intValue();
            case INT64 -> ((Number) value).longValue();
            case FLOAT32 -> ((Number) value).floatValue();
            case FLOAT64 -> ((Number) value).doubleValue();
            case DECIMAL -> toBigDecimal(value);
            case STRING -> value instanceof InetAddress address ? address.getHostAddress() : value.toString();
            case DATE -> value;
            case TIMESTAMP_WITH_TIMEZONE -> value instanceof ZonedDateTime zoned
                    ? zoned.toOffsetDateTime()
                    : toOffsetDateTime(value);
        };
    }

    /**
     * Converts a Java value to the value the ClickHouse client writes to this column.
     *
     * @throws IllegalArgumentException if the value cannot be stored in the column
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public Object toClickHouse(Object value) {
        if (value == null) {
            if (!column.isNullable()) {
                throw new IllegalArgumentException("Column '" + name() + "' is not Nullable, but the value is null");
            }
            return null;
        }
        try {
            return switch (column.getDataType()) {
                case Bool -> value instanceof Boolean ? value : ((Number) value).intValue() != 0;
                case Int8 -> ((Number) value).byteValue();
                case UInt8, Int16 -> ((Number) value).shortValue();
                case UInt16, Int32 -> ((Number) value).intValue();
                case UInt32, Int64 -> ((Number) value).longValue();
                case UInt64, Int128, UInt128, Int256, UInt256 -> toBigInteger(value);
                case Decimal, Decimal32, Decimal64, Decimal128, Decimal256 -> toBigDecimal(value);
                case Float32 -> ((Number) value).floatValue();
                case Float64 -> ((Number) value).doubleValue();
                case UUID -> value instanceof UUID ? value : UUID.fromString(value.toString());
                case IPv4, IPv6 -> value instanceof InetAddress ? value : InetAddress.getByName(value.toString());
                case Date, Date32 -> value instanceof LocalDate ? value : LocalDate.parse(value.toString());
                case DateTime, DateTime32, DateTime64 -> toZonedDateTime(value);
                default -> value.toString();
            };
        } catch (ClassCastException | IllegalArgumentException | UnknownHostException
                 | java.time.DateTimeException e) {
            throw new IllegalArgumentException("Cannot store the " + value.getClass().getName() + " value '" + value
                    + "' in column '" + name() + "' of type " + column.getOriginalTypeName(), e);
        }
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        } else if (value instanceof BigInteger integer) {
            return new BigDecimal(integer);
        } else if (value instanceof Double || value instanceof Float) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        } else if (value instanceof Number number) {
            return BigDecimal.valueOf(number.longValue());
        }
        return new BigDecimal(value.toString());
    }

    private static BigInteger toBigInteger(Object value) {
        if (value instanceof BigInteger integer) {
            return integer;
        } else if (value instanceof BigDecimal decimal) {
            return decimal.toBigIntegerExact();
        } else if (value instanceof Number number) {
            return BigInteger.valueOf(number.longValue());
        }
        return new BigInteger(value.toString());
    }

    private static OffsetDateTime toOffsetDateTime(Object value) {
        if (value instanceof OffsetDateTime offset) {
            return offset;
        } else if (value instanceof Instant instant) {
            return instant.atOffset(ZoneOffset.UTC);
        } else if (value instanceof LocalDateTime local) {
            return local.atOffset(ZoneOffset.UTC);
        }
        return OffsetDateTime.parse(value.toString());
    }

    private ZonedDateTime toZonedDateTime(Object value) {
        if (value instanceof ZonedDateTime zoned) {
            return zoned;
        } else if (value instanceof OffsetDateTime offset) {
            return offset.toZonedDateTime();
        } else if (value instanceof Instant instant) {
            return instant.atZone(ZoneOffset.UTC);
        } else if (value instanceof LocalDateTime local) {
            // a time without an offset is in the column's time zone, as ClickHouse reads such values
            TimeZone timeZone = column.getTimeZone();
            return local.atZone(timeZone == null ? ZoneOffset.UTC : ZoneId.of(timeZone.getID()));
        }
        return OffsetDateTime.parse(value.toString()).toZonedDateTime();
    }

    /**
     * Sets the field of this column in a compact record to a value of this column's Java type.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void setField(GenericRecordBuilder builder, String fieldName, Object value) {
        boolean nullable = column.isNullable();
        switch (kind) {
            case BOOLEAN -> {
                if (nullable) {
                    builder.setNullableBoolean(fieldName, (Boolean) value);
                } else {
                    builder.setBoolean(fieldName, (Boolean) value);
                }
            }
            case INT8 -> {
                if (nullable) {
                    builder.setNullableInt8(fieldName, (Byte) value);
                } else {
                    builder.setInt8(fieldName, (Byte) value);
                }
            }
            case INT16 -> {
                if (nullable) {
                    builder.setNullableInt16(fieldName, (Short) value);
                } else {
                    builder.setInt16(fieldName, (Short) value);
                }
            }
            case INT32 -> {
                if (nullable) {
                    builder.setNullableInt32(fieldName, (Integer) value);
                } else {
                    builder.setInt32(fieldName, (Integer) value);
                }
            }
            case INT64 -> {
                if (nullable) {
                    builder.setNullableInt64(fieldName, (Long) value);
                } else {
                    builder.setInt64(fieldName, (Long) value);
                }
            }
            case FLOAT32 -> {
                if (nullable) {
                    builder.setNullableFloat32(fieldName, (Float) value);
                } else {
                    builder.setFloat32(fieldName, (Float) value);
                }
            }
            case FLOAT64 -> {
                if (nullable) {
                    builder.setNullableFloat64(fieldName, (Double) value);
                } else {
                    builder.setFloat64(fieldName, (Double) value);
                }
            }
            case DECIMAL -> builder.setDecimal(fieldName, (BigDecimal) value);
            case STRING -> builder.setString(fieldName, (String) value);
            case DATE -> builder.setDate(fieldName, (LocalDate) value);
            case TIMESTAMP_WITH_TIMEZONE -> builder.setTimestampWithTimezone(fieldName, (OffsetDateTime) value);
            default -> throw new IllegalStateException("Unexpected kind " + kind);
        }
    }

    /**
     * Returns the value of a field of a compact or portable record, whatever its kind, or fails
     * if the record has no such field.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public static Object getField(GenericRecord record, String fieldName) {
        FieldKind fieldKind = record.getFieldKind(fieldName);
        return switch (fieldKind) {
            case BOOLEAN -> record.getBoolean(fieldName);
            case INT8 -> record.getInt8(fieldName);
            case INT16 -> record.getInt16(fieldName);
            case INT32 -> record.getInt32(fieldName);
            case INT64 -> record.getInt64(fieldName);
            case FLOAT32 -> record.getFloat32(fieldName);
            case FLOAT64 -> record.getFloat64(fieldName);
            case CHAR -> String.valueOf(record.getChar(fieldName));
            case NULLABLE_BOOLEAN -> record.getNullableBoolean(fieldName);
            case NULLABLE_INT8 -> record.getNullableInt8(fieldName);
            case NULLABLE_INT16 -> record.getNullableInt16(fieldName);
            case NULLABLE_INT32 -> record.getNullableInt32(fieldName);
            case NULLABLE_INT64 -> record.getNullableInt64(fieldName);
            case NULLABLE_FLOAT32 -> record.getNullableFloat32(fieldName);
            case NULLABLE_FLOAT64 -> record.getNullableFloat64(fieldName);
            case STRING -> record.getString(fieldName);
            case DECIMAL -> record.getDecimal(fieldName);
            case DATE -> record.getDate(fieldName);
            case TIMESTAMP -> record.getTimestamp(fieldName);
            case TIMESTAMP_WITH_TIMEZONE -> record.getTimestampWithTimezone(fieldName);
            case NOT_AVAILABLE -> throw new IllegalArgumentException("The record has no field '" + fieldName + "'");
            default -> throw new IllegalArgumentException("The field '" + fieldName + "' is of kind " + fieldKind
                    + ", which cannot be stored in ClickHouse");
        };
    }
}
