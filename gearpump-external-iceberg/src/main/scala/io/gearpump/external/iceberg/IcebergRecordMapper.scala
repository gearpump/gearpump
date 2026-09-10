/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.external.iceberg

import io.gearpump.Message
import java.nio.ByteBuffer
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import java.util
import java.util.UUID
import org.apache.iceberg.Schema
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.types.{Type, Types}
import scala.jdk.CollectionConverters._

/** Converts a Gearpump message into an Iceberg generic record. */
trait IcebergRecordMapper extends Serializable {
  def map(message: Message, schema: Schema): Record
}

object IcebergRecordMapper {

  /** Accepts only messages whose value is already an Iceberg `Record`. */
  val recordOnly: IcebergRecordMapper = new IcebergRecordMapper {
    override def map(message: Message, schema: Schema): Record = message.value match {
      case record: Record => record
      case other =>
        throw new IllegalArgumentException(
          s"IcebergSink expects org.apache.iceberg.data.Record values, but got $other")
    }
  }

  /** Maps Scala or Java maps to table columns by field name. */
  val fieldNames: IcebergRecordMapper = new IcebergRecordMapper {
    override def map(message: Message, schema: Schema): Record = {
      val values = message.value match {
        case map: scala.collection.Map[_, _] =>
          map.iterator.map { case (key, value) => key.toString -> value }.toMap
        case map: util.Map[_, _] =>
          map.asScala.iterator.map { case (key, value) => key.toString -> value }.toMap
        case other =>
          throw new IllegalArgumentException(
            s"Field-name mapping expects a Scala or Java map, but got $other")
      }

      val record = GenericRecord.create(schema)
      schema.columns().asScala.foreach { field =>
        values.get(field.name()) match {
          case Some(value) => record.setField(field.name(), convert(value, field.`type`()))
          case None if field.isRequired =>
            throw new IllegalArgumentException(
              s"Required Iceberg field '${field.name()}' is missing")
          case None => record.setField(field.name(), null)
        }
      }
      record
    }
  }

  private def convert(value: Any, icebergType: Type): Any = {
    if (value == null) {
      null
    } else {
      icebergType.typeId() match {
        case Type.TypeID.BOOLEAN if value.isInstanceOf[String] =>
          Boolean.box(value.toString.toBoolean)
        case Type.TypeID.INTEGER if value.isInstanceOf[Number] =>
          Int.box(toIntExact(value.asInstanceOf[Number], icebergType))
        case Type.TypeID.LONG if value.isInstanceOf[Number] =>
          Long.box(toLongExact(value.asInstanceOf[Number], icebergType))
        case Type.TypeID.FLOAT if value.isInstanceOf[Number] =>
          Float.box(value.asInstanceOf[Number].floatValue())
        case Type.TypeID.DOUBLE if value.isInstanceOf[Number] =>
          Double.box(value.asInstanceOf[Number].doubleValue())
        case Type.TypeID.STRING => value.toString
        case Type.TypeID.UUID if value.isInstanceOf[String] => UUID.fromString(value.toString)
        case Type.TypeID.DECIMAL if value.isInstanceOf[BigDecimal] =>
          value.asInstanceOf[BigDecimal].bigDecimal
        case Type.TypeID.FIXED | Type.TypeID.BINARY if value.isInstanceOf[Array[Byte]] =>
          ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
        case Type.TypeID.TIMESTAMP | Type.TypeID.TIMESTAMP_NANO =>
          convertTimestamp(value, adjustsToUtc(icebergType))
        case _ => value
      }
    }
  }

  private def adjustsToUtc(icebergType: Type): Boolean = icebergType match {
    case timestamp: Types.TimestampType => timestamp.shouldAdjustToUTC()
    case timestamp: Types.TimestampNanoType => timestamp.shouldAdjustToUTC()
    case _ => false
  }

  private def toBigDecimal(number: Number, icebergType: Type): java.math.BigDecimal = {
    try {
      number match {
        case decimal: java.math.BigDecimal => decimal
        case integer: java.math.BigInteger => new java.math.BigDecimal(integer)
        case decimal: BigDecimal => decimal.bigDecimal
        case integer: BigInt => new java.math.BigDecimal(integer.bigInteger)
        case _: java.lang.Byte | _: java.lang.Short | _: java.lang.Integer | _: java.lang.Long =>
          java.math.BigDecimal.valueOf(number.longValue())
        case _ => java.math.BigDecimal.valueOf(number.doubleValue())
      }
    } catch {
      case failure: ArithmeticException =>
        throw invalidNumericValue(number, icebergType, failure)
      case failure: NumberFormatException =>
        throw invalidNumericValue(number, icebergType, failure)
    }
  }

  private def toIntExact(number: Number, icebergType: Type): Int = {
    try {
      toBigDecimal(number, icebergType).intValueExact()
    } catch {
      case failure: ArithmeticException =>
        throw invalidNumericValue(number, icebergType, failure)
    }
  }

  private def toLongExact(number: Number, icebergType: Type): Long = {
    try {
      toBigDecimal(number, icebergType).longValueExact()
    } catch {
      case failure: ArithmeticException =>
        throw invalidNumericValue(number, icebergType, failure)
    }
  }

  private def invalidNumericValue(
      number: Number,
      icebergType: Type,
      cause: Exception): IllegalArgumentException = {
    new IllegalArgumentException(
      s"Numeric value '$number' cannot be represented as Iceberg $icebergType", cause)
  }

  private def convertTimestamp(value: Any, adjustToUtc: Boolean): Any = value match {
    case instant: Instant if adjustToUtc => OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)
    case instant: Instant => LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
    case timestamp: OffsetDateTime if !adjustToUtc => timestamp.toLocalDateTime
    case timestamp: LocalDateTime if adjustToUtc => timestamp.atOffset(ZoneOffset.UTC)
    case timestamp: java.sql.Timestamp if adjustToUtc =>
      OffsetDateTime.ofInstant(timestamp.toInstant, ZoneOffset.UTC)
    case timestamp: java.sql.Timestamp => timestamp.toLocalDateTime
    case other => other
  }
}
