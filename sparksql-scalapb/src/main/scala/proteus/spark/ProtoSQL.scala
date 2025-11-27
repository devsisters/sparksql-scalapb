package proteus.spark

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ExternalRDD
import org.apache.spark.sql.types.{DataTypes => _, _}
import org.apache.spark.sql._
import proteus.*
import scala.reflect.ClassTag
import proteus.ProtobufCodec.*
import proteus.ProtobufCodec.MessageField.SimpleField
import zio.blocks.schema.PrimitiveType

trait ProtoSQL {
  self =>
  import scala.language.existentials

  def protoToDataFrame[T: ProtobufCodec: Encoder](
      sparkSession: SparkSession,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    val logicalPlan: LogicalPlan = ExternalRDD(protoRdd, sparkSession)
    FramelessInternals.ofRows(sparkSession, logicalPlan)
  }

  def protoToDataFrame[T: ProtobufCodec: Encoder](
      sqlContext: SQLContext,
      protoRdd: org.apache.spark.rdd.RDD[T]
  ): DataFrame = {
    protoToDataFrame(sqlContext.sparkSession, protoRdd)
  }

  def schemaFor(codec: ProtobufCodec.Message[?]): DataType =
    StructType(codec.simpleFields.map(structFieldFor))

  def singularDataType(codec: ProtobufCodec[?]): DataType =
    codec match {
      case Primitive(primitiveType) =>
        primitiveType match {
          case _: PrimitiveType.Double  => DoubleType
          case _: PrimitiveType.Boolean => BooleanType
          case _: PrimitiveType.Int     => IntegerType
          case _: PrimitiveType.Long    => LongType
          case _: PrimitiveType.String  => StringType
          case _: PrimitiveType.Float   => FloatType
        }
      case Bytes                  => BinaryType
      case msg: Message[_]        => schemaFor(msg)
      case _: Enum[_]             => StringType
      case Optional(codec)        => singularDataType(codec)
      case Transform(_, _, codec) => singularDataType(codec)
    }

  def dataTypeFor(codec: ProtobufCodec[?]): DataType =
    codec match {
      case RepeatedMap(element, _, _) =>
        element match {
          case msg: Message[_] =>
            MapType(
              singularDataType(msg.simpleFields(0).codec),
              singularDataType(msg.simpleFields(1).codec)
            )
          case _ =>
            throw new RuntimeException(
              "Unexpected: field marked as map, but does not have an entry message associated"
            )
        }
      case Repeated(element, _, _, _) => ArrayType(singularDataType(element), containsNull = false)
      case Optional(codec)            => singularDataType(codec)
      case Transform(_, _, codec)     => singularDataType(codec)
      case _                          => singularDataType(codec)
    }

  def structFieldFor(fd: SimpleField[?]): StructField = {
    StructField(
      fd.name,
      dataTypeFor(fd.codec),
      nullable = isOptional(fd.codec)
    )
  }

  private def isOptional(codec: ProtobufCodec[?]): Boolean =
    codec match {
      case c: Transform[_, _] => isOptional(c.codec)
      case _: Optional[_]     => true
      case _                  => false
    }

  def columnName(field: SimpleField[?]): String =
    field.name

  val implicits: Implicits = new Implicits {
    val typedEncoders = new TypedEncoders {
      val protoSql = self
    }
  }
}

object ProtoSQL extends ProtoSQL
