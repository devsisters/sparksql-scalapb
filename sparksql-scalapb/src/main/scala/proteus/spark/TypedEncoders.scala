package proteus.spark

import com.google.protobuf.ByteString
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNull, Literal}
import org.apache.spark.sql.types._
import proteus.*

import scala.reflect.ClassTag

trait TypedEncoders extends FromCatalystHelpers with ToCatalystHelpers with Serializable {
  class MessageTypedEncoder[T](implicit codec: ProtobufCodec.Message[T], ct: ClassTag[T])
      extends TypedEncoder[T] {
    override def nullable: Boolean = false

    override def jvmRepr: DataType = ObjectType(ct.runtimeClass)

    override def catalystRepr: DataType = protoSql.schemaFor(codec)

    def fromCatalyst(path: Expression): Expression = {
      val expr = pmessageFromCatalyst(cmp, path)

      val reads = Invoke(
        Literal.fromObject(cmp),
        "messageReads",
        ObjectType(classOf[Reads[?]]),
        Nil
      )

      val read = Invoke(reads, "read", ObjectType(classOf[Function[?, ?]]))

      val ret = Invoke(read, "apply", ObjectType(ct.runtimeClass), expr :: Nil)
      ret
    }

    override def toCatalyst(path: Expression): Expression = {
      val ret = messageToCatalyst(codec, path)
      ret
    }
  }

  class EnumTypedEncoder[T](implicit codec: ProtobufCodec.Enum[T], ct: ClassTag[T])
      extends TypedEncoder[T] {
    override def nullable: Boolean = false

    override def jvmRepr: DataType = ObjectType(ct.runtimeClass)

    override def catalystRepr: DataType = StringType

    override def fromCatalyst(path: Expression): Expression = {
      val expr = Invoke(
        Literal.fromObject(cmp),
        "fromValue",
        ObjectType(ct.runtimeClass),
        StaticInvoke(
          JavaHelpers.getClass,
          IntegerType,
          "enumValueFromString",
          Literal.fromObject(cmp) :: path :: Nil
        ) :: Nil
      )
      If(IsNull(path), Literal.create(null, expr.dataType), expr)
    }

    override def toCatalyst(path: Expression): Expression =
      StaticInvoke(
        JavaHelpers.getClass,
        StringType,
        "enumToString",
        Literal.fromObject(cmp) :: path :: Nil
      )
  }

  object ByteStringTypedEncoder extends TypedEncoder[ByteString] {
    override def nullable: Boolean = false

    override def jvmRepr: DataType = ObjectType(classOf[ByteString])

    override def catalystRepr: DataType = BinaryType

    override def fromCatalyst(path: Expression): Expression =
      StaticInvoke(
        classOf[ByteString],
        ObjectType(classOf[ByteString]),
        "copyFrom",
        path :: Nil
      )

    override def toCatalyst(path: Expression): Expression =
      Invoke(path, "toByteArray", BinaryType, Seq.empty)
  }
}

trait Implicits {
  private[proteus] val typedEncoders: TypedEncoders

  implicit def messageTypedEncoder[
      T: ProtobufCodec.Message: ClassTag
  ]: TypedEncoder[T] = new typedEncoders.MessageTypedEncoder[T]

  implicit def enumTypedEncoder[T](implicit
      codec: ProtobufCodec.Enum[T],
      ct: ClassTag[T]
  ): TypedEncoder[T] = new typedEncoders.EnumTypedEncoder[T]

  implicit def byteStringTypedEncoder: TypedEncoder[ByteString] =
    typedEncoders.ByteStringTypedEncoder

  implicit def typedEncoderToEncoder[T: ClassTag](implicit
      ev: TypedEncoder[T]
  ): Encoder[T] =
    TypedExpressionEncoder(using ev)
}

object Implicits extends Implicits {
  private[proteus] val typedEncoders: TypedEncoders = new TypedEncoders {
    @transient
    lazy val protoSql = ProtoSQL
  }
}
