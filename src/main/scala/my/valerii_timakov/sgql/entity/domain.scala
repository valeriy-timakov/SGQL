package my.valerii_timakov.sgql.entity


import java.time.{LocalDate, LocalDateTime, LocalTime}

sealed trait EntityId {
    def serialize: String
}
final case class IntId(value: Int) extends EntityId {
    override def serialize: String = value.toString
}
final case class LongId(value: Long) extends EntityId {
    override def serialize: String = value.toString
}
final case class StringId(value: String) extends EntityId {
    override def serialize: String = value
}
final case class UUIDId(value: java.util.UUID) extends EntityId {
    override def serialize: String = value.toString
}

sealed trait EntityIdTypeDefinition {
    def parse(value: String): Either[IdParseError, EntityId] = {
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    }
    protected def parseIner(value: String): EntityId
}
case object IntIdTypeDefinition extends EntityIdTypeDefinition {
    protected override def parseIner(value: String): EntityId = IntId(value.toInt)
}
case object LongIdTypeDefinition extends EntityIdTypeDefinition {
    protected override def parseIner(value: String): EntityId = LongId(value.toLong)
}
case object StringIdTypeDefinition extends EntityIdTypeDefinition {
    protected override def parseIner(value: String): EntityId = StringId(value)
}
case object UUIDIdTypeDefinition extends EntityIdTypeDefinition {
    protected override def parseIner(value: String): EntityId = UUIDId(java.util.UUID.fromString(value))
}

case class EntityType(name: String, idType: EntityIdTypeDefinition, valueType: EntityFieldTypeDefinition)
case class Entity(id: EntityId, value: EntityFieldType)

sealed trait GetFieldsDescriptor
case class ObjectGetFieldsDescriptor(fields: Map[String, GetFieldsDescriptor])
case class ListGetFieldsDescriptor(fields: GetFieldsDescriptor, limit: Int, offset: Int)
case object AllGetFieldsDescriptor extends GetFieldsDescriptor

sealed trait SearchCondition
final case class EqSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class NeSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class GtSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class GeSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class LtSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class LeSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class BetweenSearchCondition(fieldName: String, from: EntityFieldType, to: EntityFieldType) extends SearchCondition
final case class LikeSearchCondition(fieldName: String, value: String) extends SearchCondition
final case class InSearchCondition[T <: EntityFieldType](fieldName: String, value: List[T]) extends SearchCondition
final case class AndSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class OrSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition

sealed trait EntityFieldType
sealed class PrimitiveFieldType extends EntityFieldType
final case class StringType(value: String) extends PrimitiveFieldType
final case class IntType(value: Int) extends PrimitiveFieldType
final case class LongType(value: Long) extends PrimitiveFieldType
final case class DoubleType(value: Double) extends PrimitiveFieldType
final case class FloatType(value: Float) extends PrimitiveFieldType
final case class BooleanType(value: Boolean) extends PrimitiveFieldType
final case class DateType(value: LocalDate) extends PrimitiveFieldType
final case class DateTimeType(value: LocalTime) extends PrimitiveFieldType
final case class TimeType(value: LocalDateTime) extends PrimitiveFieldType
final case class BinaryType(value: Array[Byte]) extends PrimitiveFieldType
final case class ArrayType[T <: EntityFieldType](value: List[T]) extends EntityFieldType
final case class ObjectType(value: Map[String, EntityFieldType]) extends EntityFieldType

sealed trait EntityFieldTypeDefinition
sealed class PrimitiveFieldTypeDefinition extends EntityFieldTypeDefinition
case object StringTypeDefinition extends PrimitiveFieldTypeDefinition
case object IntTypeDefinition extends PrimitiveFieldTypeDefinition
case object LongTypeDefinition extends PrimitiveFieldTypeDefinition
case object DoubleTypeDefinition extends PrimitiveFieldTypeDefinition
case object FloatTypeDefinition extends PrimitiveFieldTypeDefinition
case object BooleanTypeDefinition extends PrimitiveFieldTypeDefinition
case object DateTypeDefinition extends PrimitiveFieldTypeDefinition
case object DateTimeTypeDefinition extends PrimitiveFieldTypeDefinition
case object TimeTypeDefinition extends PrimitiveFieldTypeDefinition
case object BinaryTypeDefinition extends PrimitiveFieldTypeDefinition
final case class ArrayTypeDefinition(elementType: EntityFieldTypeDefinition) extends EntityFieldTypeDefinition
final case class ObjectTypeDefinition(value: Map[String, EntityFieldTypeDefinition]) extends EntityFieldTypeDefinition
