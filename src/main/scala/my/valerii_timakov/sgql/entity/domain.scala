package my.valerii_timakov.sgql.entity


import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.exceptions.TypeReinitializationException

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID


case class Entity(id: EntityId, value: EntityFieldType)


//sealed trait EntityTypeHolder
sealed trait AbstractEntityType: //extends EntityTypeHolder:
    def valueType: EntityFieldTypeDefinition     
sealed trait AbstractNamedEntityType extends AbstractEntityType:
    def name: String
case class EntityType(
    name: String,
    idType: EntityIdTypeDefinition,
    valueType: EntityFieldTypeDefinition,
) extends AbstractNamedEntityType
trait NamedEntitySuperType extends AbstractNamedEntityType:
    def name: String
    def valueType: EntityFieldTypeDefinition
case class PrimitiveEntitySuperType(
    name: String,
    valueType: PrimitiveFieldTypeDefinition,
) extends NamedEntitySuperType
case class ArrayEntitySuperType(
    name: String,
    valueType: ArrayTypeDefinition,
) extends NamedEntitySuperType
case class ObjectEntitySuperType(
    name: String,
    valueType: ObjectTypeDefinition,
) extends NamedEntitySuperType
case class SimpleEntityType(valueType: EntityFieldTypeDefinition) extends AbstractEntityType
sealed trait EntityId:
    def serialize: String
final case class IntId(value: Int) extends EntityId:
    override def serialize: String = value.toString
final case class LongId(value: Long) extends EntityId:
    override def serialize: String = value.toString
final case class StringId(value: String) extends EntityId:
    override def serialize: String = value
final case class UUIDId(value: java.util.UUID) extends EntityId:
    override def serialize: String = value.toString

sealed trait EntityIdTypeDefinition(val name: String):
    def parse(value: String): Either[IdParseError, EntityId] = {
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    }
    protected def parseIner(value: String): EntityId
    
case object IntIdTypeDefinition extends EntityIdTypeDefinition("Integer"):
    protected override def parseIner(value: String): EntityId = IntId(value.toInt)
case object LongIdTypeDefinition extends EntityIdTypeDefinition("Long"):
    protected override def parseIner(value: String): EntityId = LongId(value.toLong)
case object StringIdTypeDefinition extends EntityIdTypeDefinition("String"):
    protected override def parseIner(value: String): EntityId = StringId(value)
case object UUIDIdTypeDefinition extends EntityIdTypeDefinition("UUID"):
    protected override def parseIner(value: String): EntityId = UUIDId(java.util.UUID.fromString(value))


val idTypesMap = Map(
    IntIdTypeDefinition.name -> IntIdTypeDefinition,
    LongIdTypeDefinition.name -> LongIdTypeDefinition,
    StringIdTypeDefinition.name -> StringIdTypeDefinition,
    UUIDIdTypeDefinition.name -> UUIDIdTypeDefinition,
)

sealed abstract class EntityFieldType
sealed abstract class PrimitiveFieldType extends EntityFieldType
final case class StringType(value: String) extends PrimitiveFieldType
final case class IntType(value: Int) extends PrimitiveFieldType
final case class LongType(value: Long) extends PrimitiveFieldType
final case class DoubleType(value: Double) extends PrimitiveFieldType
final case class FloatType(value: Float) extends PrimitiveFieldType
final case class BooleanType(value: Boolean) extends PrimitiveFieldType
final case class DateType(value: LocalDate) extends PrimitiveFieldType
final case class DateTimeType(value: LocalTime) extends PrimitiveFieldType
final case class TimeType(value: LocalDateTime) extends PrimitiveFieldType
final case class UUIDType(value: UUID) extends PrimitiveFieldType
final case class BinaryType(value: Array[Byte]) extends PrimitiveFieldType
final case class ArrayType[T <: EntityFieldType](value: List[T]) extends EntityFieldType
final case class ObjectType(value: Map[String, EntityFieldType]) extends EntityFieldType

val primitiveFieldTypesMap = Map(
    StringTypeDefinition.name -> StringTypeDefinition,
    LongIdTypeDefinition.name -> LongTypeDefinition,
    IntTypeDefinition.name -> IntTypeDefinition,
    DoubleTypeDefinition.name -> DoubleTypeDefinition,
    FloatTypeDefinition.name -> FloatTypeDefinition,
    BooleanTypeDefinition.name -> BooleanTypeDefinition,
    DateTypeDefinition.name -> DateTypeDefinition,
    DateTimeTypeDefinition.name -> DateTimeTypeDefinition,
    TimeTypeDefinition.name -> TimeTypeDefinition,
    UUIDTypeDefinition.name -> UUIDTypeDefinition,
    BinaryTypeDefinition.name -> BinaryTypeDefinition
)

sealed abstract class EntityFieldTypeDefinition
sealed abstract class PrimitiveFieldTypeDefinition extends EntityFieldTypeDefinition:
    def parse(value: String): Either[IdParseError, EntityFieldType]
final case class CustomPrimitiveTypeDefinition(parent: PrimitiveEntitySuperType) 
extends PrimitiveFieldTypeDefinition:
    def parse(value: String): Either[IdParseError, EntityFieldType] = parent.valueType.parse(value)
sealed abstract case class RootPrimitiveTypeDefinition(name: String) extends PrimitiveFieldTypeDefinition:
    def parse(value: String): Either[IdParseError, EntityFieldType] = {
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    }
    protected def parseIner(value: String): EntityFieldType
object StringTypeDefinition extends RootPrimitiveTypeDefinition("String"):
    protected override def parseIner(value: String): StringType = StringType(value)
object IntTypeDefinition extends RootPrimitiveTypeDefinition("Integer"):
    protected override def parseIner(value: String): IntType = IntType(value.toInt)
object LongTypeDefinition extends RootPrimitiveTypeDefinition("Long"):
    protected override def parseIner(value: String): LongType = LongType(value.toLong)
object DoubleTypeDefinition extends RootPrimitiveTypeDefinition("Double"):
    protected override def parseIner(value: String): DoubleType = DoubleType(value.toDouble)
object FloatTypeDefinition extends RootPrimitiveTypeDefinition("Float"):
    protected override def parseIner(value: String): FloatType = FloatType(value.toFloat)
object BooleanTypeDefinition extends RootPrimitiveTypeDefinition("Boolean"):
    protected override def parseIner(value: String): BooleanType = BooleanType(value.toBoolean)
object DateTypeDefinition extends RootPrimitiveTypeDefinition("Date"):
    protected override def parseIner(value: String): DateType = DateType(LocalDate.parse(value))
object DateTimeTypeDefinition extends RootPrimitiveTypeDefinition("DateTime"):
    protected override def parseIner(value: String): DateTimeType = DateTimeType(LocalTime.parse(value))
object TimeTypeDefinition extends RootPrimitiveTypeDefinition("Time"):
    protected override def parseIner(value: String): TimeType = TimeType(LocalDateTime.parse(value))
object UUIDTypeDefinition extends RootPrimitiveTypeDefinition("UUID"):
    protected override def parseIner(value: String): UUIDType = UUIDType(java.util.UUID.fromString(value))
object BinaryTypeDefinition extends RootPrimitiveTypeDefinition("Binary"):
    protected override def parseIner(value: String): BinaryType = BinaryType(Base64.rfc2045().decode(value))
object ArrayTypeDefinition {
    val name = "Array"
}
    
final case class ArrayTypeDefinition(private var _elementTypes: Set[AbstractEntityType], parent: Option[ArrayEntitySuperType]) 
extends EntityFieldTypeDefinition:
    private var initiated = false
    def elementTypes: Set[AbstractEntityType] = _elementTypes
    def setChildren(elementTypesValues: Set[AbstractEntityType]): Unit =
        if (initiated) throw new TypeReinitializationException
        _elementTypes = elementTypesValues
        initiated = true
    lazy val allElementTypes: Set[AbstractEntityType] =
        _elementTypes ++ parent.map(_.valueType.allElementTypes).getOrElse(Set.empty[AbstractEntityType])
object ObjectTypeDefinition {
    val name = "Object"
}
final case class ObjectTypeDefinition(
    private var _fields: Map[String, AbstractEntityType],
    parent: Option[ObjectEntitySuperType]
) extends EntityFieldTypeDefinition:
    private var initiated = false
    def fields: Map[String, AbstractEntityType] = _fields
    def setChildren(fieldsValues: Map[String, AbstractEntityType]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, AbstractEntityType] = 
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, AbstractEntityType])

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
