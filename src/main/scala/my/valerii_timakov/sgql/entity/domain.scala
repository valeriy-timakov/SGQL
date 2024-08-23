package my.valerii_timakov.sgql.entity


import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.exceptions.TypeReinitializationException

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec
import scala.util.Try


case class Entity(id: EntityId, value: EntityFieldType)


sealed trait AbstractEntityType: 
    def valueType: EntityTypeDefinition     
    
sealed trait AbstractNamedEntityType extends AbstractEntityType:
    def name: String
    
case class EntityType(
    name: String,
    valueType: EntityTypeDefinition,
) extends AbstractNamedEntityType

trait NamedEntitySuperType extends AbstractNamedEntityType:
    def name: String
    def valueType: EntityTypeDefinition

case class PrimitiveEntitySuperType[+TD <: CustomPrimitiveTypeDefinition](
    name: String,
    valueType: TD,
) extends NamedEntitySuperType

case class ArrayEntitySuperType(
    name: String,
    valueType: ArrayTypeDefinition,
) extends NamedEntitySuperType

case class ObjectEntitySuperType(
    name: String,
    valueType: ObjectTypeDefinition,
) extends NamedEntitySuperType

type FieldTypeDefinitions = TypeReferenceDefinition | TypeBackReferenceDefinition | RootPrimitiveTypeDefinition | SimpleObjectTypeDefinition
type ArrayItemTypeDefinitions = TypeReferenceDefinition | RootPrimitiveTypeDefinition

//trait ItemEntityType extends AbstractEntityType//:
    //def valueType: ArrayItemTypeDefinitions
case class FieldType(valueType: FieldTypeDefinitions) //extends AbstractEntityType
case class ArrayItemType(valueType: ArrayItemTypeDefinitions)// extends AbstractEntityType

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

sealed trait EntityTypeDefinition:
    def idType: EntityIdTypeDefinition
    def parent: Option[NamedEntitySuperType]

sealed abstract class AbstractTypeDefinition
    
sealed case class TypeReferenceDefinition(
    referencedType: AbstractNamedEntityType, 
) extends AbstractTypeDefinition, EntityTypeDefinition:
    lazy val idType: EntityIdTypeDefinition = referencedType.valueType.idType
    lazy val parent: Option[NamedEntitySuperType] = None
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}])"

sealed case class TypeBackReferenceDefinition(
    referencedType: AbstractNamedEntityType,
    refField: String
) extends AbstractTypeDefinition, EntityTypeDefinition:
    lazy val idType: EntityIdTypeDefinition = referencedType.valueType.idType
    lazy val parent: Option[NamedEntitySuperType] = None
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}], $refField)"

sealed abstract class PrimitiveFieldTypeDefinition extends AbstractTypeDefinition:
    def parse(value: String): Either[IdParseError, EntityFieldType]
    def rootType: RootPrimitiveTypeDefinition
        
final case class CustomPrimitiveTypeDefinition(parentNode: 
   Either[(EntityIdTypeDefinition, RootPrimitiveTypeDefinition), PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]]
) extends PrimitiveFieldTypeDefinition, EntityTypeDefinition:
    @tailrec def rootType: RootPrimitiveTypeDefinition = this.parentNode match
        case Left((_, root)) => root
        case Right(parent) => parent.valueType.rootType
    def parse(value: String): Either[IdParseError, EntityFieldType] = rootType.parse(value)
    lazy val idType: EntityIdTypeDefinition = parentNode.fold(_._1, _.valueType.idType)
    lazy val parent: Option[PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]] = parentNode.toOption

sealed abstract case class RootPrimitiveTypeDefinition(name: String) extends PrimitiveFieldTypeDefinition:
    def rootType: RootPrimitiveTypeDefinition = this
    def parse(value: String): Either[IdParseError, EntityFieldType] =
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
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
    
object ArrayTypeDefinition:
    val name = "Array"

    
final case class ArrayTypeDefinition(private var _elementTypes: Set[ArrayItemType], idOrParent: Either[EntityIdTypeDefinition, ArrayEntitySuperType])
extends AbstractTypeDefinition, EntityTypeDefinition:
    private var initiated = false
    def elementTypes: Set[ArrayItemType] = _elementTypes
    def setChildren(elementTypesValues: Set[ArrayItemType]): Unit =
        if (initiated) throw new TypeReinitializationException
        _elementTypes = elementTypesValues
        initiated = true
    lazy val allElementTypes: Set[ArrayItemType] =
        _elementTypes ++ parent.map(_.valueType.allElementTypes).getOrElse(Set.empty[ArrayItemType])
    lazy val idType: EntityIdTypeDefinition = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ArrayEntitySuperType] = idOrParent.toOption
        
object ObjectTypeDefinition:
    val name = "Object"


final case class ObjectTypeDefinition(
    private var _fields: Map[String, FieldType],
    idOrParent: Either[EntityIdTypeDefinition, ObjectEntitySuperType]
) extends AbstractTypeDefinition, EntityTypeDefinition:
    private var initiated = false
    def fields: Map[String, FieldType] = _fields
    def setChildren(fieldsValues: Map[String, FieldType]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldType] = 
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldType])
    lazy val idType: EntityIdTypeDefinition = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ObjectEntitySuperType] = idOrParent.toOption

final case class SimpleObjectTypeDefinition(
    private var _fields: Map[String, FieldType],
    parent: Option[ObjectEntitySuperType]
) extends AbstractTypeDefinition:
    private var initiated = false
    def fields: Map[String, FieldType] = _fields
    def setChildren(fieldsValues: Map[String, FieldType]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldType] =
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldType])

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
