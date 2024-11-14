package my.valerii_timakov.sgql.entity


import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, TypeReinitializationException}

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec


case class Entity(id: EntityId, value: EntityFieldType)


sealed trait AbstractEntityType: 
    def valueType: EntityTypeDefinition     
    
sealed trait AbstractNamedEntityType extends AbstractEntityType:
    def name: String
    
case class EntityType[D <: EntityTypeDefinition](
    name: String,
    valueType: D,
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

sealed trait EntityId:
    def serialize: String
    def typeDefinition: EntityIdTypeDefinition
final case class ByteId(value: Byte) extends EntityId:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition = ByteIdTypeDefinition
final case class ShortIntId(value: Short) extends EntityId:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition = ShortIdTypeDefinition
final case class IntId(value: Int) extends EntityId:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition = IntIdTypeDefinition
final case class LongId(value: Long) extends EntityId:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition = LongIdTypeDefinition
final case class StringId(value: String) extends EntityId:
    override def serialize: String = value
    override val typeDefinition: EntityIdTypeDefinition = StringIdTypeDefinition
final case class FixedStringId(value: String, typeRef: FixedStringIdTypeDefinition) extends EntityId:
    if value == null || typeRef == null || value.length != typeRef.length then throw new IllegalArgumentException(
        s"FixedStringId value must be of length ${typeRef.length}! Got: $value")
    override def serialize: String = value
    override val typeDefinition: EntityIdTypeDefinition = FixedStringIdTypeDefinition
final case class UUIDId(value: java.util.UUID) extends EntityId:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition = UUIDIdTypeDefinition

sealed abstract class EntityIdTypeDefinition(val name: String):
    def parse(value: String): Either[IdParseError, EntityId] = {
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    }
    protected def parseIner(value: String): EntityId

case object ByteIdTypeDefinition extends EntityIdTypeDefinition("Byte"):
    protected override def parseIner(value: String): EntityId = ByteId(value.toByte)
case object ShortIdTypeDefinition extends EntityIdTypeDefinition("Short"):
    protected override def parseIner(value: String): EntityId = ShortIntId(value.toShort)
case object IntIdTypeDefinition extends EntityIdTypeDefinition("Integer"):
    protected override def parseIner(value: String): EntityId = IntId(value.toInt)
case object LongIdTypeDefinition extends EntityIdTypeDefinition("Long"):
    protected override def parseIner(value: String): EntityId = LongId(value.toLong)
case object UUIDIdTypeDefinition extends EntityIdTypeDefinition("UUID"):
    protected override def parseIner(value: String): EntityId = UUIDId(java.util.UUID.fromString(value))
case object StringIdTypeDefinition extends EntityIdTypeDefinition("String"):
    protected override def parseIner(value: String): EntityId = StringId(value)
case class FixedStringIdTypeDefinition(length: Int) extends EntityIdTypeDefinition("String"):
    protected override def parseIner(value: String): EntityId = StringId(value)
case object FixedStringIdTypeDefinition extends EntityIdTypeDefinition("String"):
    protected override def parseIner(value: String): EntityId = StringId(value)


val idTypesMap = Map(
    ByteIdTypeDefinition.name -> ByteIdTypeDefinition,
    ShortIdTypeDefinition.name -> ShortIdTypeDefinition,
    IntIdTypeDefinition.name -> IntIdTypeDefinition,
    LongIdTypeDefinition.name -> LongIdTypeDefinition,
    UUIDIdTypeDefinition.name -> UUIDIdTypeDefinition,
    StringIdTypeDefinition.name -> StringIdTypeDefinition,
    FixedStringIdTypeDefinition.name -> FixedStringIdTypeDefinition,
)

sealed abstract class EntityFieldType:
    def typeDefinition: AbstractTypeDefinition
sealed abstract class PrimitiveFieldType[T] extends EntityFieldType:
    def value: T
sealed abstract class RootPrimitiveFieldType[T] extends PrimitiveFieldType[T]:
    def typeDefinition: RootPrimitiveTypeDefinition
final case class StringType(value: String) extends RootPrimitiveFieldType[String]:
    def typeDefinition: RootPrimitiveTypeDefinition = StringTypeDefinition
final case class ByteType(value: Byte) extends RootPrimitiveFieldType[Byte]:
    def typeDefinition: RootPrimitiveTypeDefinition = ByteTypeDefinition
final case class ShortIntType(value: Short) extends RootPrimitiveFieldType[Short]:
    def typeDefinition: RootPrimitiveTypeDefinition = ShortIntTypeDefinition
final case class IntType(value: Int) extends RootPrimitiveFieldType[Int]:
    def typeDefinition: RootPrimitiveTypeDefinition = IntTypeDefinition
final case class LongType(value: Long) extends RootPrimitiveFieldType[Long]:
    def typeDefinition: RootPrimitiveTypeDefinition = LongTypeDefinition
final case class DoubleType(value: Double) extends RootPrimitiveFieldType[Double]:
    def typeDefinition: RootPrimitiveTypeDefinition = DoubleTypeDefinition
final case class FloatType(value: Float) extends RootPrimitiveFieldType[Float]:
    def typeDefinition: RootPrimitiveTypeDefinition = FloatTypeDefinition
final case class BooleanType(value: Boolean) extends RootPrimitiveFieldType[Boolean]:
    def typeDefinition: RootPrimitiveTypeDefinition = BooleanTypeDefinition
final case class DateType(value: LocalDate) extends RootPrimitiveFieldType[LocalDate]:
    def typeDefinition: RootPrimitiveTypeDefinition = DateTypeDefinition
final case class DateTimeType(value: LocalTime) extends RootPrimitiveFieldType[LocalTime]:
    def typeDefinition: RootPrimitiveTypeDefinition = DateTimeTypeDefinition
final case class TimeType(value: LocalDateTime) extends RootPrimitiveFieldType[LocalDateTime]:
    def typeDefinition: RootPrimitiveTypeDefinition = TimeTypeDefinition
final case class UUIDType(value: UUID) extends RootPrimitiveFieldType[UUID]:
    def typeDefinition: RootPrimitiveTypeDefinition = UUIDTypeDefinition
final case class DecimalType(value: BigDecimal) extends RootPrimitiveFieldType[BigDecimal]:
    def typeDefinition: RootPrimitiveTypeDefinition = DecimalTypeDefinition
final case class BinaryType(value: Array[Byte]) extends RootPrimitiveFieldType[Array[Byte]]:
    def typeDefinition: RootPrimitiveTypeDefinition = BinaryTypeDefinition
    
final case class CustomPrimitiveType[T](
                                           rootValue: RootPrimitiveFieldType[T],
                                           definition: EntityType[CustomPrimitiveTypeDefinition]
                                       ) extends PrimitiveFieldType[T]:
    if definition.valueType.rootType != rootValue.typeDefinition then throw new ConsistencyException(
        s"CustomPrimitiveTypeDefinition ${definition.valueType.rootType} does not match provided value type ${rootValue.typeDefinition}!")
    def value: T = rootValue.value
    val typeDefinition: CustomPrimitiveTypeDefinition = definition.valueType

type ArrayItemType = RootPrimitiveFieldType[?] | ReferenceType

final case class ArrayType[T <: ArrayItemType](
                                                  value: List[T],
                                                  definition: EntityType[ArrayTypeDefinition]
                                              ) extends EntityFieldType:
    private val acceptableItemsTypes = definition.valueType.elementTypes.map(_.valueType)
    value.foreach(item =>
        val itemValueDef: ArrayItemValueTypeDefinitions = item.typeDefinition match
            case arrDef: ArrayItemValueTypeDefinitions => arrDef
            //impossible, but scala does not understand it
            case _ => throw new ConsistencyException(s"Unexpected ArrayItemType definition! $item")
        if acceptableItemsTypes.contains(itemValueDef)
            then throw new ConsistencyException(s"Array item type ${item.typeDefinition} does not match provided " +
                s"element type ${definition.valueType.elementTypes.head.valueType}!")
    )
    val typeDefinition: ArrayTypeDefinition = definition.valueType

type FieldType = RootPrimitiveFieldType[?] | ReferenceType | SimpleObjectType
        
final case class ObjectType(
                               value: Map[String, FieldType],
                               definition: EntityType[ObjectTypeDefinition]
                           ) extends EntityFieldType:
    checkObjectTypeData(value, definition.valueType)
    val typeDefinition: ObjectTypeDefinition = definition.valueType


final case class SimpleObjectType(
                               value: Map[String, FieldType],
                               definition: SimpleObjectTypeDefinition
                           ) extends EntityFieldType:
    checkObjectTypeData(value, definition)
    val typeDefinition: SimpleObjectTypeDefinition = definition
        
final case class ReferenceType(value: EntityId, definition: EntityType[TypeReferenceDefinition]) extends EntityFieldType:
    checkReferenceId(value, definition.valueType)
    val typeDefinition: TypeReferenceDefinition = definition.valueType
    protected var _refValue: Option[Entity] = None
    def refValue: Entity = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def setRefValue(value: Entity): Unit = _refValue =
        checkReferenceValue(value, definition.valueType.referencedType, this.value)
        Some(value)


    sealed trait SuperType[D <: AbstractTypeDefinition] extends EntityFieldType:
        def valueItem: EntityFieldType

        def definition: AbstractNamedEntityType

        val typeDefinition: D = definition.valueType match
            case customDef: D => customDef
            case _ => throw new ConsistencyException(s"Unexpected $typeName definition! $definition")

        protected def typeName: String

    final case class CustomPrimitiveSuperType[T](valueItem: RootPrimitiveFieldType[T], definition: AbstractNamedEntityType)
        extends PrimitiveFieldType[T], SuperType[CustomPrimitiveTypeDefinition]:
        def value: T = valueItem.value

        protected val typeName: String = "CustomPrimitiveType"

    final case class ArraySuperType[T <: EntityFieldType](valueItem: List[T], definition: AbstractNamedEntityType)
        extends EntityFieldType, SuperType[ArrayTypeDefinition]:
        protected val typeName: String = "ArrayType"

    final case class ObjectSuperType(valueItem: Map[String, EntityFieldType], definition: AbstractNamedEntityType)
        extends EntityFieldType, SuperType[ObjectTypeDefinition]:
        protected val typeName: String = "ObjectType"
    
final case class BackReferenceType(value: EntityId, definition: EntityType[TypeBackReferenceDefinition]) extends EntityFieldType:
    checkReferenceId(value, definition.valueType)
    val typeDefinition: TypeBackReferenceDefinition = definition.valueType
    protected var _refValue: Option[Seq[Entity]] = None
    def refValue: Seq[Entity] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def setRefValue(value: Seq[Entity]): Unit = 
        value.foreach(entity => checkReferenceValue(entity, definition.valueType.referencedType, this.value))
        _refValue = Some(value)
        
private def checkObjectTypeData(
                                   value: Map[String, FieldType],
                                   definition: FieldsContainer
                               ): Unit =
    val allFieldsDefsMap = definition.allFields
    value.foreach((fieldName, fieldValue) =>
        val fieldValueDef = fieldValue.typeDefinition match
            case feildDef: FieldValueTypeDefinitions => feildDef
            case _ => throw new ConsistencyException(s"Unexpected ObjectType definition! $definition")
        val fieldDef = allFieldsDefsMap.getOrElse(fieldName,
            throw new ConsistencyException(s"Field $fieldName is not defined in $definition!"))
        if fieldValueDef != fieldDef.valueType
        then throw new ConsistencyException(s"Field $fieldName type $fieldValueDef does not match provided " +
            s"type ${fieldDef.valueType}!")
    )
    
private def checkReferenceId(
                                value: EntityId,
                                definition: TypeReferenceDefinition | TypeBackReferenceDefinition
                            ): Unit =
    if value.typeDefinition != definition.idType then
        throw new ConsistencyException(s"Reference type ${value.typeDefinition} does not match provided " +
            s"type ${definition.idType}!")

private def checkReferenceValue(entity: Entity, refTypeDef: AbstractNamedEntityType, idValue: EntityId): Unit =
    if entity.value.typeDefinition != refTypeDef.valueType then
        throw new ConsistencyException(s"Reference value type ${entity.value.typeDefinition} does not match " +
            s"provided type ${refTypeDef.valueType}!")
    if entity.id != idValue then
        throw new ConsistencyException(s"Reference value id ${entity.id} does not match provided id $idValue!")
    

val primitiveFieldTypesMap = Map(
    BooleanTypeDefinition.name -> BooleanTypeDefinition,
    ByteTypeDefinition.name -> ByteTypeDefinition,
    ShortIntTypeDefinition.name -> ShortIntTypeDefinition,
    IntTypeDefinition.name -> IntTypeDefinition,
    LongTypeDefinition.name -> LongTypeDefinition,
    FloatTypeDefinition.name -> FloatTypeDefinition,
    DoubleTypeDefinition.name -> DoubleTypeDefinition,
    UUIDTypeDefinition.name -> UUIDTypeDefinition,
    TimeTypeDefinition.name -> TimeTypeDefinition,
    DateTypeDefinition.name -> DateTypeDefinition,
    DateTimeTypeDefinition.name -> DateTimeTypeDefinition,
    StringIdTypeDefinition.name -> StringTypeDefinition,
    FixedStringIdTypeDefinition.name -> FixedStringTypeDefinition,
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
    //reference to abstract type to make it possible to reference to any concrete nested type
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

object ByteTypeDefinition extends RootPrimitiveTypeDefinition("Byte"):
    protected override def parseIner(value: String): ByteType = ByteType(value.toByte)
object ShortIntTypeDefinition extends RootPrimitiveTypeDefinition("Short"):
    protected override def parseIner(value: String): ShortIntType = ShortIntType(value.toShort)
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
object DecimalTypeDefinition extends RootPrimitiveTypeDefinition("Decimal"):
    protected override def parseIner(value: String): DecimalType = DecimalType(BigDecimal(value))
object StringTypeDefinition extends RootPrimitiveTypeDefinition("String"):
    protected override def parseIner(value: String): StringType = StringType(value)
class FixedStringTypeDefinition(val length: Int) extends RootPrimitiveTypeDefinition("FixedString"):
    protected override def parseIner(value: String): StringType = StringType(value)
object FixedStringTypeDefinition extends RootPrimitiveTypeDefinition("FixedString"):
    protected override def parseIner(value: String): StringType = StringType(value)
    
object ArrayTypeDefinition:
    val name = "Array"
    
final case class ArrayTypeDefinition(
                                        private var _elementTypes: Set[ArrayItemTypeDefinition],
                                        idOrParent: Either[EntityIdTypeDefinition, ArrayEntitySuperType]
) extends AbstractTypeDefinition, EntityTypeDefinition:
    private var initiated = false
    def elementTypes: Set[ArrayItemTypeDefinition] = _elementTypes
    def setChildren(elementTypesValues: Set[ArrayItemTypeDefinition]): Unit =
        if (initiated) throw new TypeReinitializationException
        _elementTypes = elementTypesValues
        initiated = true
    lazy val allElementTypes: Set[ArrayItemTypeDefinition] =
        _elementTypes ++ parent.map(_.valueType.allElementTypes).getOrElse(Set.empty[ArrayItemTypeDefinition])
    lazy val idType: EntityIdTypeDefinition = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ArrayEntitySuperType] = idOrParent.toOption

type ArrayItemValueTypeDefinitions = TypeReferenceDefinition | RootPrimitiveTypeDefinition

case class ArrayItemTypeDefinition(valueType: ArrayItemValueTypeDefinitions):
    def name: String = valueType match
        case ref: TypeReferenceDefinition => ref.referencedType.name
        case root: RootPrimitiveTypeDefinition => root.name
        case _ => throw new RuntimeException("Unexpected ArrayItemType valueType")
    override def toString: String = name


case class FieldTypeDefinition(valueType: FieldValueTypeDefinitions)

type FieldValueTypeDefinitions = TypeReferenceDefinition | RootPrimitiveTypeDefinition | TypeBackReferenceDefinition | SimpleObjectTypeDefinition

object ObjectTypeDefinition:
    val name = "Object"

trait FieldsContainer:
    def fields: Map[String, FieldTypeDefinition]
    def allFields: Map[String, FieldTypeDefinition]


final case class ObjectTypeDefinition(
                                         private var _fields: Map[String, FieldTypeDefinition],
                                         idOrParent: Either[EntityIdTypeDefinition, ObjectEntitySuperType]
) extends AbstractTypeDefinition, EntityTypeDefinition, FieldsContainer:
    private var initiated = false
    def fields: Map[String, FieldTypeDefinition] = _fields
    def setChildren(fieldsValues: Map[String, FieldTypeDefinition]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldTypeDefinition] =
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldTypeDefinition])
    lazy val idType: EntityIdTypeDefinition = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ObjectEntitySuperType] = idOrParent.toOption

final case class SimpleObjectTypeDefinition(
                                               private var _fields: Map[String, FieldTypeDefinition],
                                               parent: Option[ObjectEntitySuperType]
) extends AbstractTypeDefinition, FieldsContainer:
    private var initiated = false
    def fields: Map[String, FieldTypeDefinition] = _fields
    def setChildren(fieldsValues: Map[String, FieldTypeDefinition]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldTypeDefinition] =
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldTypeDefinition])
    override def toString: String = parent.map(_.name).getOrElse("") + "{" +
        fields.map(f => s"${f._1}: ${f._2}").mkString(", ") + "}"

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
