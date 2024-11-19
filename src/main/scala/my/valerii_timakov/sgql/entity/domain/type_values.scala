package my.valerii_timakov.sgql.entity.domain.type_values

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, AbstractNamedType, AbstractType, AbstractTypeDefinition, ItemValueTypeDefinition, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntityType, EntityTypeDefinition, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectTypeDefinition, RootPrimitiveType, RootPrimitiveTypeDefinition, ShortIdTypeDefinition, ShortIntTypeDefinition, SimpleObjectType, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.ConsistencyException

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID


sealed trait EntityId:
    def serialize: String
object EmptyId extends EntityId:
    override def serialize: String = "()"
sealed trait FilledEntityId extends EntityId:
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

sealed abstract class EntityValue:
    def definition: FieldValueTypeDefinition
    
sealed abstract class FilledEntityValue extends EntityValue

final case class EmptyValue(typeDefinition: AbstractNamedType) extends EntityValue

sealed abstract class PrimitiveValue[T] extends FilledEntityValue:
    def value: T
sealed abstract class RootPrimitiveValue[T] extends PrimitiveValue[T]:
    override def typeDefinition: RootPrimitiveType
final case class StringValue(value: String) extends RootPrimitiveValue[String]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(StringTypeDefinition)
final case class ByteValue(value: Byte) extends RootPrimitiveValue[Byte]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(ByteTypeDefinition)
final case class ShortIntValue(value: Short) extends RootPrimitiveValue[Short]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(ShortIntTypeDefinition)
final case class IntValue(value: Int) extends RootPrimitiveValue[Int]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(IntTypeDefinition)
final case class LongValue(value: Long) extends RootPrimitiveValue[Long]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(LongTypeDefinition)
final case class DoubleValue(value: Double) extends RootPrimitiveValue[Double]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(DoubleTypeDefinition)
final case class FloatValue(value: Float) extends RootPrimitiveValue[Float]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(FloatTypeDefinition)
final case class BooleanValue(value: Boolean) extends RootPrimitiveValue[Boolean]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(BooleanTypeDefinition)
final case class DateValue(value: LocalDate) extends RootPrimitiveValue[LocalDate]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(DateTypeDefinition)
final case class DateTimeValue(value: LocalTime) extends RootPrimitiveValue[LocalTime]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(DateTimeTypeDefinition)
final case class TimeValue(value: LocalDateTime) extends RootPrimitiveValue[LocalDateTime]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(TimeTypeDefinition)
final case class UUIDValue(value: UUID) extends RootPrimitiveValue[UUID]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(UUIDTypeDefinition)
final case class DecimalValue(value: BigDecimal) extends RootPrimitiveValue[BigDecimal]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(DecimalTypeDefinition)
final case class BinaryValue(value: Array[Byte]) extends RootPrimitiveValue[Array[Byte]]:
    def typeDefinition: RootPrimitiveType = RootPrimitiveType(BinaryTypeDefinition)


trait Entity:
    def typeDefinition: AbstractType
    def id: EntityId
    def value: FilledEntityValue
    def cloneWithId(newId: EntityId): this.type 

final case class CustomPrimitiveValue(
    id: EntityId,
    value: RootPrimitiveValue[?],
    typeDefinition: EntityType[CustomPrimitiveValue, RootPrimitiveValue[?], CustomPrimitiveTypeDefinition]
) extends Entity:
    if typeDefinition.valueType.rootType != value.typeDefinition then throw new ConsistencyException(
        s"CustomPrimitiveTypeDefinition ${typeDefinition.valueType.rootType} does not match provided value type ${value.typeDefinition}!")
//    def primitiveValue: T = value.value
    def cloneWithId(newId: EntityId): CustomPrimitiveValue = this.copy(id = newId)

type ArrayItemValue = RootPrimitiveValue[?] | ReferenceValue

final case class ArrayValue(
    id: EntityId,
    value: Seq[ArrayItemValue],
    typeDefinition: EntityType[ArrayValue, Seq[ArrayItemValue], ArrayTypeDefinition]
) extends Entity:
    private val acceptableItemsTypes = typeDefinition.valueType.elementTypes.map(_.valueType)
    def cloneWithId(newId: EntityId): ArrayValue = this.copy(id = newId)
    value.foreach(item =>
        val itemValueDef: ItemValueTypeDefinition = item.definition match
            case arrDef: ItemValueTypeDefinition => arrDef
            //impossible, but scala does not understand it
            case _ => throw new ConsistencyException(s"Unexpected ArrayItemType definition! $item")
        if acceptableItemsTypes.contains(itemValueDef)
        then throw new ConsistencyException(s"Array item type ${item.definition} does not match provided " +
            s"element type ${typeDefinition.valueType.elementTypes.head.valueType}!")
    )

type FieldValue = RootPrimitiveValue[?] | ReferenceValue | BackReferenceValue | SimpleObjectValue | EmptyValue

final case class ObjectValue(
    id: EntityId,
    value: Map[String, FieldValue],
    typeDefinition: EntityType[ObjectValue, Map[String, FieldValue], ObjectTypeDefinition]
) extends Entity:
    checkObjectTypeData(value, typeDefinition.valueType)
    def cloneWithId(newId: EntityId): ObjectValue = this.copy(id = newId)


final case class SimpleObjectValue(
    value: Map[String, FieldValue],
    valueType: SimpleObjectType
) extends FilledEntityValue:
    checkObjectTypeData(value, valueType.valueType)
    val typeDefinition: SimpleObjectType = valueType

final case class ReferenceValue(value: FilledEntityId, definition: TypeReferenceDefinition) extends FilledEntityValue:
    checkReferenceId(value, definition)
    protected var _refValue: Option[Entity] = None
    def refValue: Entity = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def setRefValue(value: Entity): Unit = _refValue =
        checkReferenceValue(value, definition.referencedType, this.value)
        Some(value)

final case class BackReferenceValue(value: FilledEntityId, definition: TypeBackReferenceDefinition) extends FilledEntityValue:
    checkReferenceId(value, definition)
    protected var _refValue: Option[Seq[Entity]] = None
    def refValue: Seq[Entity] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def setRefValue(value: Seq[Entity]): Unit =
        value.foreach(entity => checkReferenceValue(entity, definition.referencedType, this.value))
        _refValue = Some(value)


private def checkObjectTypeData(
    value: Map[String, FieldValue],
    definition: FieldsContainer
): Unit =
    val allFieldsDefsMap = definition.allFields
    value.foreach((fieldName, fieldValue) =>
        val fieldValueDef = fieldValue.typeDefinition match
            case feildDef: FieldValueTypeDefinition => feildDef
            case _ => throw new ConsistencyException(s"Unexpected ObjectType definition! $definition")
        val fieldDef = allFieldsDefsMap.getOrElse(fieldName,
            throw new ConsistencyException(s"Field $fieldName is not defined in $definition!"))
        if fieldValueDef != fieldDef.valueType then 
            throw new ConsistencyException(s"Field $fieldName type $fieldValueDef does not match provided " +
                s"type ${fieldDef.valueType}!")
    )

private def checkReferenceId(
    value: FilledEntityId,
    definition: TypeReferenceDefinition | TypeBackReferenceDefinition
): Unit =
    if value.typeDefinition != definition.idType then
        throw new ConsistencyException(s"Reference type ${value.typeDefinition} does not match provided " +
            s"type ${definition.idType}!")

private def checkReferenceValue(entity: Entity, refTypeDef: AbstractEntityType, idValue: EntityId): Unit =
    if entity.value.typeDefinition != refTypeDef.valueType then
        throw new ConsistencyException(s"Reference value type ${entity.value.typeDefinition} does not match " +
            s"provided type ${refTypeDef.valueType}!")
    if entity.id != idValue then
        throw new ConsistencyException(s"Reference value id ${entity.id} does not match provided id $idValue!")

//private def isParentOf(parent: AbstractEntityType, child: AbstractTypeDefinition): Boolean =
//    parent match
//        case EntityType(name, _) => name == child.name
//        case superType: NamedEntitySuperType =>
//            child match
//                case childEntityType: EntityTypeDefinition => childEntityType.parent.exists(_parent => _parent == parent || isParentOf(parent, _parent))
//                    isParentOf(parent, childEntityType)


