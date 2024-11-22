package my.valerii_timakov.sgql.entity.domain.type_values

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, AbstractNamedType, AbstractType, AbstractTypeDefinition, ArrayEntityType, ArrayTypeDefinition, BackReferenceType, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveEntityType, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntitySuperType, EntityType, EntityTypeDefinition, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, ItemValueTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectEntityType, ObjectTypeDefinition, ReferenceDefinition, ReferenceType, RootPrimitiveType, RootPrimitiveTypeDefinition, ShortIdTypeDefinition, ShortIntTypeDefinition, SimpleObjectType, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition, FieldValueType, ItemValueType}
import my.valerii_timakov.sgql.exceptions.ConsistencyException

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID


sealed trait EntityId:
    def serialize: String
    def typeDefinition: EntityIdTypeDefinition
//object EmptyId extends EntityId:
//    override def serialize: String = "()"
sealed trait FilledEntityId extends EntityId
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
    def typeDefinition: FieldValueType
    
sealed abstract class ItemValue extends EntityValue:
    def typeDefinition: ItemValueType

final case class EmptyValue(typeDefinition: FieldValueType) extends EntityValue

sealed abstract class RootPrimitiveValue[T] extends ItemValue:
    def value: T
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

final case class SimpleObjectValue(
    id: Option[EntityId],
    value: Map[String, EntityValue],
    typeDefinition: SimpleObjectType
) extends EntityValue:
    checkMaybeId(id, typeDefinition.valueType.idTypeOpt)
    checkObjectTypeData(value, typeDefinition.valueType)

final case class ReferenceValue(value: FilledEntityId, typeDefinition: ReferenceType) extends ItemValue:
    checkReferenceId(value, typeDefinition.valueType)
    protected var _refValue: Option[Entity[?, ?]] = None

    def refValue: Entity[?, ?] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Entity[?, ?]] = _refValue

    def setRefValue(value: Entity[?, ?]): Unit = _refValue =
        checkReferenceValue(value, typeDefinition.valueType.referencedType, this.value)
        Some(value)

final case class BackReferenceValue(value: FilledEntityId, typeDefinition: BackReferenceType) extends EntityValue:
    checkReferenceId(value, typeDefinition.valueType)
    protected var _refValue: Option[Seq[Entity[?, ?]]] = None

    def refValue: Seq[Entity[?, ?]] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Seq[Entity[?, ?]]] = _refValue

    def setRefValue(value: Seq[Entity[?, ?]]): Unit =
        value.foreach(entity => checkReferenceValue(entity, typeDefinition.valueType.referencedType, this.value))
        _refValue = Some(value)
        
type ValueTypes = RootPrimitiveValue[?] | Seq[ItemValue] | Map[String, EntityValue]

trait Entity[VT <: Entity[VT, V], V <: ValueTypes]:
    def typeDefinition: EntityType[VT , V]
    def id: EntityId
    def value: V
    def cloneWithId(newId: EntityId): VT 

final case class CustomPrimitiveValue(
    id: EntityId,
    value: RootPrimitiveValue[?],
    typeDefinition: CustomPrimitiveEntityType
) extends Entity[CustomPrimitiveValue, RootPrimitiveValue[?]]:
    checkId(id, typeDefinition.valueType.idType)
    checkValue(value, typeDefinition.valueType)
    if typeDefinition.valueType.rootType != value.typeDefinition then throw new ConsistencyException(
        s"CustomPrimitiveTypeDefinition ${typeDefinition.valueType.rootType} does not match provided value type ${value.typeDefinition}!")
    def cloneWithId(newId: EntityId): CustomPrimitiveValue = this.copy(id = newId)

final case class ArrayValue(
    id: EntityId,
    value: Seq[ItemValue],
    typeDefinition: ArrayEntityType
) extends Entity[ArrayValue, Seq[ItemValue]]:
    checkId(id, typeDefinition.valueType.idType)
    checkArrayDate(value, typeDefinition.valueType)
    def cloneWithId(newId: EntityId): ArrayValue = this.copy(id = newId)

final case class ObjectValue(
    id: EntityId,
    value: Map[String, EntityValue],
    typeDefinition: ObjectEntityType
) extends Entity[ObjectValue, Map[String, EntityValue]]:
    checkId(id, typeDefinition.valueType.idType)
    checkObjectTypeData(value, typeDefinition.valueType)
    def cloneWithId(newId: EntityId): ObjectValue = this.copy(id = newId)

private def checkId(id: EntityId, typeDefinition: EntityIdTypeDefinition): Unit =
    if id.typeDefinition != typeDefinition then
        throw new ConsistencyException(s"Expected id type $typeDefinition does not match " +
            s"provided type ${id.typeDefinition}!")

private def checkMaybeId(id: Option[EntityId], typeDefinitionOpt: Option[EntityIdTypeDefinition]): Unit =
    typeDefinitionOpt match
        case Some(idType) =>
            id match
                case Some(idValue) =>
                    if idValue.typeDefinition != idType then
                        throw new ConsistencyException(s"Expected id type ${idValue.typeDefinition} does not match " +
                            s"provided type $idType!")
                case None => throw new ConsistencyException("Id is not provided!")
        case None =>
            if id.isDefined then throw new ConsistencyException("Id is not expected!")
            
private def checkValue(value: RootPrimitiveValue[?], definition: CustomPrimitiveTypeDefinition): Unit =
    if value.typeDefinition != definition.rootType then
        throw new ConsistencyException(s"Expected value type $definition does not match provided type ${value.typeDefinition}!")


private def checkArrayDate(value: Seq[ItemValue], definition: ArrayTypeDefinition): Unit =
    val acceptableItemsTypes = definition.elementTypes.map(_.valueType)
    value.foreach(item =>
        if acceptableItemsTypes.contains(item.typeDefinition.valueType) then
            throw new ConsistencyException(s"Array item type ${item.typeDefinition} does not match provided " +
                s"element type ${definition.elementTypes.head.valueType}!")
    )
    
private def checkObjectTypeData(
    value: Map[String, EntityValue],
    definition: FieldsContainer, 
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
    definition: ReferenceDefinition
): Unit =
    if value.typeDefinition != definition.idType then
        throw new ConsistencyException(s"Reference type ${value.typeDefinition} does not match provided " +
            s"type ${definition.idType}!")

private def checkReferenceValue(entity: Entity[?, ?], refTypeDef: AbstractEntityType, idValue: EntityId): Unit =
    if entity.typeDefinition.valueType != refTypeDef.valueType then
        throw new ConsistencyException(s"Reference value type ${entity.typeDefinition.valueType} does not match " +
            s"provided type ${refTypeDef.valueType}!")
    if entity.id != idValue then
        throw new ConsistencyException(s"Reference value id ${entity.id} does not match provided id $idValue!")

//private def isParentOf(parent: AbstractEntityType, child: EntityType[?, ?]): Boolean =
//    parent match
//        case entityType: EntityType[?, ?] => entityType.getId == child.getId
//        case superType: EntitySuperType =>
//            child.valueType.parent.fo match
//                case childEntityType: EntityTypeDefinition => childEntityType.parent.exists(_parent => _parent == parent || isParentOf(parent, _parent))
//                    isParentOf(parent, childEntityType)


