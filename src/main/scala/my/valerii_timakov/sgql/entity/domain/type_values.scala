package my.valerii_timakov.sgql.entity.domain.type_values

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityIdTypeDefinition, AbstractEntityType, AbstractNamedType, AbstractRootPrimitiveTypeDefinition, AbstractType, AbstractTypeDefinition, ArrayEntityType, ArrayTypeDefinition, BackReferenceType, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveEntityType, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntitySuperType, EntityType, EntityTypeDefinition, FieldValueType, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, FixedStringTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, ItemValueType, ItemValueTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectEntityType, ObjectTypeDefinition, ReferenceDefinition, ReferenceType, RootPrimitiveType, ShortIdTypeDefinition, ShortIntTypeDefinition, SimpleObjectType, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.ConsistencyException
import spray.json.{JsNull, JsValue}

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID


sealed trait EntityId[T, V <: EntityId[T, V]]:
    def serialize: String
    def typeDefinition: EntityIdTypeDefinition[V]
    def toJson: JsValue = typeDefinition.toJson(this.asInstanceOf[V])
//object EmptyId extends EntityId:
//    override def serialize: String = "()"
sealed trait FilledEntityId[T, V <: EntityId[T, V]] extends EntityId[T, V]
final case class ByteId(value: Byte) extends EntityId[Byte, ByteId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[ByteId] = ByteIdTypeDefinition
final case class ShortIntId(value: Short) extends EntityId[Short, ShortIntId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[ShortIntId] = ShortIdTypeDefinition
final case class IntId(value: Int) extends EntityId[Int, IntId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[IntId] = IntIdTypeDefinition
final case class LongId(value: Long) extends EntityId[Long, LongId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[LongId] = LongIdTypeDefinition
final case class StringId(value: String) extends EntityId[String, StringId]:
    if value == null then throw new IllegalArgumentException("StringId cannot be null!")
    override def serialize: String = value
    override val typeDefinition: EntityIdTypeDefinition[StringId] = StringIdTypeDefinition
final case class FixedStringId(value: String, typeRef: FixedStringIdTypeDefinition) extends EntityId[String, FixedStringId]:
    if value == null then throw new IllegalArgumentException("FixedStringId cannot be null!")
    if value == null then throw new IllegalArgumentException("FixedStringId cannot have null type!")
    if value.length != typeRef.length then throw new IllegalArgumentException(
        s"FixedStringId value must be of length ${typeRef.length}! Got: $value")
    override def serialize: String = value
    override val typeDefinition: EntityIdTypeDefinition[FixedStringId] = typeRef
final case class UUIDId(value: UUID) extends EntityId[UUID, UUIDId]:
    if value == null then throw new IllegalArgumentException("UUIDId cannot be null!")
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[UUIDId] = UUIDIdTypeDefinition

sealed abstract class EntityValue:
    def typeDefinition: FieldValueType
    def toJson: JsValue
    
sealed abstract class ItemValue extends EntityValue:
    def typeDefinition: ItemValueType

final case class EmptyValue(typeDefinition: FieldValueType) extends EntityValue:
    def toJson: JsValue = JsNull

sealed abstract class RootPrimitiveValue[T, V <: RootPrimitiveValue[T, V]] extends ItemValue:
    def value: T
    override def typeDefinition: RootPrimitiveType[T, V]
    
final case class StringValue(value: String) extends RootPrimitiveValue[String, StringValue]:
    def typeDefinition: RootPrimitiveType[String, StringValue] = RootPrimitiveType[String, StringValue](StringTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class FixedStringValue(value: String, typeDef: FixedStringTypeDefinition) extends RootPrimitiveValue[String, FixedStringValue]:
    if value == null then throw new IllegalArgumentException("FixedStringValue cannot be null!")
    if value == null then throw new IllegalArgumentException("FixedStringValue cannot have null type!")
    if value.length != typeDef.length then throw new IllegalArgumentException(
        s"FixedStringValue value must be of length ${typeDef.length}! Got: $value")
    def typeDefinition: RootPrimitiveType[String, FixedStringValue] = RootPrimitiveType[String, FixedStringValue](typeDef)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class ByteValue(value: Byte) extends RootPrimitiveValue[Byte, ByteValue]:
    def typeDefinition: RootPrimitiveType[Byte, ByteValue] = RootPrimitiveType[Byte, ByteValue](ByteTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class ShortIntValue(value: Short) extends RootPrimitiveValue[Short, ShortIntValue]:
    def typeDefinition: RootPrimitiveType[Short, ShortIntValue] = RootPrimitiveType[Short, ShortIntValue](ShortIntTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class IntValue(value: Int) extends RootPrimitiveValue[Int, IntValue]:
    def typeDefinition: RootPrimitiveType[Int, IntValue] = RootPrimitiveType(IntTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class LongValue(value: Long) extends RootPrimitiveValue[Long, LongValue]:
    def typeDefinition: RootPrimitiveType[Long, LongValue] = RootPrimitiveType(LongTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)

final case class DecimalValue(value: BigDecimal) extends RootPrimitiveValue[BigDecimal, DecimalValue]:
    if value == null then throw new IllegalArgumentException("DecimalValue cannot be null!")
    def typeDefinition: RootPrimitiveType[BigDecimal, DecimalValue] = RootPrimitiveType(DecimalTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class DoubleValue(value: Double) extends RootPrimitiveValue[Double, DoubleValue]:
    def typeDefinition: RootPrimitiveType[Double, DoubleValue] = RootPrimitiveType(DoubleTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class FloatValue(value: Float) extends RootPrimitiveValue[Float, FloatValue]:
    def typeDefinition: RootPrimitiveType[Float, FloatValue] = RootPrimitiveType(FloatTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class BooleanValue(value: Boolean) extends RootPrimitiveValue[Boolean, BooleanValue]:
    def typeDefinition: RootPrimitiveType[Boolean, BooleanValue] = RootPrimitiveType(BooleanTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class DateValue(value: LocalDate) extends RootPrimitiveValue[LocalDate, DateValue]:
    if value == null then throw new IllegalArgumentException("DateValue cannot be null!")
    def typeDefinition: RootPrimitiveType[LocalDate, DateValue] = RootPrimitiveType(DateTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class DateTimeValue(value: LocalDateTime) extends RootPrimitiveValue[LocalDateTime, DateTimeValue]:
    if value == null then throw new IllegalArgumentException("DateTimeValue cannot be null!")
    def typeDefinition: RootPrimitiveType[LocalDateTime, DateTimeValue] = RootPrimitiveType(DateTimeTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class TimeValue(value: LocalTime) extends RootPrimitiveValue[LocalTime, TimeValue]:
    if value == null then throw new IllegalArgumentException("TimeValue cannot be null!")
    def typeDefinition: RootPrimitiveType[LocalTime, TimeValue] = RootPrimitiveType(TimeTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class UUIDValue(value: UUID) extends RootPrimitiveValue[UUID, UUIDValue]:
    if value == null then throw new IllegalArgumentException("UUIDValue cannot be null!")
    def typeDefinition: RootPrimitiveType[UUID, UUIDValue] = RootPrimitiveType(UUIDTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)
    
final case class BinaryValue(value: Array[Byte]) extends RootPrimitiveValue[Array[Byte], BinaryValue]:
    if value == null then throw new IllegalArgumentException("BinaryValue cannot be null!")
    def typeDefinition: RootPrimitiveType[Array[Byte], BinaryValue] = RootPrimitiveType(BinaryTypeDefinition)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)

final case class SimpleObjectValue(
    id: Option[EntityId[?, ?]],
    value: Map[String, EntityValue],
    typeDefinition: SimpleObjectType
) extends EntityValue:
    checkMaybeId(id, typeDefinition.valueType.idTypeOpt)
    checkObjectTypeData(value, typeDefinition.valueType)
    def toJson: JsValue = ???//typeDefinition.valueType.toJson(value) 

final case class ReferenceValue(refId: FilledEntityId[?, ?], typeDefinition: ReferenceType) extends ItemValue:
    checkReferenceId(refId, typeDefinition.valueType)
    protected var _refValue: Option[Entity[?, ?]] = None
    def toJson: JsValue = ???//typeDefinition.valueType.toJson(value)


    def refValue: Entity[?, ?] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Entity[?, ?]] = _refValue

    def setRefValue(value: Entity[?, ?]): Unit = _refValue =
        checkReferenceValue(value, typeDefinition.valueType.referencedType, this.refId)
        Some(value)

final case class BackReferenceValue(value: FilledEntityId[?, ?], typeDefinition: BackReferenceType) extends EntityValue:
    checkReferenceId(value, typeDefinition.valueType)
    protected var _refValue: Option[Seq[Entity[?, ?]]] = None
    def toJson: JsValue = ???

    def refValue: Seq[Entity[?, ?]] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Seq[Entity[?, ?]]] = _refValue

    def setRefValue(value: Seq[Entity[?, ?]]): Unit =
        value.foreach(entity => checkReferenceValue(entity, typeDefinition.valueType.referencedType, this.value))
        _refValue = Some(value)
        
type ValueTypes = RootPrimitiveValue[?, ?] | Seq[ItemValue] | Map[String, EntityValue]

trait Entity[VT <: Entity[VT, V], V <: ValueTypes]:
    def typeDefinition: EntityType[VT , V]
    def id: EntityId[?, ?]
    def value: V
    def cloneWithId(newId: EntityId[?, ?]): VT
    def toJson: JsValue = typeDefinition.valueType.toJson(this.value)

final case class CustomPrimitiveValue(
    id: EntityId[?, ?],
    value: RootPrimitiveValue[?, ?],
    typeDefinition: CustomPrimitiveEntityType
) extends Entity[CustomPrimitiveValue, RootPrimitiveValue[?, ?]]:
    checkId(id, typeDefinition.valueType.idType)
    checkValue(value, typeDefinition.valueType)
    if typeDefinition.valueType.rootType != value.typeDefinition then throw new ConsistencyException(
        s"CustomPrimitiveTypeDefinition ${typeDefinition.valueType.rootType} does not match provided value type ${value.typeDefinition}!")
    def cloneWithId(newId: EntityId[?, ?]): CustomPrimitiveValue = this.copy(id = newId)

final case class ArrayValue(
    id: EntityId[?, ?],
    value: Seq[ItemValue],
    typeDefinition: ArrayEntityType
) extends Entity[ArrayValue, Seq[ItemValue]]:
    checkId(id, typeDefinition.valueType.idType)
    checkArrayData(value, typeDefinition.valueType)
    def cloneWithId(newId: EntityId[?, ?]): ArrayValue = this.copy(id = newId)

final case class ObjectValue(
    id: EntityId[?, ?],
    value: Map[String, EntityValue],
    typeDefinition: ObjectEntityType
) extends Entity[ObjectValue, Map[String, EntityValue]]:
    checkId(id, typeDefinition.valueType.idType)
    checkObjectTypeData(value, typeDefinition.valueType)
    def cloneWithId(newId: EntityId[?, ?]): ObjectValue = this.copy(id = newId)

private def checkId(id: EntityId[?, ?], typeDefinition: AbstractEntityIdTypeDefinition[?]): Unit =
    if id.typeDefinition.name != typeDefinition.name then
        throw new ConsistencyException(s"Expected id type $typeDefinition does not match " +
            s"provided type ${id.typeDefinition}!")

private def checkMaybeId(id: Option[EntityId[?, ?]], typeDefinitionOpt: Option[AbstractEntityIdTypeDefinition[?]]): Unit =
    typeDefinitionOpt match
        case Some(idType) =>
            id match
                case Some(idValue) =>
                    if idValue.typeDefinition.name != idType.name then
                        throw new ConsistencyException(s"Expected id type ${idValue.typeDefinition} does not match " +
                            s"provided type $idType!")
                case None => throw new ConsistencyException("Id is not provided!")
        case None =>
            if id.isDefined then throw new ConsistencyException("Id is not expected!")
            
private def checkValue(value: RootPrimitiveValue[?, ?], definition: CustomPrimitiveTypeDefinition): Unit =
    if value.typeDefinition.name != definition.rootType.name then
        throw new ConsistencyException(s"Expected value type $definition does not match provided type ${value.typeDefinition}!")


private def checkArrayData(value: Seq[ItemValue], definition: ArrayTypeDefinition): Unit =
    val acceptableItemsTypes = definition.elementTypes.map(_.name)
    value.foreach(item =>
        if acceptableItemsTypes.contains(item.typeDefinition.name) then
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
            case fieldDef: FieldValueTypeDefinition[?] => fieldDef
            case _ => throw new ConsistencyException(s"Unexpected ObjectType definition! $definition")
        val fieldDef = allFieldsDefsMap.getOrElse(fieldName,
            throw new ConsistencyException(s"Field $fieldName is not defined in $definition!"))
        if (fieldValueDef != fieldDef.valueType) 
            throw new ConsistencyException(s"Field $fieldName type $fieldValueDef does not match provided " +
                s"type ${fieldDef.valueType}!")
    )

private def checkReferenceId(
    value: FilledEntityId[?, ?],
    definition: ReferenceDefinition[?]
): Unit =
    if value.typeDefinition != definition.idType then
        throw new ConsistencyException(s"Reference type ${value.typeDefinition} does not match provided " +
            s"type ${definition.idType}!")

private def checkReferenceValue(entity: Entity[?, ?], refTypeDef: AbstractEntityType, idValue: EntityId[?, ?]): Unit =
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


