package my.valerii_timakov.sgql.entity.domain.type_values

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityIdTypeDefinition,  AbstractRootPrimitiveTypeDefinition, AbstractTypeDefinition, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntityTypeDefinition, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, FixedStringTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, ItemValueTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectTypeDefinition, ReferenceDefinition, ShortIdTypeDefinition, ShortIntTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, AbstractNamedType, AbstractType, ArrayEntityType, BackReferenceType, CustomPrimitiveEntityType,  EntitySuperType, EntityType, FieldValueType,  ItemValueType, ObjectEntityType,ReferenceType, RootPrimitiveType, SimpleObjectType}

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
sealed trait FilledEntityId[T, V <: FilledEntityId[T, V]] extends EntityId[T, V]
final case class ByteId(value: Byte) extends FilledEntityId[Byte, ByteId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[ByteId] = ByteIdTypeDefinition
final case class ShortIntId(value: Short) extends FilledEntityId[Short, ShortIntId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[ShortIntId] = ShortIdTypeDefinition
final case class IntId(value: Int) extends FilledEntityId[Int, IntId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[IntId] = IntIdTypeDefinition
final case class LongId(value: Long) extends FilledEntityId[Long, LongId]:
    override def serialize: String = value.toString
    override val typeDefinition: EntityIdTypeDefinition[LongId] = LongIdTypeDefinition
final case class StringId(value: String) extends FilledEntityId[String, StringId]:
    if value == null then throw new IllegalArgumentException("StringId cannot be null!")
    override def serialize: String = value
    override val typeDefinition: EntityIdTypeDefinition[StringId] = StringIdTypeDefinition
final case class FixedStringId(value: String, typeRef: FixedStringIdTypeDefinition) extends FilledEntityId[String, FixedStringId]:
    if value == null then throw new IllegalArgumentException("FixedStringId cannot be null!")
    if value == null then throw new IllegalArgumentException("FixedStringId cannot have null type!")
    if value.length != typeRef.length then throw new IllegalArgumentException(
        s"FixedStringId value must be of length ${typeRef.length}! Got: $value")
    override def serialize: String = value
    override val typeDefinition: EntityIdTypeDefinition[FixedStringId] = typeRef
final case class UUIDId(value: UUID) extends FilledEntityId[UUID, UUIDId]:
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

final case class SimpleObjectValue[ID <: FilledEntityId[_, ID]](
    id: Option[ID],
    value: Map[String, EntityValue],
    typeDefinition: SimpleObjectType[ID]
) extends EntityValue:
    checkMaybeId(id, typeDefinition.valueType.idTypeOpt)
    checkObjectTypeData(value, typeDefinition.valueType)
    def toJson: JsValue = typeDefinition.valueType.toJson(this)

final case class ReferenceValue[ID <: FilledEntityId[_, ID]](refId: ID, typeDefinition: ReferenceType[ID]) extends ItemValue:
    checkReferenceId(refId, typeDefinition.valueType)
    protected var _refValue: Option[Entity[ID, _, _]] = None
    def toJson: JsValue = typeDefinition.valueType.toJson(this)


    def refValue: Entity[ID, _, _] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Entity[ID, _, _]] = _refValue

    def setRefValue(value: Entity[ID, _, _]): Unit = _refValue =
        checkReferenceValue(value, typeDefinition.valueType.referencedType, this.refId)
        Some(value)

final case class BackReferenceValue[ID <: FilledEntityId[_, ID]](value: ID, typeDefinition: BackReferenceType[ID]) extends EntityValue:
    checkReferenceId(value, typeDefinition.valueType)
    private var _refValue: Option[Seq[Entity[ID, _, _]]] = None
    def toJson: JsValue = typeDefinition.valueType.toJson(this)

    def refValue: Seq[Entity[ID, _, _]] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Seq[Entity[ID, _, _]]] = _refValue

    def setRefValue(value: Seq[Entity[ID, _, _]]): Unit =
        value.foreach(entity => checkReferenceValue(entity, typeDefinition.valueType.referencedType, this.value))
        _refValue = Some(value)
        
type ValueTypes = RootPrimitiveValue[_, _] | Seq[ItemValue] | Map[String, EntityValue]

trait Entity[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes]:
    def typeDefinition: EntityType[ID, VT , V]
    def id: ID
    def value: V
    def cloneWithId(newId: ID):  Entity[ID, VT, V]
    def toJson: JsValue = typeDefinition.valueType.toJson(this.value)

final case class CustomPrimitiveValue[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[_, V]](
    id: ID,
    value: V,
    typeDefinition: CustomPrimitiveEntityType[ID, VT, V]
) extends Entity[ID, VT, V]:
    checkId(id, typeDefinition.valueType.idType)
    checkValue(value, typeDefinition.valueType)
    if typeDefinition.valueType.rootType != value.typeDefinition then throw new ConsistencyException(
        s"CustomPrimitiveTypeDefinition ${typeDefinition.valueType.rootType} does not match provided value type ${value.typeDefinition}!")
    def cloneWithId(newId: ID): CustomPrimitiveValue[ID, VT, V] = this.copy(id = newId)

final case class ArrayValue[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
    id: ID,
    value: Seq[ItemValue],
    typeDefinition: ArrayEntityType[ID, VT]
) extends Entity[ID, VT, Seq[ItemValue]]:
    checkId(id, typeDefinition.valueType.idType)
    checkArrayData(value, typeDefinition.valueType)
    def cloneWithId(newId: ID): VT = this.copy(id = newId).asInstanceOf[VT]

final case class ObjectValue[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
    id: ID,
    value: Map[String, EntityValue],
    typeDefinition: ObjectEntityType[ID, VT]
) extends Entity[ID, VT, Map[String, EntityValue]]:
    checkId(id, typeDefinition.valueType.idType)
    checkObjectTypeData(value, typeDefinition.valueType)
    def cloneWithId(newId: ID): ObjectValue[ID, VT] = this.copy(id = newId)

private def checkId(id: EntityId[_, _], typeDefinition: AbstractEntityIdTypeDefinition[_]): Unit =
    if id.typeDefinition.name != typeDefinition.name then
        throw new ConsistencyException(s"Expected id type $typeDefinition does not match " +
            s"provided type ${id.typeDefinition}!")

private def checkMaybeId(id: Option[EntityId[_, _]], typeDefinitionOpt: Option[AbstractEntityIdTypeDefinition[_]]): Unit =
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
            
private def checkValue(value: RootPrimitiveValue[_, _], definition: CustomPrimitiveTypeDefinition[_, _, _]): Unit =
    if value.typeDefinition.name != definition.rootType.name then
        throw new ConsistencyException(s"Expected value type $definition does not match provided type ${value.typeDefinition}!")


private def checkArrayData(value: Seq[ItemValue], definition: ArrayTypeDefinition[_, _]): Unit =
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
            case fieldDef: FieldValueTypeDefinition[_] => fieldDef
            case _ => throw new ConsistencyException(s"Unexpected ObjectType definition! $definition")
        val fieldDef = allFieldsDefsMap.getOrElse(fieldName,
            throw new ConsistencyException(s"Field $fieldName is not defined in $definition!"))
        if (fieldValueDef != fieldDef.valueType) 
            throw new ConsistencyException(s"Field $fieldName type $fieldValueDef does not match provided " +
                s"type ${fieldDef.valueType}!")
    )

private def checkReferenceId[ID <: FilledEntityId[_, ID]](
    value: ID,
    definition: ReferenceDefinition[ID, _]
): Unit =
    if value.typeDefinition != definition.idType then
        throw new ConsistencyException(s"Reference type ${value.typeDefinition} does not match provided " +
            s"type ${definition.idType}!")

private def checkReferenceValue(entity: Entity[_, _, _], refTypeDef: AbstractEntityType[_, _, _], idValue: EntityId[_, _]): Unit =
    if entity.typeDefinition.valueType != refTypeDef.valueType then
        throw new ConsistencyException(s"Reference value type ${entity.typeDefinition.valueType} does not match " +
            s"provided type ${refTypeDef.valueType}!")
    if entity.id != idValue then
        throw new ConsistencyException(s"Reference value id ${entity.id} does not match provided id $idValue!")

//private def isParentOf(parent: AbstractEntityType, child: EntityType[_, _]): Boolean =
//    parent match
//        case entityType: EntityType[_, _] => entityType.getId == child.getId
//        case superType: EntitySuperType =>
//            child.valueType.parent.fo match
//                case childEntityType: EntityTypeDefinition => childEntityType.parent.exists(_parent => _parent == parent || isParentOf(parent, _parent))
//                    isParentOf(parent, childEntityType)


