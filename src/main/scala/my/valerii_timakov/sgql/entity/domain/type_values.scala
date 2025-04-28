package my.valerii_timakov.sgql.entity.domain.type_values

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityIdTypeDefinition, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, FixedStringTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ReferenceDefinition, ShortIdTypeDefinition, ShortIntTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, AbstractObjectEntityType, ArrayEntityType, BackReferenceType, CustomPrimitiveEntityType, EntitySuperType, EntityType, FieldValueType, ItemValueType, ObjectEntityType, ReferenceType, RootPrimitiveType, SimpleObjectType}
import my.valerii_timakov.sgql.exceptions.ConsistencyException
import spray.json.{JsNull, JsValue}

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID


sealed trait TypeValue:
    def value: Any

sealed trait EntityId[T, V <: EntityId[T, V]] extends TypeValue:
    def serialize: String
    def typeDefinition: EntityIdTypeDefinition[V]
    def toJson: JsValue = typeDefinition.toJson(this.asInstanceOf[V])
    def value: T
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
    def valueType: FieldValueType
    def toJson: JsValue
    
sealed abstract class ItemValue extends EntityValue:
    def valueType: ItemValueType

final case class EmptyValue(valueType: FieldValueType) extends EntityValue:
    def toJson: JsValue = JsNull

sealed abstract class RootPrimitiveValue[V <: RootPrimitiveValue[V]] extends ItemValue with TypeValue:
    override def valueType: RootPrimitiveType[V]
    
final case class StringValue(value: String) extends RootPrimitiveValue[StringValue]:
    def valueType: RootPrimitiveType[StringValue] = RootPrimitiveType[StringValue](StringTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class FixedStringValue(value: String, typeDef: FixedStringTypeDefinition) extends RootPrimitiveValue[FixedStringValue]:
    if value == null then throw new IllegalArgumentException("FixedStringValue cannot be null!")
    if value == null then throw new IllegalArgumentException("FixedStringValue cannot have null type!")
    if value.length != typeDef.length then throw new IllegalArgumentException(
        s"FixedStringValue value must be of length ${typeDef.length}! Got: $value")
    def valueType: RootPrimitiveType[FixedStringValue] = RootPrimitiveType[FixedStringValue](typeDef)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class ByteValue(value: Byte) extends RootPrimitiveValue[ByteValue]:
    def valueType: RootPrimitiveType[ByteValue] = RootPrimitiveType[ByteValue](ByteTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class ShortIntValue(value: Short) extends RootPrimitiveValue[ShortIntValue]:
    def valueType: RootPrimitiveType[ShortIntValue] = RootPrimitiveType[ShortIntValue](ShortIntTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class IntValue(value: Int) extends RootPrimitiveValue[IntValue]:
    def valueType: RootPrimitiveType[IntValue] = RootPrimitiveType(IntTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class LongValue(value: Long) extends RootPrimitiveValue[LongValue]:
    def valueType: RootPrimitiveType[LongValue] = RootPrimitiveType(LongTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)

final case class DecimalValue(value: BigDecimal) extends RootPrimitiveValue[DecimalValue]:
    if value == null then throw new IllegalArgumentException("DecimalValue cannot be null!")
    def valueType: RootPrimitiveType[DecimalValue] = RootPrimitiveType(DecimalTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class DoubleValue(value: Double) extends RootPrimitiveValue[DoubleValue]:
    def valueType: RootPrimitiveType[DoubleValue] = RootPrimitiveType(DoubleTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class FloatValue(value: Float) extends RootPrimitiveValue[FloatValue]:
    def valueType: RootPrimitiveType[FloatValue] = RootPrimitiveType(FloatTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class BooleanValue(value: Boolean) extends RootPrimitiveValue[BooleanValue]:
    def valueType: RootPrimitiveType[BooleanValue] = RootPrimitiveType(BooleanTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class DateValue(value: LocalDate) extends RootPrimitiveValue[DateValue]:
    if value == null then throw new IllegalArgumentException("DateValue cannot be null!")
    def valueType: RootPrimitiveType[DateValue] = RootPrimitiveType(DateTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class DateTimeValue(value: LocalDateTime) extends RootPrimitiveValue[DateTimeValue]:
    if value == null then throw new IllegalArgumentException("DateTimeValue cannot be null!")
    def valueType: RootPrimitiveType[DateTimeValue] = RootPrimitiveType(DateTimeTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class TimeValue(value: LocalTime) extends RootPrimitiveValue[TimeValue]:
    if value == null then throw new IllegalArgumentException("TimeValue cannot be null!")
    def valueType: RootPrimitiveType[TimeValue] = RootPrimitiveType(TimeTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class UUIDValue(value: UUID) extends RootPrimitiveValue[UUIDValue]:
    if value == null then throw new IllegalArgumentException("UUIDValue cannot be null!")
    def valueType: RootPrimitiveType[UUIDValue] = RootPrimitiveType(UUIDTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    
final case class BinaryValue(value: Array[Byte]) extends RootPrimitiveValue[BinaryValue]:
    if value == null then throw new IllegalArgumentException("BinaryValue cannot be null!")
    def valueType: RootPrimitiveType[BinaryValue] = RootPrimitiveType(BinaryTypeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)

final case class SimpleObjectValue[ID <: EntityId[_, ID]](
                                                             //save concrete parent type, because it is defined by value, not by type definition
                                                             idAndParentType: Option[(ID, AbstractObjectEntityType[ID, _])],
                                                             value: Map[String, EntityValue],
                                                             valueType: SimpleObjectType[ID]
) extends EntityValue:
    checkMaybeId(id, valueType.typeDefinition.idTypeOpt)
    checkObjectTypeData(value, valueType.typeDefinition)
    def toJson: JsValue = valueType.typeDefinition.toJson(this)
    def id: Option[ID] = idAndParentType.map(_._1)

final case class ReferenceValue[ID <: EntityId[_, ID]](refId: ID, valueType: ReferenceType[ID]) extends ItemValue:
    checkReferenceId(refId, valueType.typeDefinition)
    protected var _refValue: Option[Entity[ID, _, _]] = None
    def toJson: JsValue = valueType.typeDefinition.toJson(this)


    def refValue: Entity[ID, _, _] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Entity[ID, _, _]] = _refValue

    def setRefValue(value: Entity[ID, _, _]): Unit = _refValue =
        checkReferenceValue(value, valueType.typeDefinition.referencedType, this.refId)
        Some(value)

final case class BackReferenceValue[ID <: EntityId[_, ID]](value: ID, valueType: BackReferenceType[ID]) extends EntityValue:
    checkReferenceId(value, valueType.typeDefinition)
    private var _refValue: Option[Seq[Entity[ID, _, _]]] = None
    def toJson: JsValue = valueType.typeDefinition.toJson(this)

    def refValue: Seq[Entity[ID, _, _]] = _refValue.getOrElse(throw new ConsistencyException("Reference value is not set!"))
    def refValueOpt: Option[Seq[Entity[ID, _, _]]] = _refValue

    def setRefValue(value: Seq[Entity[ID, _, _]]): Unit =
        value.foreach(entity => checkReferenceValue(entity, valueType.typeDefinition.referencedType, this.value))
        _refValue = Some(value)
        
type ValueTypes = RootPrimitiveValue[_] | Seq[ItemValue] | Map[String, EntityValue]

trait Entity[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes]:
    def typeDefinition: EntityType[ID, VT , V]
    def id: ID
    def value: V
    def cloneWithId(newId: ID):  Entity[ID, VT, V]
    def toJson: JsValue = typeDefinition.typeDefinition.toJson(this.value)

final case class CustomPrimitiveValue[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[V]](
    id: ID,
    value: V,
    typeDefinition: CustomPrimitiveEntityType[ID, VT, V]
) extends Entity[ID, VT, V]:
    checkId(id, typeDefinition.typeDefinition.idType)
    checkValue(value, typeDefinition.typeDefinition)
    if typeDefinition.typeDefinition.rootType != value.valueType then throw new ConsistencyException(
        s"CustomPrimitiveTypeDefinition ${typeDefinition.typeDefinition.rootType} does not match provided value type ${value.valueType}!")
    def cloneWithId(newId: ID): CustomPrimitiveValue[ID, VT, V] = this.copy(id = newId)
    

final case class ArrayValue[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
    id: ID,
    value: Seq[ItemValue],
    typeDefinition: ArrayEntityType[ID, VT]
) extends Entity[ID, VT, Seq[ItemValue]]:
    checkId(id, typeDefinition.typeDefinition.idType)
    checkArrayData(value, typeDefinition.typeDefinition)
    def cloneWithId(newId: ID): VT = this.copy(id = newId).asInstanceOf[VT]

final case class ObjectValue[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
    id: ID,
    value: Map[String, EntityValue],
    typeDefinition: ObjectEntityType[ID, VT]
) extends Entity[ID, VT, Map[String, EntityValue]]:
    checkId(id, typeDefinition.typeDefinition.idType)
    checkObjectTypeData(value, typeDefinition.typeDefinition)
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
            
private def checkValue(value: RootPrimitiveValue[_], definition: CustomPrimitiveTypeDefinition[_, _, _]): Unit =
    if value.valueType.name != definition.rootType.name then
        throw new ConsistencyException(s"Expected value type $definition does not match provided type ${value.valueType}!")


private def checkArrayData(value: Seq[ItemValue], definition: ArrayTypeDefinition[_, _]): Unit =
    val acceptableItemsTypes = definition.elementTypes.map(_.name)
    value.foreach(item =>
        if acceptableItemsTypes.contains(item.valueType.name) then
            throw new ConsistencyException(s"Array item type ${item.valueType} does not match provided " +
                s"element type ${definition.elementTypes.head.valueType}!")
    )
    
private def checkObjectTypeData(
    value: Map[String, EntityValue],
    definition: FieldsContainer, 
): Unit =
    val allFieldsDefsMap = definition.allFields
    value.foreach((fieldName, fieldValue) =>
        val fieldValueDef = fieldValue.valueType.typeDefinition match
            case fieldDef: FieldValueTypeDefinition[_] => fieldDef
            case _ => throw new ConsistencyException(s"Unexpected ObjectType definition! $definition")
        val fieldDef = allFieldsDefsMap.getOrElse(fieldName,
            throw new ConsistencyException(s"Field $fieldName is not defined in $definition!"))
        if (fieldValueDef != fieldDef.valueType) 
            throw new ConsistencyException(s"Field $fieldName type $fieldValueDef does not match provided " +
                s"type ${fieldDef.valueType}!")
    )

private def checkReferenceId[ID <: EntityId[_, ID]](
    value: ID,
    definition: ReferenceDefinition[ID, _]
): Unit =
    if value.typeDefinition != definition.idType then
        throw new ConsistencyException(s"Reference type ${value.typeDefinition} does not match provided " +
            s"type ${definition.idType}!")

private def checkReferenceValue(entity: Entity[_, _, _], refTypeDef: AbstractEntityType[_, _, _], idValue: EntityId[_, _]): Unit =
    refTypeDef match
        case refSuperTypeDef: EntitySuperType[_, _, _] =>
            if (!entity.typeDefinition.isChildOfRaw(refSuperTypeDef))
                throw new ConsistencyException(s"Reference value type ${entity.typeDefinition.typeDefinition} is not parent of " +
                    s"provided type ${refSuperTypeDef.typeDefinition}!")
        case refEntityType: EntityType[_, _, _] =>
            if (entity.typeDefinition.getId != refEntityType.getId)
                throw new ConsistencyException(s"Reference value type ${entity.typeDefinition.typeDefinition} does not match " +
                    s"provided type ${refEntityType.typeDefinition}!")
    if entity.id != idValue then
        throw new ConsistencyException(s"Reference value id ${entity.id} does not match provided id $idValue!")

//private def isParentOf(parent: AbstractEntityType, child: EntityType[_, _]): Boolean =
//    parent match
//        case entityType: EntityType[_, _] => entityType.getId == child.getId
//        case superType: EntitySuperType =>
//            child.valueType.parent.fo match
//                case childEntityType: EntityTypeDefinition => childEntityType.parent.exists(_parent => _parent == parent || isParentOf(parent, _parent))
//                    isParentOf(parent, childEntityType)


