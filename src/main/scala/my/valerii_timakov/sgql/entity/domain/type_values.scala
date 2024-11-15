package my.valerii_timakov.sgql.entity.domain.type_values

import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractNamedEntityType, AbstractTypeDefinition, ArrayItemValueTypeDefinitions, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, Entity, EntityIdTypeDefinition, EntityType, FieldValueTypeDefinitions, FieldsContainer, FixedStringIdTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, ShortIdTypeDefinition, ShortIntTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, TypeReinitializationException}

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec

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

//private def isParentOf(parent: AbstractNamedEntityType, child: AbstractTypeDefinition): Boolean =
//    parent match
//        case EntityType(name, _) => name == child.name
//        case superType: NamedEntitySuperType =>
//            child match
//                case childEntityType: EntityTypeDefinition => childEntityType.parent.exists(_parent => _parent == parent || isParentOf(parent, _parent))
//                    isParentOf(parent, childEntityType)


