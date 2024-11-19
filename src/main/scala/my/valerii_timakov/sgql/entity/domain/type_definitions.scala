package my.valerii_timakov.sgql.entity.domain.type_definitions


import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity.IdParseError
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayItemValue, ArrayValue, BinaryValue, BooleanValue, ByteId, ByteValue, CustomPrimitiveValue, DateTimeValue, DateValue, DecimalValue, DoubleValue, Entity, EntityId, EntityValue, FieldValue, FilledEntityValue, FloatValue, IntId, IntValue, LongId, LongValue, ObjectValue, RootPrimitiveValue, ShortIntId, ShortIntValue, StringId, StringValue, TimeValue, UUIDId, UUIDValue}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, TypeReinitializationException, WrongStateExcetion}

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec

private class TypesMaps(
                           val runVersion: Short,
                           val byNameMap: java.util.Map[String, AbstractNamedType] = new java.util.HashMap(),
                           val byIdMap: java.util.Map[Long, AbstractNamedType] = new java.util.HashMap(),
)

object TypesMap:
    private var maps: Option[TypesMaps] = None
    private final val VERSION_SHIFT = 64-16

    def init(types: Seq[AbstractNamedType], version: Short): Unit =
        if maps.nonEmpty then throw new ConsistencyException("Types already initialized!")
        maps = Some(new TypesMaps(version, 
            new java.util.HashMap(types.size),
            new java.util.HashMap(types.size)
        ))
        addTypes(types)
        
    private def byNameMap: java.util.Map[String, AbstractNamedType] = maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!")).byNameMap
    private def byIdMap: java.util.Map[Long, AbstractNamedType] = maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!")).byIdMap
    private def runVersion: Short = maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!")).runVersion

    def getTypeByName(name: String): Option[AbstractNamedType] = Option(byNameMap.get(name))
    def getTypeById(id: Long): Option[AbstractNamedType] = Option(byIdMap.get(id))
    def getAllTypes: Set[AbstractNamedType] = byNameMap.values().toArray(Array.empty[AbstractNamedType]).toSet
    
    def addTypes(types: Seq[AbstractNamedType]): Unit =
        types.foreach(tmpType => {
            if byNameMap.containsKey(tmpType.name) then throw new ConsistencyException(s"Type ${tmpType.name} already exists!")
            val id = (byIdMap.size + 1) | (runVersion.toLong << VERSION_SHIFT)
            tmpType.initId(id)
            byNameMap.put(tmpType.name, tmpType)
            byIdMap.put(id, tmpType)
        })


sealed trait AbstractType:
    def valueType: AbstractTypeDefinition
    
sealed trait ValueType extends AbstractType
    
case class SimpleObjectType(valueType: SimpleObjectTypeDefinition) extends ValueType

sealed abstract class AbstractNamedType extends AbstractType:
    private var id: Long = 0
    def name: String
    private[type_definitions] def initId(id: Long): Unit =
        if (this.id != 0) throw new ConsistencyException(s"Type $name already has id ${this.id}, when trying to set $id!")
        this.id = id

case class RootPrimitiveType(valueType: RootPrimitiveTypeDefinition) extends AbstractNamedType, ValueType:
    def name: String = valueType.name
    
case class ReferenceType(valueType: TypeReferenceDefinition) extends ValueType

case class BackReferenceType(valueType: TypeBackReferenceDefinition) extends ValueType

sealed abstract class AbstractEntityType extends AbstractNamedType:
    override def valueType: EntityTypeDefinition[?, ?, ?]

case class EntityType[VT <: Entity, V, D <: EntityTypeDefinition[VT, V, D]](
    name: String,
    valueType: D,
) extends AbstractEntityType:
    def createEntity(id: EntityId, value: V): VT = valueType.createEntity(id, value, this)

trait EntitySuperType extends AbstractEntityType:
    def name: String
    def valueType: EntityTypeDefinition[?, ?, ?]

case class PrimitiveEntitySuperType[+TD <: CustomPrimitiveTypeDefinition](
    name: String,
    valueType: TD,
) extends EntitySuperType

case class ArrayEntitySuperType(
    name: String,
    valueType: ArrayTypeDefinition,
) extends EntitySuperType

case class ObjectEntitySuperType(
    name: String,
    valueType: ObjectTypeDefinition,
) extends EntitySuperType

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
case class FixedStringIdTypeDefinition(length: Int) extends EntityIdTypeDefinition(FixedStringIdTypeDefinition.name):
    protected override def parseIner(value: String): EntityId = StringId(value)
case object FixedStringIdTypeDefinition extends EntityIdTypeDefinition("FixedString"):
    protected override def parseIner(value: String): EntityId = StringId(value)

sealed trait AbstractTypeDefinition

sealed trait FieldValueTypeDefinition extends AbstractTypeDefinition

sealed trait ItemValueTypeDefinition extends FieldValueTypeDefinition

sealed abstract class PrimitiveTypeDefinition extends AbstractTypeDefinition:
    def parse(value: String): Either[IdParseError, FilledEntityValue]
    def rootType: RootPrimitiveTypeDefinition

sealed abstract case class RootPrimitiveTypeDefinition(name: String) extends PrimitiveTypeDefinition, ItemValueTypeDefinition:
    def rootType: RootPrimitiveTypeDefinition = this
    def parse(value: String): Either[IdParseError, FilledEntityValue] =
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    protected def parseIner(value: String): FilledEntityValue

object ByteTypeDefinition extends RootPrimitiveTypeDefinition("Byte"):
    protected override def parseIner(value: String): ByteValue = ByteValue(value.toByte)
object ShortIntTypeDefinition extends RootPrimitiveTypeDefinition("Short"):
    protected override def parseIner(value: String): ShortIntValue = ShortIntValue(value.toShort)
object IntTypeDefinition extends RootPrimitiveTypeDefinition("Integer"):
    protected override def parseIner(value: String): IntValue = IntValue(value.toInt)
object LongTypeDefinition extends RootPrimitiveTypeDefinition("Long"):
    protected override def parseIner(value: String): LongValue = LongValue(value.toLong)
object DoubleTypeDefinition extends RootPrimitiveTypeDefinition("Double"):
    protected override def parseIner(value: String): DoubleValue = DoubleValue(value.toDouble)
object FloatTypeDefinition extends RootPrimitiveTypeDefinition("Float"):
    protected override def parseIner(value: String): FloatValue = FloatValue(value.toFloat)
object BooleanTypeDefinition extends RootPrimitiveTypeDefinition("Boolean"):
    protected override def parseIner(value: String): BooleanValue = BooleanValue(value.toBoolean)
object DateTypeDefinition extends RootPrimitiveTypeDefinition("Date"):
    protected override def parseIner(value: String): DateValue = DateValue(LocalDate.parse(value))
object DateTimeTypeDefinition extends RootPrimitiveTypeDefinition("DateTime"):
    protected override def parseIner(value: String): DateTimeValue = DateTimeValue(LocalTime.parse(value))
object TimeTypeDefinition extends RootPrimitiveTypeDefinition("Time"):
    protected override def parseIner(value: String): TimeValue = TimeValue(LocalDateTime.parse(value))
object UUIDTypeDefinition extends RootPrimitiveTypeDefinition("UUID"):
    protected override def parseIner(value: String): UUIDValue = UUIDValue(java.util.UUID.fromString(value))
object BinaryTypeDefinition extends RootPrimitiveTypeDefinition("Binary"):
    protected override def parseIner(value: String): BinaryValue = BinaryValue(Base64.rfc2045().decode(value))
object DecimalTypeDefinition extends RootPrimitiveTypeDefinition("Decimal"):
    protected override def parseIner(value: String): DecimalValue = DecimalValue(BigDecimal(value))
object StringTypeDefinition extends RootPrimitiveTypeDefinition("String"):
    protected override def parseIner(value: String): StringValue = StringValue(value)
class FixedStringTypeDefinition(val length: Int) extends RootPrimitiveTypeDefinition(FixedStringTypeDefinition.name):
    protected override def parseIner(value: String): StringValue = StringValue(value)
object FixedStringTypeDefinition extends RootPrimitiveTypeDefinition("FixedString"):
    protected override def parseIner(value: String): StringValue = StringValue(value)

sealed trait EntityTypeDefinition[VT <: Entity, V, D <: EntityTypeDefinition[VT, V, D]] extends AbstractTypeDefinition:
    def idType: EntityIdTypeDefinition
    def parent: Option[EntitySuperType]
    private[type_definitions] def createEntity(
            id: EntityId,
            value: V,
            typeDefinition: EntityType[VT, V, D]
        ): VT

final case class CustomPrimitiveTypeDefinition(
    parentNode: Either[(EntityIdTypeDefinition, RootPrimitiveTypeDefinition), PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]]
) extends PrimitiveTypeDefinition, EntityTypeDefinition[CustomPrimitiveValue, RootPrimitiveValue[?], CustomPrimitiveTypeDefinition]:
    @tailrec def rootType: RootPrimitiveTypeDefinition = this.parentNode match
        case Left((_, root)) => root
        case Right(parent) => parent.valueType.rootType
    def parse(value: String): Either[IdParseError, FilledEntityValue] = rootType.parse(value)
    private[type_definitions] def createEntity(
                id: EntityId,
                value: RootPrimitiveValue[?],
                typeDefinition: EntityType[CustomPrimitiveValue, RootPrimitiveValue[?], CustomPrimitiveTypeDefinition]
            ): CustomPrimitiveValue =
        CustomPrimitiveValue(id, value, typeDefinition)
    lazy val idType: EntityIdTypeDefinition = parentNode.fold(_._1, _.valueType.idType)
    lazy val parent: Option[PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]] = parentNode.toOption

object ArrayTypeDefinition:
    val name = "Array"

final case class ArrayTypeDefinition(
    private var _elementTypes: Set[ArrayItemTypeDefinition],
    idOrParent: Either[EntityIdTypeDefinition, ArrayEntitySuperType]
) extends EntityTypeDefinition[ArrayValue, Seq[ArrayItemValue], ArrayTypeDefinition]:
    private var initiated = false
    def elementTypes: Set[ArrayItemTypeDefinition] = _elementTypes
    def setChildren(elementTypesValues: Set[ArrayItemTypeDefinition]): Unit =
        if (initiated) throw new TypeReinitializationException
        _elementTypes = elementTypesValues
        initiated = true
    private[type_definitions] def createEntity(
                id: EntityId, 
                value: Seq[ArrayItemValue], 
                typeDefinition: EntityType[ArrayValue, Seq[ArrayItemValue], ArrayTypeDefinition]
            ): ArrayValue = 
        ArrayValue(id, value, typeDefinition)        
    lazy val allElementTypes: Set[ArrayItemTypeDefinition] =
        _elementTypes ++ parent.map(_.valueType.allElementTypes).getOrElse(Set.empty[ArrayItemTypeDefinition])
    lazy val idType: EntityIdTypeDefinition = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ArrayEntitySuperType] = idOrParent.toOption

case class ArrayItemTypeDefinition(valueType: ItemValueTypeDefinition):
    def name: String = valueType match
        case ref: TypeReferenceDefinition => ref.referencedType.name
        case root: RootPrimitiveTypeDefinition => root.name
        case _ => throw new RuntimeException("Unexpected ArrayItemType valueType")
    override def toString: String = name

case class FieldTypeDefinition(valueType: FieldValueTypeDefinition, isNullable: Boolean)

object ObjectTypeDefinition:
    val name = "Object"

trait FieldsContainer:
    def fields: Map[String, FieldTypeDefinition]
    def allFields: Map[String, FieldTypeDefinition]


final case class ObjectTypeDefinition(
    private var _fields: Map[String, FieldTypeDefinition],
    idOrParent: Either[EntityIdTypeDefinition, ObjectEntitySuperType]
) extends EntityTypeDefinition[ObjectValue, Map[String, FieldValue], ObjectTypeDefinition], FieldsContainer:
    private var initiated = false
    def fields: Map[String, FieldTypeDefinition] = _fields
    def setChildren(fieldsValues: Map[String, FieldTypeDefinition]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    private[type_definitions] def createEntity(
                id: EntityId,
                value: Map[String, FieldValue],
                typeDefinition: EntityType[ObjectValue, Map[String, FieldValue], ObjectTypeDefinition]
            ): ObjectValue =
        ObjectValue(id, value, typeDefinition)
    lazy val allFields: Map[String, FieldTypeDefinition] =
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldTypeDefinition])
    lazy val idType: EntityIdTypeDefinition = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ObjectEntitySuperType] = idOrParent.toOption

final case class SimpleObjectTypeDefinition(
    private var _fields: Map[String, FieldTypeDefinition],
    parent: Option[ObjectEntitySuperType]
) extends FieldValueTypeDefinition, FieldsContainer:
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

final case class TypeReferenceDefinition(
                                            referencedType: AbstractEntityType,
) extends ItemValueTypeDefinition:
    lazy val idType: EntityIdTypeDefinition = referencedType.valueType.idType
    lazy val parent: Option[EntitySuperType] = None
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}])"

final case class TypeBackReferenceDefinition(
    //reference to abstract type to make it possible to reference to any concrete nested type
    referencedType: AbstractEntityType,
    refField: String
) extends FieldValueTypeDefinition:
    lazy val idType: EntityIdTypeDefinition = referencedType.valueType.idType
    lazy val parent: Option[EntitySuperType] = None
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}], $refField)"



val idTypesMap = Map(
    ByteIdTypeDefinition.name -> ByteIdTypeDefinition,
    ShortIdTypeDefinition.name -> ShortIdTypeDefinition,
    IntIdTypeDefinition.name -> IntIdTypeDefinition,
    LongIdTypeDefinition.name -> LongIdTypeDefinition,
    UUIDIdTypeDefinition.name -> UUIDIdTypeDefinition,
    StringIdTypeDefinition.name -> StringIdTypeDefinition,
    FixedStringIdTypeDefinition.name -> FixedStringIdTypeDefinition,
)

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
