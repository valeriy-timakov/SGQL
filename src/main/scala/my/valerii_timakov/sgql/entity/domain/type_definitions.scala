package my.valerii_timakov.sgql.entity.domain.type_definitions


import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity.IdParseError
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, BinaryValue, BooleanValue, ByteId, ByteValue, ValueTypes, CustomPrimitiveValue, DateTimeValue, DateValue, DecimalValue, DoubleValue, Entity, EntityId, EntityValue, FloatValue, IntId, IntValue, ItemValue, LongId, LongValue, ObjectValue, RootPrimitiveValue, ShortIntId, ShortIntValue, StringId, StringValue, TimeValue, UUIDId, UUIDValue}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, TypeReinitializationException, WrongStateExcetion}
import spray.json.JsValue

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
    
sealed trait FieldValueType extends AbstractType:
    def valueType: FieldValueTypeDefinition

sealed trait ItemValueType extends FieldValueType:
    def valueType: ItemValueTypeDefinition
    
case class SimpleObjectType(valueType: SimpleObjectTypeDefinition) extends FieldValueType

case class ReferenceType(valueType: TypeReferenceDefinition) extends ItemValueType

case class BackReferenceType(valueType: TypeBackReferenceDefinition) extends FieldValueType

sealed abstract class AbstractNamedType extends AbstractType:
    private var id: Option[Long] = None
    def name: String
    private[type_definitions] def initId(id: Long): Unit =
        if (this.id.isDefined) throw new ConsistencyException(s"Type $name already has id ${this.id}, when trying to set $id!")
        this.id = Some(id)
    def getId: Long = id.getOrElse(throw new ConsistencyException(s"Type $name has no id yet!"))

case class RootPrimitiveType(valueType: RootPrimitiveTypeDefinition) extends AbstractNamedType, ItemValueType:
    def name: String = valueType.name

sealed abstract class AbstractEntityType extends AbstractNamedType:
    override def valueType: EntityTypeDefinition[?, ?]

abstract class EntityType[VT <: Entity[VT, V], V <: ValueTypes] extends AbstractEntityType:
    override def valueType: EntityTypeDefinition[VT, V]
    def createEntity(id: EntityId, value: V): VT
    def parseValue(data: JsValue): Either[IdParseError, V] = ???

case class CustomPrimitiveEntityType(
    name: String,
    valueType: CustomPrimitiveTypeDefinition,
) extends EntityType[CustomPrimitiveValue, RootPrimitiveValue[?]]:
    def createEntity(id: EntityId, value: RootPrimitiveValue[?]): CustomPrimitiveValue =
        CustomPrimitiveValue(id, value, this)

case class ArrayEntityType(
    name: String,
    valueType: ArrayTypeDefinition,
) extends EntityType[ArrayValue, Seq[ItemValue]]:
    def createEntity(id: EntityId, value: Seq[ItemValue]): ArrayValue =
        ArrayValue(id, value, this)

case class ObjectEntityType(
    name: String,
    valueType: ObjectTypeDefinition,
) extends EntityType[ObjectValue, Map[String, EntityValue]]:
    def createEntity(id: EntityId, value: Map[String, EntityValue]): ObjectValue =
        ObjectValue(id, value, this)

trait EntitySuperType extends AbstractEntityType:
    def name: String
    def valueType: EntityTypeDefinition[?, ?]

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

sealed abstract case class RootPrimitiveTypeDefinition(name: String) extends ItemValueTypeDefinition:
    def rootType: RootPrimitiveTypeDefinition = this
    def parse(value: String): Either[IdParseError, RootPrimitiveValue[?]] =
        try {
            Right(parseIner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    protected def parseIner(value: String): RootPrimitiveValue[?]

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
trait FieldsContainer:
    def fields: Map[String, FieldTypeDefinition]
    def allFields: Map[String, FieldTypeDefinition]
    def idTypeOpt: Option[EntityIdTypeDefinition]

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
    def idTypeOpt: Option[EntityIdTypeDefinition] = parent.map(_.valueType.idType)
    
sealed trait ReferenceDefinition extends FieldValueTypeDefinition:
    def idType: EntityIdTypeDefinition
    def referencedType: AbstractEntityType

final case class TypeReferenceDefinition(
                                            referencedType: AbstractEntityType,
                                        ) extends ItemValueTypeDefinition, ReferenceDefinition:
    lazy val idType: EntityIdTypeDefinition = referencedType.valueType.idType
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}])"

final case class TypeBackReferenceDefinition(
                                                //reference to abstract type to make it possible to reference to any concrete nested type
                                                referencedType: AbstractEntityType,
                                                refField: String
                                            ) extends FieldValueTypeDefinition, ReferenceDefinition:
    lazy val idType: EntityIdTypeDefinition = referencedType.valueType.idType
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}], $refField)"


sealed trait EntityTypeDefinition[VT <: Entity[VT, V], V <: ValueTypes] extends AbstractTypeDefinition:
    def idType: EntityIdTypeDefinition
    def parent: Option[EntitySuperType]

final case class CustomPrimitiveTypeDefinition(
    parentNode: Either[(EntityIdTypeDefinition, RootPrimitiveTypeDefinition), PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]]
) extends EntityTypeDefinition[CustomPrimitiveValue, RootPrimitiveValue[?]]:
    @tailrec def rootType: RootPrimitiveTypeDefinition = this.parentNode match
        case Left((_, root)) => root
        case Right(parent) => parent.valueType.rootType
    lazy val idType: EntityIdTypeDefinition = parentNode.fold(_._1, _.valueType.idType)
    lazy val parent: Option[PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]] = parentNode.toOption

object ArrayTypeDefinition:
    val name = "Array"

final case class ArrayTypeDefinition(
    private var _elementTypes: Set[ArrayItemTypeDefinition],
    idOrParent: Either[EntityIdTypeDefinition, ArrayEntitySuperType]
) extends EntityTypeDefinition[ArrayValue, Seq[ItemValue]]:
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

case class ArrayItemTypeDefinition(valueType: ItemValueTypeDefinition):
    def name: String = valueType match
        case ref: TypeReferenceDefinition => ref.referencedType.name
        case root: RootPrimitiveTypeDefinition => root.name
        case _ => throw new RuntimeException("Unexpected ArrayItemType valueType")
    override def toString: String = name

case class FieldTypeDefinition(valueType: FieldValueTypeDefinition, isNullable: Boolean)

object ObjectTypeDefinition:
    val name = "Object"



final case class ObjectTypeDefinition(
    private var _fields: Map[String, FieldTypeDefinition],
    idOrParent: Either[EntityIdTypeDefinition, ObjectEntitySuperType]
) extends EntityTypeDefinition[ObjectValue, Map[String, EntityValue]], FieldsContainer:
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
    def idTypeOpt: Option[EntityIdTypeDefinition] = Some(idType)
    


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
