package my.valerii_timakov.sgql.entity.domain.type_definitions


import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity.IdParseError
import my.valerii_timakov.sgql.entity.domain.type_values.{BinaryType, BooleanType, ByteId, ByteType, DateTimeType, DateType, DecimalType, DoubleType, EntityFieldType, EntityId, FloatType, IntId, IntType, LongId, LongType, ShortIntId, ShortIntType, StringId, StringType, TimeType, UUIDId, UUIDType}
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


 
