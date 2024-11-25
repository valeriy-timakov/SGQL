package my.valerii_timakov.sgql.entity.domain.type_definitions


import akka.parboiled2.util.Base64
import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.type_definitions.LongTypeDefinition.name
import my.valerii_timakov.sgql.entity.{IdParseError, ValueParseError}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, BackReferenceValue, BinaryValue, BooleanValue, ByteId, ByteValue, CustomPrimitiveValue, DateTimeValue, DateValue, DecimalValue, DoubleValue, Entity, EntityId, EntityValue, FixedStringId, FixedStringValue, FloatValue, IntId, IntValue, ItemValue, LongId, LongValue, ObjectValue, ReferenceValue, RootPrimitiveValue, ShortIntId, ShortIntValue, SimpleObjectValue, StringId, StringValue, TimeValue, UUIDId, UUIDValue, ValueTypes}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, TypeReinitializationException, WrongStateExcetion}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec
import scala.util.boundary
import boundary.break

private class JsonSerializationData(
                                       val timeFormat: DateTimeFormatter,
                                       val dateFormat: DateTimeFormatter,
                                       val dateTimeFormat: DateTimeFormatter,
                                   ):
    def parseTime(value: String): LocalTime = LocalTime.parse(value, timeFormat)
    def serialize(value: LocalTime): String = value.format(timeFormat)
    def parseDate(value: String): LocalDate = LocalDate.parse(value, dateFormat)
    def serialize(value: LocalDate): String = value.format(dateFormat)
    def parseDateTime(value: String): LocalDateTime = LocalDateTime.parse(value, dateTimeFormat)
    def serialize(value: LocalDateTime): String = value.format(dateTimeFormat)

object GlobalSerializationData:
    private var _json: Option[JsonSerializationData] = None
    def initJson(conf: Config): Unit =
        _json = Some(new JsonSerializationData(
            DateTimeFormatter.ofPattern(conf.getString("date-format")),
            DateTimeFormatter.ofPattern(conf.getString("time-format")),
            DateTimeFormatter.ofPattern(conf.getString("date-time-format")),
        ))
    def json: JsonSerializationData = _json.getOrElse(throw new WrongStateExcetion("Formats not initialized!"))


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
    def valueType: FieldValueTypeDefinition[?]

sealed trait ItemValueType extends FieldValueType:
    def valueType: ItemValueTypeDefinition[?]
    def name: String = valueType.name
    
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

case class RootPrimitiveType[T, V <: RootPrimitiveValue[T, V]](valueType: RootPrimitiveTypeDefinition[V]) extends AbstractNamedType, ItemValueType

sealed abstract class AbstractEntityType extends AbstractNamedType:
    override def valueType: EntityTypeDefinition[?, ?]

sealed trait AbstractObjectEntityType extends AbstractEntityType:
    def valueType: ObjectTypeDefinition

sealed abstract class EntityType[VT <: Entity[VT, V], V <: ValueTypes] extends AbstractEntityType:
    override def valueType: EntityTypeDefinition[VT, V]
    def createEntity(id: EntityId[?, ?], value: V): VT
//    def toJson(value: V): JsValue
//    def parseValue(data: JsValue): Either[ValueParseError, V]
//    def toJson(entity: VT): JsValue = JsObject(
//        "type" -> JsString(name),
//        "typeId" -> id.map(id => JsNumber(id)).getOrElse(JsNull),
//        "id" -> entity.id.typeDefinition.toJson(entity.id),
//        "value" -> toJson(entity.value),
//    )

case class CustomPrimitiveEntityType(
    name: String,
    valueType: CustomPrimitiveTypeDefinition,
) extends EntityType[CustomPrimitiveValue, RootPrimitiveValue[?, ?]]:
    def createEntity(id: EntityId[?, ?], value: RootPrimitiveValue[?, ?]): CustomPrimitiveValue =
        CustomPrimitiveValue(id, value, this)

case class ArrayEntityType(
    name: String,
    valueType: ArrayTypeDefinition,
) extends EntityType[ArrayValue, Seq[ItemValue]]:
    def createEntity(id: EntityId[?, ?], value: Seq[ItemValue]): ArrayValue =
        ArrayValue(id, value, this)

case class ObjectEntityType(
    name: String,
    valueType: ObjectTypeDefinition,
) extends EntityType[ObjectValue, Map[String, EntityValue]], AbstractObjectEntityType:
    def createEntity(id: EntityId[?, ?], value: Map[String, EntityValue]): ObjectValue =
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
) extends EntitySuperType, AbstractObjectEntityType

sealed abstract class AbstractEntityIdTypeDefinition[V <: EntityId[?, V]]:
    def name: String


sealed abstract class EntityIdTypeDefinition[V <: EntityId[?, V]](val name: String) extends AbstractEntityIdTypeDefinition[V]:
    def parse(value: String): Either[IdParseError, V] = {
        try {
            Right(parseInner(value))
        } catch {
            case _: Throwable => Left(new IdParseError(this.getClass.getSimpleName, value))
        }
    }
    def toJson(value: V): JsValue
    def parse(value: JsValue): Either[IdParseError, V]
    protected def parseInner(value: String): V

case object ByteIdTypeDefinition extends EntityIdTypeDefinition[ByteId]("Byte"):
    def toJson(value: ByteId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[IdParseError, ByteId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => ByteId(v.byteValue),
            rootCause => Left(new IdParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): ByteId = ByteId(value.toByte)

case object ShortIdTypeDefinition extends EntityIdTypeDefinition[ShortIntId]("Short"):
    def toJson(value: ShortIntId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[IdParseError, ShortIntId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => ShortIntId(v.byteValue),
            rootCause => Left(new IdParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): ShortIntId = ShortIntId(value.toShort)

case object IntIdTypeDefinition extends EntityIdTypeDefinition[IntId]("Integer"):
    def toJson(value: IntId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[IdParseError, IntId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => IntId(v.byteValue),
            rootCause => Left(new IdParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): IntId = IntId(value.toInt)

case object LongIdTypeDefinition extends EntityIdTypeDefinition[LongId]("Long"):
    def toJson(value: LongId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[IdParseError, LongId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => LongId(v.byteValue),
            rootCause => Left(new IdParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): LongId = LongId(value.toLong)

case object UUIDIdTypeDefinition extends EntityIdTypeDefinition[UUIDId]("UUID"):
    def toJson(value: UUIDId): JsValue = JsString(value.value.toString)
    def parse(value: JsValue): Either[IdParseError, UUIDId] =
        parseFormattedString[UUID, UUIDId, IdParseError](value,  UUID.fromString, UUIDId.apply,
            rootCause => Left(new IdParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): UUIDId = UUIDId(java.util.UUID.fromString(value))

case object StringIdTypeDefinition extends EntityIdTypeDefinition[StringId]("String"):
    def toJson(value: StringId): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[IdParseError, StringId] =
        parseString(value, rootCause => Left(new IdParseError(name, value.toString, rootCause))).map(StringId.apply)
    protected override def parseInner(value: String): StringId = StringId(value)

case class FixedStringIdTypeDefinition(length: Int) extends EntityIdTypeDefinition[FixedStringId](FixedStringIdTypeDefinition.name):
    def toJson(value: FixedStringId): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[IdParseError, FixedStringId] =
        parseString(value, rootCause => Left(new IdParseError(name, value.toString, rootCause))).map(FixedStringId(_, this))
    protected override def parseInner(value: String): FixedStringId = FixedStringId(value, this)

case object FixedStringIdTypeDefinition extends AbstractEntityIdTypeDefinition[FixedStringId]:
    val name = "FixedString"

sealed trait AbstractTypeDefinition

sealed trait FieldValueTypeDefinition[V <: EntityValue] extends AbstractTypeDefinition:
    def toJson(value: V): JsValue
    def parse(value: JsValue): Either[ValueParseError, V]

sealed trait ItemValueTypeDefinition[V <: ItemValue] extends FieldValueTypeDefinition[V]:
    def name: String

sealed trait ReferenceDefinition[V <: EntityValue] extends FieldValueTypeDefinition[V]:
    def idType: AbstractEntityIdTypeDefinition[?]
    def referencedType: AbstractEntityType
    def name: String

final case class TypeReferenceDefinition(
                                            referencedType: AbstractEntityType,
                                        ) extends ItemValueTypeDefinition[ReferenceValue], ReferenceDefinition[ReferenceValue]:
    lazy val idType: AbstractEntityIdTypeDefinition[?] = referencedType.valueType.idType
    override def name: String = referencedType.name
    def toJson(value: ReferenceValue): JsValue =
        JsObject(
            "refId" -> value.refId.toJson,
            "type" -> JsString(referencedType.name),
            "value" -> value.refValueOpt.map(v => v.toJson).getOrElse(JsNull),
        )
    def parse(value: JsValue): Either[ValueParseError, ReferenceValue] =
        value match
            case JsObject(fields) =>
                fields.get("refId") match
                    case Some(_refId) =>
                        val refId = idType.parse(_refId)
                        val res = ReferenceValue(refId, this)
                        val refValue = fields.get("value")
                            .map(v => referencedType.valueType.parseValue(v))
                            .foreach(v => res.setRefValue(v)
                        Right(res)
                    case None =>
                        Left(new ValueParseError(name, value.toString, "no refId"))
            case _ =>
                Left(new ValueParseError(name, value.toString, "wrong reference format"))
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}])"

final case class TypeBackReferenceDefinition(
                                                //reference to abstract type to make it possible to reference to any concrete nested type
                                                referencedType: AbstractObjectEntityType,
                                                refField: String
                                            ) extends FieldValueTypeDefinition[BackReferenceValue], ReferenceDefinition[BackReferenceValue]:
    lazy val idType: AbstractEntityIdTypeDefinition[?] = referencedType.valueType.idType
    override def name: String = referencedType.name + "." + refField + "[]"
    def toJson(value: BackReferenceValue): JsValue = ???
    def parse(value: JsValue): Either[ValueParseError, BackReferenceValue] = ???
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}], $refField)"


sealed abstract trait AbstractRootPrimitiveTypeDefinition:
    def name: String

sealed abstract case class RootPrimitiveTypeDefinition[V <: RootPrimitiveValue[?, V]](name: String)
            extends AbstractRootPrimitiveTypeDefinition,  ItemValueTypeDefinition[V]:
    def parse(value: String): Either[ValueParseError, V] =
        try {
            Right(parseInner(value))
        } catch {
            case e: Exception => Left(new ValueParseError(this.getClass.getSimpleName, value, e.getMessage))
        }
    def parse(value: JsValue): Either[ValueParseError, V]
    protected def parseInner(value: String): V

object ByteTypeDefinition extends RootPrimitiveTypeDefinition[ByteValue]("Byte"):
    override def toJson(value: ByteValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, ByteValue] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => ByteValue(v.byteValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): ByteValue = ByteValue(value.toByte)

object ShortIntTypeDefinition extends RootPrimitiveTypeDefinition[ShortIntValue]("Short"):
    override def toJson(value: ShortIntValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, ShortIntValue] =
        parseWholeNumber(value, v => v >= Short.MinValue && v <= Short.MaxValue, v => ShortIntValue(v.shortValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)) )
    protected override def parseInner(value: String): ShortIntValue = ShortIntValue(value.toShort)

object IntTypeDefinition extends RootPrimitiveTypeDefinition[IntValue]("Integer"):
    override def toJson(value: IntValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, IntValue] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => IntValue(v.intValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)) )
    protected override def parseInner(value: String): IntValue = IntValue(value.toInt)

object LongTypeDefinition extends RootPrimitiveTypeDefinition[LongValue]("Long"):
    override def toJson(value: LongValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, LongValue] =
        parseWholeNumber(value, v => true, v => LongValue(v),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)) )
    protected override def parseInner(value: String): LongValue = LongValue(value.toLong)

object DoubleTypeDefinition extends RootPrimitiveTypeDefinition[DoubleValue]("Double"):
    override def toJson(value: DoubleValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, DoubleValue] =
        parseDecimalNumber(value, v => v.isDecimalDouble, v => DoubleValue(v.doubleValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): DoubleValue = DoubleValue(value.toDouble)

object FloatTypeDefinition extends RootPrimitiveTypeDefinition[FloatValue]("Float"):
    override def toJson(value: FloatValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, FloatValue] =
        parseDecimalNumber(value, v => v.isDecimalFloat, v => FloatValue(v.floatValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): FloatValue = FloatValue(value.toFloat)

object DecimalTypeDefinition extends RootPrimitiveTypeDefinition[DecimalValue]("Decimal"):
    override def toJson(value: DecimalValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, DecimalValue] =
        parseDecimalNumber(value, v => true, v => DecimalValue(v),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): DecimalValue = DecimalValue(BigDecimal(value))

object BooleanTypeDefinition extends RootPrimitiveTypeDefinition[BooleanValue]("Boolean"):
    override def toJson(value: BooleanValue): JsValue = JsBoolean(value.value)
    def parse(value: JsValue): Either[ValueParseError, BooleanValue] =
        value match
            case JsBoolean(bool) => Right(BooleanValue(bool))
            case _ => Left(new ValueParseError(this.getClass.getSimpleName, value.toString))
    protected override def parseInner(value: String): BooleanValue = BooleanValue(value.toBoolean)

object DateTypeDefinition extends RootPrimitiveTypeDefinition[DateValue]("Date"):
    override def toJson(value: DateValue): JsValue = JsString(GlobalSerializationData.json.serialize(value.value))
    def parse(value: JsValue): Either[ValueParseError, DateValue] =
        parseFormattedString[LocalDate, DateValue, ValueParseError](value, GlobalSerializationData.json.parseDate,
            DateValue.apply, rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): DateValue = DateValue(LocalDate.parse(value))

object DateTimeTypeDefinition extends RootPrimitiveTypeDefinition[DateTimeValue]("DateTime"):
    override def toJson(value: DateTimeValue): JsValue = JsString(GlobalSerializationData.json.serialize(value.value))
    def parse(value: JsValue): Either[ValueParseError, DateTimeValue] =
        parseFormattedString[LocalDateTime, DateTimeValue, ValueParseError](value, GlobalSerializationData.json.parseDateTime,
            DateTimeValue.apply, rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): DateTimeValue = DateTimeValue(LocalDateTime.parse(value))

object TimeTypeDefinition extends RootPrimitiveTypeDefinition[TimeValue]("Time"):
    override def toJson(value: TimeValue): JsValue = JsString(GlobalSerializationData.json.serialize(value.value))
    def parse(value: JsValue): Either[ValueParseError, TimeValue] =
        parseFormattedString[LocalTime, TimeValue, ValueParseError](value, GlobalSerializationData.json.parseTime, 
            TimeValue.apply, rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): TimeValue = TimeValue(LocalTime.parse(value))

object UUIDTypeDefinition extends RootPrimitiveTypeDefinition[UUIDValue]("UUID"):
    override def toJson(value: UUIDValue): JsValue = JsString(value.value.toString)
    def parse(value: JsValue): Either[ValueParseError, UUIDValue] =
        parseFormattedString[UUID, UUIDValue, ValueParseError](value, UUID.fromString, UUIDValue.apply,
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): UUIDValue = UUIDValue(java.util.UUID.fromString(value))

object BinaryTypeDefinition extends RootPrimitiveTypeDefinition[BinaryValue]("Binary"):
    override def toJson(value: BinaryValue): JsValue = JsString(Base64.rfc2045().encodeToString(value.value, false))
    def parse(value: JsValue): Either[ValueParseError, BinaryValue] =
        parseFormattedString[Array[Byte], BinaryValue, ValueParseError](value, Base64.rfc2045().decode, BinaryValue.apply,
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): BinaryValue = BinaryValue(Base64.rfc2045().decode(value))

object StringTypeDefinition extends RootPrimitiveTypeDefinition[StringValue]("String"):
    override def toJson(value: StringValue): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[ValueParseError, StringValue] =
        parseString(value, rootCause => Left(new ValueParseError(name, value.toString, rootCause))).map(StringValue.apply)
    protected override def parseInner(value: String): StringValue = StringValue(value)

class FixedStringTypeDefinition(val length: Int)
        extends RootPrimitiveTypeDefinition[FixedStringValue](FixedStringTypeDefinition.name):
    override def toJson(value: FixedStringValue): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[ValueParseError, FixedStringValue] =
        parseString(value, rootCause => Left(new ValueParseError(name, value.toString, rootCause))).map(FixedStringValue(_, this))
    protected override def parseInner(value: String): FixedStringValue = FixedStringValue(value, this)

object FixedStringTypeDefinition extends AbstractRootPrimitiveTypeDefinition:
    val name = "FixedString"

trait FieldsContainer:
    def fields: Map[String, FieldTypeDefinition[?]]
    def allFields: Map[String, FieldTypeDefinition[?]]
    def idTypeOpt: Option[AbstractEntityIdTypeDefinition[?]]

private def parseWholeNumber[V <: RootPrimitiveValue[?, ?] | EntityId[?, ?], E <: ValueParseError | IdParseError](
                                                            value: JsValue,
                                                            rangeCheker: Long => Boolean,
                                                            valueWrapper: Long => V,
                                                            errorGenerator: String => Left[E, V],
                                                        ): Either[E, V] =
    value match
        case JsNumber(valueInner) =>
            if (valueInner.isWhole)
                wrappNumberInRange(valueInner.toLong, rangeCheker, valueWrapper, errorGenerator)
            else
                errorGenerator(" not a whole number")
        case _ =>
            errorGenerator(" not a number")

private def parseDecimalNumber[V <: RootPrimitiveValue[?, ?], E <: ValueParseError | IdParseError](
                                                            value: JsValue,
                                                            rangeCheker: BigDecimal => Boolean,
                                                            valueWrapper: BigDecimal => V,
                                                            errorGenerator: String => Left[E, V],
                                                             ): Either[E, V] =
    value match
        case JsNumber(valueInner) =>
            wrappNumberInRange(valueInner, rangeCheker, valueWrapper, errorGenerator)
        case _ =>
            errorGenerator("not a number")

private def wrappNumberInRange[P, V <: RootPrimitiveValue[?, ?] | EntityId[?, ?], E <: ValueParseError | IdParseError](
                                                  value: P,
                                                  rangeCheker: P => Boolean,
                                                  valueWrapper: P => V,
                                                  errorGenerator: String => Left[E, V],
                                             ): Either[E, V] =
        if (rangeCheker(value))
            Right(valueWrapper(value))
        else
            errorGenerator("out of range")

private def parseString[E <: ValueParseError | IdParseError](
                                                    value: JsValue,
                                                    errorGenerator: String => Left[E, String]
                                                        ): Either[E, String] =
    value match
        case JsString(data) => Right(data)
        case _ => errorGenerator("not a JSON string")

private def parseFormattedString[T, V <: RootPrimitiveValue[?, ?] | EntityId[?, ?], E <: ValueParseError | IdParseError](
                                                        value: JsValue,
                                                        exceptionalParser: String => T,
                                                        valueWrapper: T => V,
                                                        errorGenerator: String => Left[E, T],
                                                    ): Either[E, V] =
    parseString(value, errorGenerator.asInstanceOf[String => Left[E, String]])
        .map(data => wrappExceptionalParser(exceptionalParser)(data, errorGenerator))
        .flatMap(identity)
        .map(valueWrapper)


private def wrappExceptionalParser[T, E <: ValueParseError | IdParseError](parser: String => T): 
                        (String, String => Left[E, T]) => Either[E, T] =
    (value: String, errorGenerator: String => Left[E, T]) =>
        try {
            Right(parser(value))
        } catch {
            case e: Exception => errorGenerator(s"not a UUID: ${e.getMessage}")
        }


final case class SimpleObjectTypeDefinition(
                                               private var _fields: Map[String, FieldTypeDefinition[?]],
                                               parent: Option[ObjectEntitySuperType]
                                           ) extends FieldValueTypeDefinition[SimpleObjectValue], FieldsContainer:
    private var initiated = false
    def fields: Map[String, FieldTypeDefinition[?]] = _fields
    def setChildren(fieldsValues: Map[String, FieldTypeDefinition[?]]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldTypeDefinition[?]] =
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldTypeDefinition[?]])
    override def toString: String = parent.map(_.name).getOrElse("") + "{" +
        fields.map(f => s"${f._1}: ${f._2}").mkString(", ") + "}"
    def idTypeOpt: Option[AbstractEntityIdTypeDefinition[?]] = parent.map(_.valueType.idType)
    def toJson(value: SimpleObjectValue): JsValue = ???
    def parse(value: JsValue): Either[ValueParseError, SimpleObjectValue] = ???

sealed trait EntityTypeDefinition[VT <: Entity[VT, V], V <: ValueTypes] extends AbstractTypeDefinition:
    def idType: EntityIdTypeDefinition[?]
    def parent: Option[EntitySuperType]
    def parseValue(data: JsValue): Either[ValueParseError, V]
    def toJson(value: V): JsValue

final case class CustomPrimitiveTypeDefinition(
    parentNode: Either[(EntityIdTypeDefinition[?], RootPrimitiveTypeDefinition[?]), PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]]
) extends EntityTypeDefinition[CustomPrimitiveValue, RootPrimitiveValue[?, ?]]:
    @tailrec def rootType: RootPrimitiveTypeDefinition[?] = this.parentNode match
        case Left((_, root)) => root
        case Right(parent) => parent.valueType.rootType
    lazy val idType: EntityIdTypeDefinition[?] = parentNode.fold(_._1, _.valueType.idType)
    lazy val parent: Option[PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]] = parentNode.toOption

    def toJson(value: RootPrimitiveValue[?, ?]): JsValue = 
        value.toJson
    def parseValue(data: JsValue): Either[ValueParseError, RootPrimitiveValue[?, ?]] =
        rootType.parse(data)

object ArrayTypeDefinition:
    val name = "Array"

final case class ArrayTypeDefinition(
    private var _elementTypes: Option[Set[ArrayItemTypeDefinition]],
    idOrParent: Either[EntityIdTypeDefinition[?], ArrayEntitySuperType]
) extends EntityTypeDefinition[ArrayValue, Seq[ItemValue]]:
    def elementTypes: Set[ArrayItemTypeDefinition] = _elementTypes
        .getOrElse(throw new WrongStateExcetion("Array element types not initialized!"))
    def setChildren(elementTypesValues: Set[ArrayItemTypeDefinition]): Unit =
        if (_elementTypes.nonEmpty) throw new TypeReinitializationException
        _elementTypes = Some(elementTypesValues)
    lazy val allElementTypes: Map[String, ArrayItemTypeDefinition] =
        elementTypes.map(v => v.name -> v).toMap ++ parent.map(_.valueType.allElementTypes).getOrElse(Map.empty[String, ArrayItemTypeDefinition])

    lazy val idType: EntityIdTypeDefinition[?] = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ArrayEntitySuperType] = idOrParent.toOption

    def toJson(value: Seq[ItemValue]): JsValue =
        if (allElementTypes.size == 1)
            val onlyTypeDef = allElementTypes.head._2
            JsArray(value.map(v => v.toJson).toVector)
        else
            JsArray(value.map(v =>
                JsObject(
                    "type" -> JsString(v.typeDefinition.name),
                    "value" -> v.toJson
                )
            ).toVector)
            
    def parseValue(data: JsValue): Either[ValueParseError, Seq[ItemValue]] =
        data match
            case JsArray(items) =>
                boundary {
                    if (allElementTypes.size == 1)
                        val onlyTypeDef = allElementTypes.head._2
                        Right(items.map(item =>
                            onlyTypeDef.valueType.parse(item) match
                                case Right(value) =>
                                    value.asInstanceOf[ItemValue]
                                case Left(error) =>
                                    break( Left(ValueParseError("Array", item.toString, error.message)) )
                        ))
                    else
                        Right(items.map {
                            case item@JsObject(fields) =>
                                (fields.get("type"), fields.get("value")) match
                                    case (Some(JsString(typeName)), Some(value: JsValue)) =>
                                        allElementTypes.get(typeName) match
                                            case Some(typeDef) =>
                                                typeDef.valueType.parse(value) match
                                                    case Right(parsedValue) =>
                                                        parsedValue.asInstanceOf[ItemValue]
                                                    case Left(error) =>
                                                        break( Left(ValueParseError("Array", item.toString, error.message)) )
                                            case None =>
                                                break( Left(ValueParseError("Array", item.toString, s"Type $typeName not found")) )
                                    case _ =>
                                        break( Left(ValueParseError("Array", item.toString, "not a described array item (fields not found)")) )
                            case item =>
                                break( Left(ValueParseError("Array", item.toString, "not a described array item (not an object)")) )
                        })
                }
            case _ => Left(new ValueParseError("Array", data.toString, " not an array"))

case class ArrayItemTypeDefinition(valueType: ItemValueTypeDefinition[?]):
    def name: String = valueType.name
    override def toString: String = name

case class FieldTypeDefinition[V <: EntityValue](valueType: FieldValueTypeDefinition[V], isNullable: Boolean)

object ObjectTypeDefinition:
    val name = "Object"



final case class ObjectTypeDefinition(
    private var _fields: Map[String, FieldTypeDefinition[?]],
    idOrParent: Either[EntityIdTypeDefinition[?], ObjectEntitySuperType]
) extends EntityTypeDefinition[ObjectValue, Map[String, EntityValue]], FieldsContainer:
    private var initiated = false
    def fields: Map[String, FieldTypeDefinition[?]] = _fields
    def setChildren(fieldsValues: Map[String, FieldTypeDefinition[?]]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldTypeDefinition[?]] =
        _fields ++ parent.map(_.valueType.allFields).getOrElse(Map.empty[String, FieldTypeDefinition[?]])
    lazy val idType: EntityIdTypeDefinition[?] = idOrParent.fold(identity, _.valueType.idType)
    lazy val parent: Option[ObjectEntitySuperType] = idOrParent.toOption
    def idTypeOpt: Option[AbstractEntityIdTypeDefinition[?]] = Some(idType)

    def toJson(value: Map[String, EntityValue]): JsValue = JsObject(value.map {
        case (fieldName, fieldValue) => fieldName -> fieldValue.toJson
    })
    
    def parseValue(data: JsValue): Either[ValueParseError, Map[String, EntityValue]] =
        data match
            case JsObject(fields) =>
                boundary {
                    Right(fields.map {
                        case (fieldName, fieldValue) =>
                            this.fields.get(fieldName) match
                                case Some(fieldDef) =>
                                    fieldDef.valueType.parse(fieldValue) match
                                        case Right(parsedValue) =>
                                            (fieldName, parsedValue)
                                        case Left(error) =>
                                            break( Left(ValueParseError("Object", data.toString, error.message)) )
                                case None =>
                                    break( Left(ValueParseError("Object", data.toString, s"Field $fieldName not found")) )
                    })
                }
            case _ => Left(new ValueParseError("Object", data.toString, " not an JSON object"))



val idTypesMap = Map[String, AbstractEntityIdTypeDefinition[?]](
    ByteIdTypeDefinition.name -> ByteIdTypeDefinition,
    ShortIdTypeDefinition.name -> ShortIdTypeDefinition,
    IntIdTypeDefinition.name -> IntIdTypeDefinition,
    LongIdTypeDefinition.name -> LongIdTypeDefinition,
    UUIDIdTypeDefinition.name -> UUIDIdTypeDefinition,
    StringIdTypeDefinition.name -> StringIdTypeDefinition,
    FixedStringIdTypeDefinition.name -> FixedStringIdTypeDefinition,
)

val primitiveFieldTypesMap: Map[String, AbstractRootPrimitiveTypeDefinition] = Map(
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

