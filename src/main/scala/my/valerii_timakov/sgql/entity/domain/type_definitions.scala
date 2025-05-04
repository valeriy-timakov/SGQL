package my.valerii_timakov.sgql.entity.domain.type_definitions


import akka.parboiled2.util.Base64
import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.type_definitions.LongTypeDefinition.name
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, BackReferenceValue, BinaryValue, BooleanValue, ByteId, ByteValue, CustomPrimitiveValue, DateTimeValue, DateValue, DecimalValue, DoubleValue, Entity, EntityId, EntityValue, FixedStringId, FixedStringValue, FloatValue, IntId, IntValue, ItemValue, LongId, LongValue, ObjectValue, ReferenceValue, RootPrimitiveValue, ShortIntId, ShortIntValue, SimpleObjectValue, StringId, StringValue, TimeValue, TypeValue, UUIDId, UUIDValue, ValueTypes}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, AbstractObjectEntityType, ArrayEntitySuperType, BackReferenceType, EntitySuperType, EntityType, GlobalTypesMap, ObjectEntitySuperType, PrimitiveEntitySuperType, ReferenceType, SimpleObjectType}
import my.valerii_timakov.sgql.entity.{SingleMessageError, ValueParseError}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, TypeReinitializationException, WrongStateExcetion}
import my.valerii_timakov.sgql.services.ValuePersistenceDataFinal
import scalikejdbc.WrappedResultSet
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec
import scala.util.boundary
import scala.util.boundary.break


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


sealed trait TypeDefinition:
    type ValueType = TypeValue
    def parse(value: String): Either[ValueParseError, ValueType]
    def name: String
    def isString: Boolean = false

sealed abstract class AbstractEntityIdTypeDefinition[V <: EntityId[_, V]]:
    def name: String
    def toJson: JsValue = JsString(name)


sealed abstract class EntityIdTypeDefinition[V <: EntityId[_, V]](val name: String) extends AbstractEntityIdTypeDefinition[V] with TypeDefinition:
    type ValueType = V
    def extract(rs: WrappedResultSet, pos: Int): Option[V]
    def parse(value: String): Either[ValueParseError, V] = {
        try {
            Right(parseInner(value))
        } catch {
            case _: Throwable => Left(new ValueParseError(this.getClass.getSimpleName, value))
        }
    }
    def toJson(value: V): JsValue
    def parse(value: JsValue): Either[ValueParseError, V]
    protected def parseInner(value: String): V

case object ByteIdTypeDefinition extends EntityIdTypeDefinition[ByteId]("Byte"):
    def createValue(value: Byte): ByteId = ByteId(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[ByteId] = rs.byteOpt(pos).map(createValue)
    def toJson(value: ByteId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, ByteId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => ByteId(v.byteValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): ByteId = ByteId(value.toByte)

case object ShortIdTypeDefinition extends EntityIdTypeDefinition[ShortIntId]("Short"):
    def createValue(value: Short): ShortIntId = ShortIntId(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[ShortIntId] = rs.shortOpt(pos).map(createValue)
    def toJson(value: ShortIntId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, ShortIntId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => ShortIntId(v.byteValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): ShortIntId = ShortIntId(value.toShort)

case object IntIdTypeDefinition extends EntityIdTypeDefinition[IntId]("Integer"):
    def createValue(value: Int): IntId = IntId(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[IntId] = rs.intOpt(pos).map(createValue)
    def toJson(value: IntId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, IntId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => IntId(v.byteValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): IntId = IntId(value.toInt)

case object LongIdTypeDefinition extends EntityIdTypeDefinition[LongId]("Long"):
    def createValue(value: Long): LongId = LongId(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[LongId] = rs.longOpt(pos).map(createValue)
    def toJson(value: LongId): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, LongId] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => LongId(v.byteValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): LongId = LongId(value.toLong)

case object UUIDIdTypeDefinition extends EntityIdTypeDefinition[UUIDId]("UUID"):
    def createValue(value: UUID): UUIDId = UUIDId(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[UUIDId] = rs.stringOpt(pos).map(parseInner)
    def toJson(value: UUIDId): JsValue = JsString(value.value.toString)
    def parse(value: JsValue): Either[ValueParseError, UUIDId] =
        parseFormattedString[UUID, UUIDId](value,  UUID.fromString, UUIDId.apply,
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    protected override def parseInner(value: String): UUIDId = UUIDId(java.util.UUID.fromString(value))

case object StringIdTypeDefinition extends EntityIdTypeDefinition[StringId]("String"):
    def createValue(value: String): StringId = StringId(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[StringId] = rs.stringOpt(pos).map(createValue)
    def toJson(value: StringId): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[ValueParseError, StringId] =
        parseString(value, rootCause => Left(new ValueParseError(name, value.toString, rootCause))).map(StringId.apply)
    override def isString: Boolean = true
    protected override def parseInner(value: String): StringId = StringId(value)

case class FixedStringIdTypeDefinition(length: Int) extends EntityIdTypeDefinition[FixedStringId](FixedStringIdTypeDefinition.name):
    def createValue(value: String): FixedStringId = FixedStringId(value, this)
    def extract(rs: WrappedResultSet, pos: Int): Option[FixedStringId] = rs.stringOpt(pos).map(createValue)
    def toJson(value: FixedStringId): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[ValueParseError, FixedStringId] =
        parseString(value, rootCause => Left(new ValueParseError(name, value.toString, rootCause))).map(FixedStringId(_, this))
    protected override def parseInner(value: String): FixedStringId = FixedStringId(value, this)
    override def isString: Boolean = true

case object FixedStringIdTypeDefinition extends AbstractEntityIdTypeDefinition[FixedStringId]:
    val name = "FixedString"

sealed trait AbstractTypeDefinition:
    def toJson: JsValue = this match
        case CustomPrimitiveTypeDefinition(idOrParentType) =>
            val parentName = idOrParentType.toOption.map(_.name).orElse(
                    idOrParentType.left.toOption.map(_._2.name))
                .map(JsString.apply)
                .getOrElse(JsNull)
            JsObject(
                "type" -> JsString("CustomPrimitive"),
                "parent" -> parentName,
                //                "id" -> idOrParentType.left.toOption.map(_._1.toJson).getOrElse(JsNull)
            )
        case RootPrimitiveTypeDefinition(name) => JsString(name)
        case ArrayTypeDefinition(elementsType, parentType, _) => JsObject(
            "type" -> JsString("Array"),
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "elements" -> elementsType.map(v => JsString(v.name)).orNull)
        case ObjectTypeDefinition(fields, parentType) => JsObject(
            "type" -> JsString("Object"),
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "fields" -> JsObject(fields.map((fieldName, fieldType) => fieldName -> fieldType.valueType.toJson)))
        case TypeReferenceDefinition(referencedType) => JsObject(
            "type" -> JsString("Reference"),
            "referencedType" -> JsString(referencedType.name))
        case TypeBackReferenceDefinition(referencedType, refField) => JsObject(
            "type" -> JsString("BackReference"),
            "referencedType" -> JsString(referencedType.name),
            "refField" -> JsString(refField))
        case SimpleObjectTypeDefinition(fields, parent) => JsObject(
            "type" -> JsString("SimpleObject"),
            "parent" -> parent.map(p => JsString(p.name)).getOrElse(JsNull),
            "fields" -> JsObject(fields.map((fieldName, fieldType) => fieldName -> fieldType.valueType.toJson)))



sealed trait FieldValueTypeDefinition[V <: EntityValue] extends AbstractTypeDefinition:
    def toJson(value: V): JsValue
    def parse(value: JsValue): Either[SingleMessageError, V]

sealed trait ItemValueTypeDefinition[V <: ItemValue] extends FieldValueTypeDefinition[V]:
    def name: String
    def extract(rs: WrappedResultSet, pos: Int): Option[V]

sealed trait ReferenceDefinition[ID <: EntityId[_, ID], V <: EntityValue] extends FieldValueTypeDefinition[V]:
    def idType: EntityIdTypeDefinition[ID]
    def referencedType: AbstractEntityType[ID, _, _]
    def name: String


final case class TypeReferenceDefinition[ID <: EntityId[_, ID]](
                                                         referencedType: AbstractEntityType[ID, _, _],
                                        ) extends ItemValueTypeDefinition[ReferenceValue[ID]], ReferenceDefinition[ID, ReferenceValue[ID]]:
    lazy val idType: EntityIdTypeDefinition[ID] = referencedType.typeDefinition.idType
    override def name: String = referencedType.name
    def extract(rs: WrappedResultSet, pos: Int): Option[ReferenceValue[ID]] = 
        idType.extract(rs, pos).map(ReferenceValue(_, ReferenceType(this)))
    def toJson(value: ReferenceValue[ID]): JsValue =
        JsObject(
            "refId" -> value.refId.toJson,
            "type" -> JsString(referencedType.name),
            "value" -> value.refValueOpt.map(v => v.toJson).getOrElse(JsNull),
        )

    override def equals(obj: Any): Boolean =
        obj match
            case that: TypeReferenceDefinition[ID] => 
                this.referencedType == that.referencedType
            case _ =>
                false
                
    def parse(value: JsValue): Either[my.valerii_timakov.sgql.entity.SingleMessageError, ReferenceValue[ID]] =
        value match
            case JsObject(fields) =>
                fields.get("refId") match
                    case Some(refId) =>
                        idType.parse(refId) match
                            case Right(refId) =>
                                val res = ReferenceValue[ID](refId, ReferenceType(this))
                                fields.get("value") match
                                    case Some(refValue) =>
                                        val refEntityTypeRes:  Either[ValueParseError, EntityType[ID, _, _]] = referencedType match
                                            case entityType: EntityType[ID, _, _] =>
                                                Right(entityType)
                                            case entitySuperType: EntitySuperType[ID, _, _] => fields.get("type") match
                                                case Some(JsString(valueEntityTypeName)) =>
                                                    GlobalTypesMap.getTypeByName(valueEntityTypeName) match
                                                        case Some(entityType: EntityType[ID, _, _]) =>
                                                            if (entitySuperType.hasChild(entityType))
                                                                Right(entityType)
                                                            else
                                                                Left(new ValueParseError(name, value.toString, s"wrong refEntityType: " +
                                                                    s"$valueEntityTypeName - not a subType of ${entitySuperType.name}"))
                                                case Some(typeUnconditional) =>
                                                    Left(new ValueParseError(name, value.toString, s"wrong refEntityType format: $typeUnconditional"))
                                                case None =>
                                                    Left(new ValueParseError(name, value.toString, "no refEntityType when refValue is present"))
                                        refEntityTypeRes match
                                            case Right(refEntityType) =>
                                                refEntityType.parseEntity(refId, refValue) match
                                                    case Right(refValue) =>
                                                        res.setRefValue(refValue)
                                                        Right(res)
                                                    case Left(error) =>
                                                        Left(error)
                                            case Left(error) =>
                                                Left(error)
                                    case None =>
                                        Right(res)
                            case Left(error) => 
                                Left(error)                        
                    case None =>
                        Left(new ValueParseError(name, value.toString, "no refId"))
            case _ =>
                Left(new ValueParseError(name, value.toString, "wrong reference format"))
    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}])"
    
object TypeReferenceDefinition:
    def apply[ID <: EntityId[_, ID]](
                                              referencedType: AbstractEntityType[_, _, _],
                                              refTypeName: String
                                          ): TypeReferenceDefinition[ID] =
        referencedType match
            case referencedType: AbstractEntityType[ID, _, _] =>
                TypeReferenceDefinition(referencedType)
            case _ =>
                throw new ConsistencyException(s"Not correct reference ID in type: $referencedType! " +
                    s"Error is impossible - analise if it was thrown! Type: $refTypeName")

final case class TypeBackReferenceDefinition[ID <: EntityId[_, ID]](
                                                                             //reference to abstract type to make it possible to reference to any concrete nested type
                                                                             referencedType: AbstractObjectEntityType[ID, _],
                                                                             refField: String
                                            ) extends ReferenceDefinition[ID, BackReferenceValue[ID]]:
    lazy val idType: EntityIdTypeDefinition[ID] = referencedType.typeDefinition.idType
    override def name: String = referencedType.name + "." + refField + "[]"
    def toJson(value: BackReferenceValue[ID]): JsValue =
        JsObject(
            "refId" -> value.value.toJson,
            "refType" -> JsString(referencedType.name),
            "value" -> value.refValueOpt.map(v => JsArray(v.map(_.toJson).toVector)).getOrElse(JsNull),
        )
    def parse(value: JsValue): Either[SingleMessageError, BackReferenceValue[ID]] = value match
        case JsObject(fields) =>
            fields.get("refId") match
                case Some(refId) =>
                    idType.parse(refId) match
                        case Right(refId) =>
                            val res = BackReferenceValue[ID](refId, BackReferenceType(this))
                            fields.get("value") match
                                case Some(refValue) =>
                                    val refEntityTypeRes:  Either[SingleMessageError, EntityType[ID, _, _]] = referencedType match
                                        case entityType: EntityType[ID, _, _] =>
                                            Right(entityType)
                                        case entitySuperType: EntitySuperType[ID, _, _] => fields.get("type") match
                                            case Some(JsString(valueEntityTypeName)) =>
                                                GlobalTypesMap.getTypeByName(valueEntityTypeName) match
                                                    case Some(entityType: EntityType[ID, _, _]) =>
                                                        if (entitySuperType.hasChild(entityType))
                                                            Right(entityType)
                                                        else
                                                            Left(new ValueParseError(name, value.toString, s"wrong refEntityType: " +
                                                                s"$valueEntityTypeName - not a subType of ${entitySuperType.name}"))
                                                    case Some(typeUnconditional) =>
                                                        Left(new ValueParseError(name, value.toString, s"found refEntityType is not EntityType: $typeUnconditional"))
                                                    case None =>
                                                        Left(new ValueParseError(name, value.toString, s"refEntityType not found by name: $valueEntityTypeName"))
                                            case Some(typeUnconditional) =>
                                                Left(new ValueParseError(name, value.toString, s"wrong refEntityType format: $typeUnconditional"))
                                            case None =>
                                                Left(new ValueParseError(name, value.toString, "no refEntityType when refValue is present"))
                                    refEntityTypeRes match
                                        case Right(refEntityType) =>
                                            val refValues: Either[SingleMessageError, Seq[Entity[ID, _, _]]] = refValue match
                                                case JsArray(refValues) =>
                                                    boundary {
                                                        Right(refValues.map(refValueJs =>
                                                            refEntityType.parseEntity(refId, refValueJs) match
                                                                case Right(refValue) =>
                                                                    refValue
                                                                case Left(error) =>
                                                                    break( Left(error) )
                                                        ))                                                    
                                                    }
                                                case _ =>
                                                    Left(new ValueParseError(name, value.toString, "wrong back reference value format"))
                                            refValues match
                                                case Right(refValues) =>
                                                    res.setRefValue(refValues)
                                                    Right(res)
                                                case Left(error) =>
                                                    Left(error)
                                        case Left(error) =>
                                            Left(error)
                                case None =>
                                    Right(res)
                        case Left(error) =>
                            Left(error)
                case None =>
                    Left(new ValueParseError(name, value.toString, "no refId"))
        case _ =>
            Left(new ValueParseError(name, value.toString, "wrong back reference format"))

    override def toString: String = s"TypeReferenceDefinition(ref[${referencedType.name}], $refField)"

object TypeBackReferenceDefinition:
    def apply[ID <: EntityId[_, ID]](
                                              referencedType: AbstractEntityType[_, _, _], 
                                              refField: String, 
                                              refTypeName: String
                                          ): TypeBackReferenceDefinition[ID] =
        referencedType match
            case referencedType: AbstractObjectEntityType[ID, _] =>
                TypeBackReferenceDefinition(referencedType, refField)
            case _ =>
                throw new ConsistencyException("Only object types could be referenced by back reference! " +
                    s"Type $refTypeName is trying to be referenced by $refField!")


final case class SimpleObjectTypeDefinition[ID <: EntityId[_, ID]](
                                        private var _fields: Map[String, FieldTypeDefinition[_]],
                                        parent: Option[ObjectEntitySuperType[ID, _]]
                                    ) extends FieldValueTypeDefinition[SimpleObjectValue[ID]], FieldsContainer:
    private var initiated = false

    def fields: Map[String, FieldTypeDefinition[_]] = _fields
    
    def createValue[ID2 <: EntityId[_, ID2]](
        idAndParentType: Option[(ID2, AbstractObjectEntityType[ID2, _])],
        value: Map[String, EntityValue]
    ): SimpleObjectValue[ID] =
        SimpleObjectValue(checkIdAndType(idAndParentType), value, SimpleObjectType(this))

    private def checkIdAndType[ID2 <: EntityId[_, ID2]](
                                                                 idAndParentType: Option[(ID2, AbstractObjectEntityType[ID2, _])]
                                                             ): Option[(ID, AbstractObjectEntityType[ID, _])] =
        (idAndParentType, parent) match
            case (Some((id, parentTypeInstance)), Some(parentTypeDef)) =>

                if (parentTypeDef.typeDefinition.idType != id.typeDefinition)
                    throw new ConsistencyException(s"Wrong ID type for simple object $id! Expected ${parentTypeDef.typeDefinition.idType}");
                if (parentTypeDef.hasChild(parentTypeInstance))
                    throw new ConsistencyException(s"Wrong parent type for simple object $id! Expected child of " +
                        s"${parentTypeDef.name}, found: ${parentTypeInstance.name}");
                Some((id.asInstanceOf[ID], parentTypeInstance.asInstanceOf[AbstractObjectEntityType[ID, _]]))
            case (None, None) =>
                None
            case _ =>
                throw new ConsistencyException(s"Wrong ID or parent type $idAndParentType for simple object type parent type: $parent!")
        

    def setChildren(fieldsValues: Map[String, FieldTypeDefinition[_]]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true

    lazy val allFields: Map[String, FieldTypeDefinition[_]] =
        _fields ++ parent.map(_.typeDefinition.allFields).getOrElse(Map.empty[String, FieldTypeDefinition[_]])

    override def toString: String = parent.map(_.name).getOrElse("") + "{" +
        fields.map(f => s"${f._1}: ${f._2}").mkString(", ") + "}"

    def idTypeOpt: Option[EntityIdTypeDefinition[ID]] = parent.map(_.typeDefinition.idType)

    def toJson(value: SimpleObjectValue[ID]): JsValue = JsObject(
        "parent" -> value.idAndParentType.map(idAndParentType => 
            JsObject(
                "id" -> value.id.map(_.toJson).getOrElse(JsNull),
                "type" -> JsString(idAndParentType._2.name)
            )
        ).getOrElse(JsNull),
        "value" -> JsObject(value.value.map(v => v._1 -> v._2.toJson))
    )

    def parse(value: JsValue): Either[SingleMessageError, SimpleObjectValue[ID]] =
        value match
            case JsObject(topFields) =>
                val idAndParentRes: Either[SingleMessageError, Option[(ID, AbstractObjectEntityType[ID, _])]] =
                    (topFields.get("parent"), parent) match
                        case (Some(JsObject(parentData)), Some(parentTypeDef)) =>
                            (parentData.get("id"), parentData.get("type")) match
                                case (Some(idJson), Some(JsString(parentTypeName))) =>
                                    val parentTypeActual: Either[ValueParseError, AbstractEntityType[_, _, _]] = GlobalTypesMap.getTypeByName(parentTypeName) match
                                        case Some(parentTypeActual) => Right(parentTypeActual)
                                        case None => Left(ValueParseError(name, value.toString, s"Parent type: \"$parentTypeName\" not found!"))
                                    val objectParentTypeActualRes: Either[ValueParseError, AbstractObjectEntityType[ID, _]] = parentTypeActual.flatMap {
                                        case objectParentTypeActual: AbstractObjectEntityType[ID, _] =>
                                            if objectParentTypeActual.isChildOf(parentTypeDef) then
                                                Right(objectParentTypeActual)
                                            else
                                                Left(ValueParseError(name, value.toString, s"Parent type: $objectParentTypeActual is not a child of " +
                                                    s"parent definition type: ${parentTypeDef.name}!"))
                                        case _ =>
                                            Left(ValueParseError(name, value.toString, s"Parent type: $parentTypeActual is not an object type!"))
                                    }
                                    val idValue = parentTypeDef.typeDefinition.idType.parse(idJson)
                                    (idValue, objectParentTypeActualRes) match  
                                        case (Right(id), Right(parentTypeActual)) =>
                                            Right(Some(id, parentTypeActual))
                                        case (idError: Left[_, _], Right(_)) =>
                                            idError.asInstanceOf[Left[SingleMessageError, Option[(ID, AbstractObjectEntityType[ID, _])]]]
                                        case (Right(_), typeError: Left[_, _]) =>
                                            typeError.asInstanceOf[Left[SingleMessageError, Option[(ID, AbstractObjectEntityType[ID, _])]]]
                                        case (Left(idError), Left(typeError)) =>
                                            Left(ValueParseError(name, value.toString, s"Parent errors found! id: ${idError.message} and " +
                                                s"type: $parentTypeActual"))
                                case _ =>
                                    Left(ValueParseError(name, value.toString, s"Parent id: ${parentData.get("id")}, or type: ${parentData.get("type")}  " +
                                        s"are absent when required!"))
                        case (None, None) =>
                            Right(None)
                        case _ => 
                            Left(ValueParseError(name, value.toString, s"Parent definition and value mismatch! Def: $parent, " +
                                s"value: ${topFields.get("parent")}"))

                val parsedFields: Either[SingleMessageError, Map[String, EntityValue]] =
                    topFields.get("value") match
                        case Some(JsObject(fields)) =>
                            boundary {
                                Right(fields.map {
                                    case (fieldName, fieldValue) =>
                                        _fields.get(fieldName) match
                                            case Some(fieldType) =>
                                                fieldType.valueType.parse(fieldValue) match
                                                    case Right(parsedValue) =>
                                                        fieldName -> parsedValue
                                                    case Left(error) =>
                                                        break(Left(error))
                                            case None =>
                                                break(Left(ValueParseError(name, value.toString, s"Field $fieldName not " +
                                                    s"found in simple object definition!")))
                                })
                            }
                        case None =>
                            Left(ValueParseError(name, value.toString, "No value field in object"))

                idAndParentRes.flatMap(idAndParent => parsedFields.map(fields => SimpleObjectValue(idAndParent, fields, SimpleObjectType(this))))
            case _ =>
                Left(ValueParseError(name, value.toString, "not an object"))

object SimpleObjectTypeDefinition:
    def apply[ID <: EntityId[_, ID]](
                                              parent: Option[ObjectEntitySuperType[_, _]],
                                              fields: Map[String, FieldTypeDefinition[_]],
                                          ): SimpleObjectTypeDefinition[ID] =
        parent match
            case parentWithCorrectId: Some[ObjectEntitySuperType[ID, _]] =>
                SimpleObjectTypeDefinition(fields, parentWithCorrectId)
            case None =>
                SimpleObjectTypeDefinition(fields, None)
            case _ =>
                throw new ConsistencyException(s"Not correct ID in simple object parent type: $parent! " +
                    s"Error is impossible - analise if it was thrown!")

sealed trait AbstractRootPrimitiveTypeDefinition:
    def name: String

sealed abstract case class RootPrimitiveTypeDefinition[V <: RootPrimitiveValue[V]](name: String)
            extends AbstractRootPrimitiveTypeDefinition with ItemValueTypeDefinition[V] with TypeDefinition:
    type ValueType = V
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
    def createValue(value: Byte): ByteValue = ByteValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[ByteValue] = rs.byteOpt(pos).map(createValue)
    protected override def parseInner(value: String): ByteValue = ByteValue(value.toByte)

object ShortIntTypeDefinition extends RootPrimitiveTypeDefinition[ShortIntValue]("Short"):
    override def toJson(value: ShortIntValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, ShortIntValue] =
        parseWholeNumber(value, v => v >= Short.MinValue && v <= Short.MaxValue, v => ShortIntValue(v.shortValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)) )
    def createValue(value: Short): ShortIntValue = ShortIntValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[ShortIntValue] = rs.shortOpt(pos).map(createValue)
    protected override def parseInner(value: String): ShortIntValue = ShortIntValue(value.toShort)

object IntTypeDefinition extends RootPrimitiveTypeDefinition[IntValue]("Integer"):
    override def toJson(value: IntValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, IntValue] =
        parseWholeNumber(value, v => v >= Byte.MinValue && v <= Byte.MaxValue, v => IntValue(v.intValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)) )
    def createValue(value: Int): IntValue = IntValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[IntValue] = rs.intOpt(pos).map(createValue)
    protected override def parseInner(value: String): IntValue = IntValue(value.toInt)

object LongTypeDefinition extends RootPrimitiveTypeDefinition[LongValue]("Long"):
    override def toJson(value: LongValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, LongValue] =
        parseWholeNumber(value, v => true, v => LongValue(v),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)) )
    def createValue(value: Long): LongValue = LongValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[LongValue] = rs.longOpt(pos).map(createValue)
    protected override def parseInner(value: String):LongValue = LongValue(value.toLong)

object DoubleTypeDefinition extends RootPrimitiveTypeDefinition[DoubleValue]("Double"):
    override def toJson(value: DoubleValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, DoubleValue] =
        parseDecimalNumber(value, v => v.isDecimalDouble, v => DoubleValue(v.doubleValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: Double): DoubleValue = DoubleValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[DoubleValue] = rs.doubleOpt(pos).map(createValue)
    protected override def parseInner(value: String): DoubleValue = DoubleValue(value.toDouble)

object FloatTypeDefinition extends RootPrimitiveTypeDefinition[FloatValue]("Float"):
    override def toJson(value: FloatValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, FloatValue] =
        parseDecimalNumber(value, v => v.isDecimalFloat, v => FloatValue(v.floatValue),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: Float): FloatValue = FloatValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[FloatValue] = rs.floatOpt(pos).map(createValue)
    protected override def parseInner(value: String): FloatValue = FloatValue(value.toFloat)

object DecimalTypeDefinition extends RootPrimitiveTypeDefinition[DecimalValue]("Decimal"):
    override def toJson(value: DecimalValue): JsValue = JsNumber(value.value)
    def parse(value: JsValue): Either[ValueParseError, DecimalValue] =
        parseDecimalNumber(value, v => true, v => DecimalValue(v),
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: BigDecimal): DecimalValue = DecimalValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[DecimalValue] = rs.bigDecimalOpt(pos).map(BigDecimal(_)).map(createValue)
    protected override def parseInner(value: String): DecimalValue = DecimalValue(BigDecimal(value))

object BooleanTypeDefinition extends RootPrimitiveTypeDefinition[BooleanValue]("Boolean"):
    override def toJson(value: BooleanValue): JsValue = JsBoolean(value.value)
    def parse(value: JsValue): Either[ValueParseError, BooleanValue] =
        value match
            case JsBoolean(bool) => Right(BooleanValue(bool))
            case _ => Left(new ValueParseError(this.getClass.getSimpleName, value.toString))
    def createValue(value: Boolean): BooleanValue = BooleanValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[BooleanValue] = rs.booleanOpt(pos).map(createValue)
    protected override def parseInner(value: String): BooleanValue = BooleanValue(value.toBoolean)

object DateTypeDefinition extends RootPrimitiveTypeDefinition[DateValue]("Date"):
    override def toJson(value: DateValue): JsValue = JsString(GlobalSerializationData.json.serialize(value.value))
    def parse(value: JsValue): Either[ValueParseError, DateValue] =
        parseFormattedString[LocalDate, DateValue](value, GlobalSerializationData.json.parseDate,
            DateValue.apply, rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: LocalDate): DateValue = DateValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[DateValue] = rs.localDateOpt(pos).map(createValue)
    protected override def parseInner(value: String): DateValue = DateValue(LocalDate.parse(value))

object DateTimeTypeDefinition extends RootPrimitiveTypeDefinition[DateTimeValue]("DateTime"):
    override def toJson(value: DateTimeValue): JsValue = JsString(GlobalSerializationData.json.serialize(value.value))
    def parse(value: JsValue): Either[ValueParseError, DateTimeValue] =
        parseFormattedString[LocalDateTime, DateTimeValue](value, GlobalSerializationData.json.parseDateTime,
            DateTimeValue.apply, rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: LocalDateTime): DateTimeValue = DateTimeValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[DateTimeValue] = rs.localDateTimeOpt(pos).map(createValue)
    protected override def parseInner(value: String): DateTimeValue = DateTimeValue(LocalDateTime.parse(value))

object TimeTypeDefinition extends RootPrimitiveTypeDefinition[TimeValue]("Time"):
    override def toJson(value: TimeValue): JsValue = JsString(GlobalSerializationData.json.serialize(value.value))
    def parse(value: JsValue): Either[ValueParseError, TimeValue] =
        parseFormattedString[LocalTime, TimeValue](value, GlobalSerializationData.json.parseTime, 
            TimeValue.apply, rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: LocalTime): TimeValue = TimeValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[TimeValue] = rs.localTimeOpt(pos).map(createValue)
    protected override def parseInner(value: String): TimeValue = TimeValue(LocalTime.parse(value))

object UUIDTypeDefinition extends RootPrimitiveTypeDefinition[UUIDValue]("UUID"):
    override def toJson(value: UUIDValue): JsValue = JsString(value.value.toString)
    def parse(value: JsValue): Either[ValueParseError, UUIDValue] =
        parseFormattedString[UUID, UUIDValue](value, UUID.fromString, UUIDValue.apply,
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: UUID): UUIDValue = UUIDValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[UUIDValue] = rs.stringOpt(pos).map(parseInner)
    protected override def parseInner(value: String): UUIDValue = UUIDValue(java.util.UUID.fromString(value))

object BinaryTypeDefinition extends RootPrimitiveTypeDefinition[BinaryValue]("Binary"):
    override def toJson(value: BinaryValue): JsValue = JsString(Base64.rfc2045().encodeToString(value.value, false))
    def parse(value: JsValue): Either[ValueParseError, BinaryValue] =
        parseFormattedString[Array[Byte], BinaryValue](value, Base64.rfc2045().decode, BinaryValue.apply,
            rootCause => Left(new ValueParseError(name, value.toString, rootCause)))
    def createValue(value: Array[Byte]): BinaryValue = BinaryValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[BinaryValue] = rs.bytesOpt(pos).map(createValue)
    protected override def parseInner(value: String): BinaryValue = BinaryValue(Base64.rfc2045().decode(value))

object StringTypeDefinition extends RootPrimitiveTypeDefinition[StringValue]("String"):
    override def toJson(value: StringValue): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[ValueParseError, StringValue] =
        parseString(value, rootCause => Left(new ValueParseError(name, value.toString, rootCause))).map(StringValue.apply)
    def createValue(value: String): StringValue = StringValue(value)
    def extract(rs: WrappedResultSet, pos: Int): Option[StringValue] = rs.stringOpt(pos).map(createValue)
    override def isString: Boolean = true
    protected override def parseInner(value: String): StringValue = StringValue(value)

class FixedStringTypeDefinition(val length: Int)
        extends RootPrimitiveTypeDefinition[FixedStringValue](FixedStringTypeDefinition.name):
    override def toJson(value: FixedStringValue): JsValue = JsString(value.value)
    def parse(value: JsValue): Either[ValueParseError, FixedStringValue] =
        parseString(value, rootCause => Left(new ValueParseError(name, value.toString, rootCause))).map(createValue)
    def createValue(value: String): FixedStringValue = FixedStringValue(value, this)
    def extract(rs: WrappedResultSet, pos: Int): Option[FixedStringValue] = rs.stringOpt(pos).map(createValue)
    override def isString: Boolean = true
    protected override def parseInner(value: String): FixedStringValue = createValue(value)

object FixedStringTypeDefinition extends AbstractRootPrimitiveTypeDefinition:
    val name = "FixedString"

trait FieldsContainer:
    def fields: Map[String, FieldTypeDefinition[_]]
    def allFields: Map[String, FieldTypeDefinition[_]]
    def idTypeOpt: Option[EntityIdTypeDefinition[_]]
    def getFieldType(fieldName: String, includeParent: Boolean): FieldTypeDefinition[_] =
        val resOpt = if (includeParent) allFields.get(fieldName)  else fields.get(fieldName)
        resOpt.getOrElse(throw new ConsistencyException(s"Field $fieldName not found in object type $this!"))

private def parseWholeNumber[V <: RootPrimitiveValue[_] | EntityId[_, _]](
                                                            value: JsValue,
                                                            rangeCheker: Long => Boolean,
                                                            valueWrapper: Long => V,
                                                            errorGenerator: String => Left[ValueParseError, V],
                                                        ): Either[ValueParseError, V] =
    value match
        case JsNumber(valueInner) =>
            if (valueInner.isWhole)
                wrappNumberInRange(valueInner.toLong, rangeCheker, valueWrapper, errorGenerator)
            else
                errorGenerator(" not a whole number")
        case _ =>
            errorGenerator(" not a number")

private def parseDecimalNumber[V <: RootPrimitiveValue[_]](
                                                            value: JsValue,
                                                            rangeCheker: BigDecimal => Boolean,
                                                            valueWrapper: BigDecimal => V,
                                                            errorGenerator: String => Left[ValueParseError, V],
                                                             ): Either[ValueParseError, V] =
    value match
        case JsNumber(valueInner) =>
            wrappNumberInRange(valueInner, rangeCheker, valueWrapper, errorGenerator)
        case _ =>
            errorGenerator("not a number")

private def wrappNumberInRange[P, V <: RootPrimitiveValue[_] | EntityId[_, _]](
                                                  value: P,
                                                  rangeCheker: P => Boolean,
                                                  valueWrapper: P => V,
                                                  errorGenerator: String => Left[ValueParseError, V],
                                             ): Either[ValueParseError, V] =
        if (rangeCheker(value))
            Right(valueWrapper(value))
        else
            errorGenerator("out of range")

private def parseString(
                                                    value: JsValue,
                                                    errorGenerator: String => Left[ValueParseError, String]
                                                        ): Either[ValueParseError, String] =
    value match
        case JsString(data) => Right(data)
        case _ => errorGenerator("not a JSON string")

private def parseFormattedString[T, V <: RootPrimitiveValue[_] | EntityId[_, _]](
                                                        value: JsValue,
                                                        exceptionalParser: String => T,
                                                        valueWrapper: T => V,
                                                        errorGenerator: String => Left[ValueParseError, T],
                                                    ): Either[ValueParseError, V] =
    parseString(value, errorGenerator.asInstanceOf[String => Left[ValueParseError, String]])
        .map(data => wrappExceptionalParser(exceptionalParser)(data, errorGenerator))
        .flatMap(identity)
        .map(valueWrapper)


private def wrappExceptionalParser[T](parser: String => T): 
                        (String, String => Left[ValueParseError, T]) => Either[ValueParseError, T] =
    (value: String, errorGenerator: String => Left[ValueParseError, T]) =>
        try {
            Right(parser(value))
        } catch {
            case e: Exception => errorGenerator(s"not a UUID: ${e.getMessage}")
        }


sealed trait EntityTypeDefinition[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes] extends AbstractTypeDefinition:
    def idType: EntityIdTypeDefinition[ID]
    def parent: Option[EntitySuperType[ID, _, V]]
    def parseValue(data: JsValue): Either[ValueParseError, V]
    def toJson(value: V): JsValue

final case class CustomPrimitiveTypeDefinition[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[V]](
    parentNode: Either[(EntityIdTypeDefinition[ID], RootPrimitiveTypeDefinition[V]), PrimitiveEntitySuperType[ID, _, V]]
) extends EntityTypeDefinition[ID, VT, V]:
    @tailrec def rootType: RootPrimitiveTypeDefinition[V] = this.parentNode match
        case Left((_, root)) => root
        case Right(parent) => parent.typeDefinition.rootType
    lazy val idType: EntityIdTypeDefinition[ID] = parentNode.fold(_._1, _.typeDefinition.idType)
    lazy val parent: Option[PrimitiveEntitySuperType[ID, _, V]] = parentNode.toOption

    def parse(value: String): Either[ValueParseError, V] =
        rootType.parse(value)
    def toJson(value: V): JsValue =
        value.toJson
    def parseValue(data: JsValue): Either[ValueParseError, V] =
        rootType.parse(data)

//object CustomPrimitiveTypeDefinition:
//    def apply[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[_, V]](
//        parentNode: Either[(EntityIdTypeDefinition[ID], RootPrimitiveTypeDefinition[_, V]), PrimitiveEntitySuperType[ID, _, V]]
//    ): CustomPrimitiveTypeDefinition[ID, VT, V] =
//        new CustomPrimitiveTypeDefinition(parentNode)

object ArrayTypeDefinition:
    val name = "Array"

final case class ArrayTypeDefinition[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
    private var _elementType: Option[ItemValueTypeDefinition[_]],
    idOrParent: Either[EntityIdTypeDefinition[ID], ArrayEntitySuperType[ID, _]],
    private var _initialized: Boolean = false,
) extends EntityTypeDefinition[ID, VT, Seq[ItemValue]]:
    lazy val elementType: ItemValueTypeDefinition[_] = 
        if (!_initialized)
            throw new WrongStateExcetion("Array element types not initialized!")
        _elementType.getOrElse(
            parent.map(_.typeDefinition.elementType)
                .getOrElse(throw new ConsistencyException("Array type without parent type shoud have element type!"))
        )
    def setChild(elementTypeValue: Option[ItemValueTypeDefinition[_]]): Unit =
        if (_initialized) 
            throw new TypeReinitializationException
        (elementTypeValue, idOrParent) match
            case (None, Left(_)) =>
                throw new ConsistencyException("Array type without parent type shoud have element type!")
            case _ => //do nothing
        _elementType = elementTypeValue
    lazy val allElementTypes: Set[ItemValueTypeDefinition[_]] =
        parent
            .map(_.typeDefinition.allElementTypes + elementType)
            .getOrElse(Set.empty[ItemValueTypeDefinition[_]])

    lazy val idType: EntityIdTypeDefinition[ID] = idOrParent.fold(identity, _.typeDefinition.idType)
    lazy val parent: Option[ArrayEntitySuperType[ID, _]] = idOrParent.toOption

    def toJson(value: Seq[ItemValue]): JsValue =
        if (allElementTypes.size == 1)
            JsArray(value.map(v => v.toJson).toVector)
        else
            JsArray(value.map(v =>
                JsObject(
                    "type" -> JsString(v.valueType.name),
                    "value" -> v.toJson
                )
            ).toVector)
            
    def parseValue(data: JsValue): Either[ValueParseError, Seq[ItemValue]] =
        data match
            case JsArray(items) =>
                boundary {
                    if (allElementTypes.size == 1)
                        val onlyTypeDef = elementType
                        Right(items.map(item =>
                            elementType.parse(item) match
                                case Right(value) =>
                                    value.asInstanceOf[ItemValue]
                                case Left(error) =>
                                    break( Left(ValueParseError("Array", item.toString, error.message)) )
                        ))
                    else
                        val typesMap = allElementTypes.map(t => t.name -> t).toMap
                        Right(items.map {
                            case item@JsObject(fields) =>
                                (fields.get("type"), fields.get("value")) match
                                    case (Some(JsString(typeName)), Some(value: JsValue)) =>
                                        typesMap.get(typeName) match
                                            case Some(typeDef) =>
                                                typeDef.parse(value) match
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

case class ArrayItemTypeDefinition(valueType: ItemValueTypeDefinition[_]):
    def name: String = valueType.name
    override def toString: String = name

case class FieldTypeDefinition[V <: EntityValue](valueType: FieldValueTypeDefinition[V], isNullable: Boolean):
    private var _persistenceData: Option[ValuePersistenceDataFinal] = None
    private[domain] def setPersistenceData(data: ValuePersistenceDataFinal): Unit =
        if (_persistenceData.isDefined) throw new ConsistencyException(s"Type $name already has persistence data!")
        _persistenceData = Some(data)
    def persistenceData: Option[ValuePersistenceDataFinal] =
        _persistenceData

object ObjectTypeDefinition:
    val name = "Object"



final case class ObjectTypeDefinition[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
    private var _fields: Map[String, FieldTypeDefinition[_]],
    idOrParent: Either[EntityIdTypeDefinition[ID], ObjectEntitySuperType[ID, _]],
) extends EntityTypeDefinition[ID, VT, Map[String, EntityValue]], FieldsContainer:
    private var initiated = false
    def fields: Map[String, FieldTypeDefinition[_]] = _fields
    private[sgql] def setFields(fieldsValues: Map[String, FieldTypeDefinition[_]]): Unit =
        if (initiated) throw new TypeReinitializationException
        _fields = fieldsValues
        initiated = true
    lazy val allFields: Map[String, FieldTypeDefinition[_]] =
        _fields ++ parent.map(_.typeDefinition.allFields).getOrElse(Map.empty[String, FieldTypeDefinition[_]])
    lazy val idType: EntityIdTypeDefinition[ID] = idOrParent.fold(identity, _.typeDefinition.idType)
    lazy val parent: Option[ObjectEntitySuperType[ID, _]] = idOrParent.toOption
    def idTypeOpt: Option[EntityIdTypeDefinition[ID]] = Some(idType)

    def toJson(value: Map[String, EntityValue]): JsValue = JsObject(value.map {
        case (fieldName, fieldValue) => fieldName -> fieldValue.toJson
    })
    
    def parseValue(data: JsValue): Either[ValueParseError, Map[String, EntityValue]] =
        data match
            case JsObject(fields) =>
                boundary {
                    Right(fields.map {
                        case (fieldName, fieldValue) =>
                            this.allFields.get(fieldName) match
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



val idTypesMap = Map[String, AbstractEntityIdTypeDefinition[_]](
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

