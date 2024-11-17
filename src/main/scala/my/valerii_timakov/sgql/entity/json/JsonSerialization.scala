package my.valerii_timakov.sgql.entity.json

import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, BinaryValue, BooleanValue, DateTimeValue, DateValue, DoubleValue, EntityValue, EntityId, FloatValue, IntId, IntValue, LongId, LongValue, ObjectValue, StringId, StringValue, TimeValue, UUIDId, Entity}
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractNamedEntityType, AbstractTypeDefinition, ArrayItemTypeDefinition, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntityType, EntityTypeDefinition, FieldTypeDefinition, FieldValueTypeDefinitions, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, NamedEntitySuperType, ObjectTypeDefinition, PrimitiveEntitySuperType, PrimitiveTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition}
import spray.json.*
import spray.json.DefaultJsonProtocol.*

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

implicit object DateTypeFormat extends RootJsonFormat[DateValue]:
    def write(c: DateValue): JsString = JsString(c.value.toString)
    def read(value: JsValue): DateValue = value match {
        case JsString(str) => DateValue(LocalDate.parse(str))
        case _ => deserializationError("Date expected")
    }


implicit object DateTimeTypeFormat extends RootJsonFormat[DateTimeValue]:
    def write(c: DateTimeValue): JsString = JsString(c.value.toString)
    def read(value: JsValue): DateTimeValue = value match {
        case JsString(str) => DateTimeValue(LocalTime.parse(str))
        case _ => deserializationError("DateTime expected")
    }


implicit object TimeTypeFormat extends RootJsonFormat[TimeValue]:
    def write(c: TimeValue): JsString = JsString(c.value.toString)
    def read(value: JsValue): TimeValue = value match {
        case JsString(str) => TimeValue(LocalDateTime.parse(str))
        case _ => deserializationError("Time expected")
    }


implicit object IntIdTypeFormat extends RootJsonFormat[IntId]:
    def write(c: IntId): JsNumber = JsNumber(c.value.toString)
    def read(value: JsValue): IntId = value match {
        case JsNumber(num) => if (num.isValidInt) IntId(num.toInt) else deserializationError("Int ID expected")
        case _ => deserializationError("Int expected")
    }


implicit object LongIdTypeFormat extends RootJsonFormat[LongId]:
    def write(c: LongId): JsNumber = JsNumber(c.value.toString)
    def read(value: JsValue): LongId = value match {
        case JsNumber(num) => if (num.isValidLong) LongId(num.toLong) else deserializationError("Long ID expected")
        case _ => deserializationError("Long expected")
    }


implicit object StringIdTypeFormat extends RootJsonFormat[StringId]:
    def write(c: StringId): JsString = JsString(c.value)
    def read(value: JsValue): StringId = value match {
        case JsString(str) => StringId(str)
        case _ => deserializationError("String ID expected")
    }


implicit object UUIDIdTypeFormat extends RootJsonFormat[UUIDId]:
    def write(c: UUIDId): JsString = JsString(c.value.toString)
    def read(value: JsValue): UUIDId = value match {
        case JsString(str) => UUIDId(UUID.fromString(str))
        case _ => deserializationError("String ID expected")
    }


def field2json(field: EntityValue): JsValue = field match 
    case StringValue(value) => JsString(value)
    case IntValue(value) => JsNumber(value)
    case LongValue(value) => JsNumber(value)
    case FloatValue(value) => JsNumber(value)
    case DoubleValue(value) => JsNumber(value)
    case BooleanValue(value) => JsBoolean(value)
    case DateValue(value) => JsString(value.toString)
    case DateTimeValue(value) => JsString(value.toString)
    case TimeValue(value) => JsString(value.toString)
    case BinaryValue(value) => JsString(Base64.rfc2045().encodeToString(value, false))
    case ArrayValue(value, _) => JsArray(value.map(field2json).toVector)
    case ObjectValue(value, _) => JsObject(value.map((fieldName, fieldValue) => fieldName -> field2json(fieldValue)))
    case _ => serializationError("Unknown EntityFieldType")


def json2field(json: JsValue): EntityValue = json match 
    case JsString(value) => StringValue(value)
    case JsNumber(value) =>
        if (value.isValidLong) LongValue(value.toLong)
        else DoubleValue(value.toDouble)
    case JsBoolean(value) => BooleanValue(value)
    case JsArray(value) => ??? //ArrayType(value.map(json2field).toList)
    case JsObject(value) => ??? //ObjectType(value.map((fieldName, fieldValue) => fieldName -> json2field(fieldValue)))
    case _ => deserializationError("Unknown EntityFieldType")


implicit object EntityFieldTypeFormat extends RootJsonFormat[EntityValue]:
    def write(fieldType: EntityValue): JsValue = field2json(fieldType)
    def read(json: JsValue): EntityValue = json2field(json)


implicit object EntityIdFormat extends RootJsonFormat[EntityId]:
    def write(c: EntityId): JsValue = c match 
        case IntId(value) => JsNumber(value)
        case LongId(value) => JsNumber(value)
        case StringId(value) => JsString(value)
        case UUIDId(value) => JsString(value.toString)
    def read(value: JsValue): EntityId = value match 
        case JsNumber(value) => LongId(value.toLong)
        case JsString(str) => StringId(str)
        case _ => deserializationError("EntityId expected")
    

implicit object EntityFormat extends RootJsonFormat[Entity]:    
    def write(entity: Entity): JsObject = JsObject(
        "id" -> entity.id.toJson,
        "value" -> entity.value.toJson,
    )    
    def read(json: JsValue): Entity = json.asJsObject.getFields("id", "fields") match 
        case Seq(id, fields) => Entity(id.convertTo[EntityId], fields.convertTo[EntityValue])
        case _ => deserializationError("Entity expected")
    

implicit val entityListFormat: RootJsonFormat[Seq[Entity]] = new RootJsonFormat[Seq[Entity]]:
    def write(obj: Seq[Entity]): JsValue = JsArray(obj.map(_.toJson).toVector)
    def read(json: JsValue): Seq[Entity] = json match
        case JsArray(array) => array.map(_.convertTo[Entity]).toList
        case _ => deserializationError("List[Entity] expected")

//------definitions------