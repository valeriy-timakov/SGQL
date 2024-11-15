package my.valerii_timakov.sgql.entity.json

import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayType, BinaryType, BooleanType, DateTimeType, DateType, DoubleType, EntityFieldType, EntityId, FloatType, IntId, IntType, LongId, LongType, ObjectType, StringId, StringType, TimeType, UUIDId}
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, AbstractNamedEntityType, AbstractTypeDefinition, ArrayItemTypeDefinition, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DoubleTypeDefinition, Entity, EntityIdTypeDefinition, EntityType, EntityTypeDefinition, FieldTypeDefinition, FieldValueTypeDefinitions, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, NamedEntitySuperType, ObjectTypeDefinition, PrimitiveEntitySuperType, PrimitiveFieldTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition}
import spray.json.*
import spray.json.DefaultJsonProtocol.*

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

implicit object DateTypeFormat extends RootJsonFormat[DateType]:
    def write(c: DateType): JsString = JsString(c.value.toString)
    def read(value: JsValue): DateType = value match {
        case JsString(str) => DateType(LocalDate.parse(str))
        case _ => deserializationError("Date expected")
    }


implicit object DateTimeTypeFormat extends RootJsonFormat[DateTimeType]:
    def write(c: DateTimeType): JsString = JsString(c.value.toString)
    def read(value: JsValue): DateTimeType = value match {
        case JsString(str) => DateTimeType(LocalTime.parse(str))
        case _ => deserializationError("DateTime expected")
    }


implicit object TimeTypeFormat extends RootJsonFormat[TimeType]:
    def write(c: TimeType): JsString = JsString(c.value.toString)
    def read(value: JsValue): TimeType = value match {
        case JsString(str) => TimeType(LocalDateTime.parse(str))
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


def field2json(field: EntityFieldType): JsValue = field match 
    case StringType(value) => JsString(value)
    case IntType(value) => JsNumber(value)
    case LongType(value) => JsNumber(value)
    case FloatType(value) => JsNumber(value)
    case DoubleType(value) => JsNumber(value)
    case BooleanType(value) => JsBoolean(value)
    case DateType(value) => JsString(value.toString)
    case DateTimeType(value) => JsString(value.toString)
    case TimeType(value) => JsString(value.toString)
    case BinaryType(value) => JsString(Base64.rfc2045().encodeToString(value, false))
    case ArrayType(value, _) => JsArray(value.map(field2json).toVector)
    case ObjectType(value, _) => JsObject(value.map((fieldName, fieldValue) => fieldName -> field2json(fieldValue)))
    case _ => serializationError("Unknown EntityFieldType")


def json2field(json: JsValue): EntityFieldType = json match 
    case JsString(value) => StringType(value)
    case JsNumber(value) =>
        if (value.isValidLong) LongType(value.toLong)
        else DoubleType(value.toDouble)
    case JsBoolean(value) => BooleanType(value)
    case JsArray(value) => ??? //ArrayType(value.map(json2field).toList)
    case JsObject(value) => ??? //ObjectType(value.map((fieldName, fieldValue) => fieldName -> json2field(fieldValue)))
    case _ => deserializationError("Unknown EntityFieldType")


implicit object EntityFieldTypeFormat extends RootJsonFormat[EntityFieldType]:
    def write(fieldType: EntityFieldType): JsValue = field2json(fieldType)
    def read(json: JsValue): EntityFieldType = json2field(json)


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
        case Seq(id, fields) => Entity(id.convertTo[EntityId], fields.convertTo[EntityFieldType])
        case _ => deserializationError("Entity expected")
    

implicit val entityListFormat: RootJsonFormat[Seq[Entity]] = new RootJsonFormat[Seq[Entity]]:
    def write(obj: Seq[Entity]): JsValue = JsArray(obj.map(_.toJson).toVector)
    def read(json: JsValue): Seq[Entity] = json match
        case JsArray(array) => array.map(_.convertTo[Entity]).toList
        case _ => deserializationError("List[Entity] expected")

//------definitions------