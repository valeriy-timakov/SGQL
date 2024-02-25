package my.valerii_timakov.sgql.entity.json

import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.{ArrayType, ArrayTypeDefinition, BinaryType, BinaryTypeDefinition, BooleanType, BooleanTypeDefinition, DateTimeType, DateTimeTypeDefinition, DateType, DateTypeDefinition, DoubleType, DoubleTypeDefinition, Entity, EntityFieldType, EntityFieldTypeDefinition, EntityId, EntityIdTypeDefinition, EntityType, FloatType, FloatTypeDefinition, IntId, IntIdTypeDefinition, IntType, IntTypeDefinition, LongId, LongIdTypeDefinition, LongType, LongTypeDefinition, ObjectType, ObjectTypeDefinition, StringId, StringIdTypeDefinition, StringType, StringTypeDefinition, TimeType, TimeTypeDefinition, UUIDId, UUIDIdTypeDefinition}
import spray.json.{JsField, *}
import spray.json.DefaultJsonProtocol.*

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

// Спочатку визначимо формати для примітивних типів
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
    case ArrayType(value) => JsArray(value.map(field2json).toVector)
    case ObjectType(value) => JsObject(value.map((fieldName, fieldValue) => fieldName -> field2json(fieldValue)))
    case _ => serializationError("Unknown EntityFieldType")


def json2field(json: JsValue): EntityFieldType = json match 
    case JsString(value) => StringType(value)
    case JsNumber(value) =>
        if (value.isValidLong) LongType(value.toLong)
        else DoubleType(value.toDouble)
    case JsBoolean(value) => BooleanType(value)
    case JsArray(value) => ArrayType(value.map(json2field).toList)
    case JsObject(value) => ObjectType(value.map((fieldName, fieldValue) => fieldName -> json2field(fieldValue)))
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
        case Seq(id, fields) => entity.Entity(id.convertTo[EntityId], fields.convertTo[EntityFieldType])
        case _ => deserializationError("Entity expected")
    

implicit val entityListFormat: RootJsonFormat[Seq[Entity]] = new RootJsonFormat[Seq[Entity]]:
    def write(obj: Seq[Entity]): JsValue = JsArray(obj.map(_.toJson).toVector)
    def read(json: JsValue): Seq[Entity] = json match
        case JsArray(array) => array.map(_.convertTo[Entity]).toList
        case _ => deserializationError("List[Entity] expected")
       

implicit val entityIdTypeDefinitionFormat: RootJsonFormat[EntityIdTypeDefinition] = new RootJsonFormat[EntityIdTypeDefinition]:    
    override def write(obj: EntityIdTypeDefinition): JsValue = obj match 
        case IntIdTypeDefinition => JsString("IntIdTypeDefinition")
        case LongIdTypeDefinition => JsString("LongIdTypeDefinition")
        case StringIdTypeDefinition => JsString("StringIdTypeDefinition")
        case UUIDIdTypeDefinition => JsString("UUIDIdTypeDefinition")    
    override def read(json: JsValue): EntityIdTypeDefinition = json match 
        case JsString(value) => value match
            case "IntIdTypeDefinition" => IntIdTypeDefinition
            case "LongIdTypeDefinition" => LongIdTypeDefinition
            case "StringIdTypeDefinition" => StringIdTypeDefinition
            case "UUIDIdTypeDefinition" => UUIDIdTypeDefinition
            case _ => deserializationError("EntityIdTypeDefinition expected")
        case _ => deserializationError("EntityIdTypeDefinition expected")
    
implicit val entityFieldTypeDefinitionFormat: RootJsonFormat[EntityFieldTypeDefinition] = new RootJsonFormat[EntityFieldTypeDefinition]:    
    override def write(obj: EntityFieldTypeDefinition): JsValue = obj match 
        case StringTypeDefinition => JsString("StringTypeDefinition")
        case IntTypeDefinition => JsString("IntTypeDefinition")
        case LongTypeDefinition => JsString("LongTypeDefinition")
        case FloatTypeDefinition => JsString("FloatTypeDefinition")
        case DoubleTypeDefinition => JsString("DoubleTypeDefinition")
        case BooleanTypeDefinition => JsString("BooleanTypeDefinition")
        case DateTypeDefinition => JsString("DateTypeDefinition")
        case DateTimeTypeDefinition => JsString("DateTimeTypeDefinition")
        case TimeTypeDefinition => JsString("TimeTypeDefinition")
        case BinaryTypeDefinition => JsString("BinaryTypeDefinition")
        case ArrayTypeDefinition(elementsType) => JsArray(write(elementsType))
        case ObjectTypeDefinition(fieldsTypes) => JsObject(fieldsTypes.map((fieldName, fieldType) => fieldName -> write(fieldType)))
    override def read(json: JsValue): EntityFieldTypeDefinition = json match 
        case JsString(value) => value match
            case "StringTypeDefinition" => StringTypeDefinition
            case "IntTypeDefinition" => IntTypeDefinition
            case "LongTypeDefinition" => LongTypeDefinition
            case "FloatTypeDefinition" => FloatTypeDefinition
            case "DoubleTypeDefinition" => DoubleTypeDefinition
            case "BooleanTypeDefinition" => BooleanTypeDefinition
            case "DateTypeDefinition" => DateTypeDefinition
            case "DateTimeTypeDefinition" => DateTimeTypeDefinition
            case "TimeTypeDefinition" => TimeTypeDefinition
            case "BinaryTypeDefinition" => BinaryTypeDefinition
            case _ => deserializationError("EntityFieldTypeDefinition expected")
        case JsArray(elements) => 
            if (elements.size == 1) 
                ArrayTypeDefinition(read(elements(0)))
            else 
                deserializationError("ArrayTypeDefinition expected") 
        case JsObject(fields) =>
            ObjectTypeDefinition(fields.map((fieldName, fieldType) => fieldName -> read(fieldType)))
        case _ => deserializationError("EntityFieldTypeDefinition expected")


implicit val entityTypeFormat: RootJsonFormat[EntityType] = new RootJsonFormat[EntityType]:    
    override def write(obj: EntityType): JsValue = JsObject(
        "name" -> JsString(obj.name),
        "idType" -> obj.idType.toJson,
        "valueType" -> obj.valueType.toJson
    )    
    override def read(json: JsValue): EntityType = json.asJsObject.getFields("name", "idType", "fields") match 
        case Seq(JsString(name), idType, fieldsType) => 
            EntityType(name, idType.convertTo[EntityIdTypeDefinition], fieldsType.convertTo[EntityFieldTypeDefinition])
        case _ => deserializationError("EntityType expected")


implicit val entityTypeListFormat: RootJsonFormat[Seq[EntityType]] = new RootJsonFormat[Seq[EntityType]]:
    def write(obj: Seq[EntityType]): JsValue = JsArray(obj.map(_.toJson).toVector)
    def read(json: JsValue): Seq[EntityType] = json match
        case JsArray(array) => array.map(_.convertTo[EntityType]).toList
        case _ => deserializationError("List[Entity] expected")


//implicit val entityTypeFormat: RootJsonFormat[entity.EntityType] = jsonFormat3(entity.EntityType.apply)

// Формат для серіалізації та десеріалізації Entity
//implicit val entityFormat: RootJsonFormat[Entity] = jsonFormat2(Entity.apply)
