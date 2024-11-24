package my.valerii_timakov.sgql.entity.json

import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, BackReferenceValue, BinaryValue, BooleanValue, CustomPrimitiveValue, DateTimeValue, DateValue, DoubleValue, EmptyValue, Entity, EntityId, EntityValue, FloatValue, IntId, IntValue, ItemValue, LongId, LongValue, ObjectValue, ReferenceValue, RootPrimitiveValue, SimpleObjectValue, StringId, StringValue, TimeValue, UUIDId, ValueTypes}
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, AbstractNamedType, AbstractTypeDefinition, ArrayItemTypeDefinition, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DoubleTypeDefinition, AbstractEntityIdTypeDefinition, EntitySuperType, EntityType, EntityTypeDefinition, FieldTypeDefinition, FieldValueTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectTypeDefinition, PrimitiveEntitySuperType, AbstractRootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition}
import spray.json.*
import spray.json.DefaultJsonProtocol.*

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.UUID
import scala.annotation.tailrec

def id2json(id: EntityId[?, ?]): JsValue = id match
    case IntId(value) => JsNumber(value)
    case LongId(value) => JsNumber(value)
    case StringId(value) => JsString(value)
    case UUIDId(value) => JsString(value.toString)
    
def primitive2json(field: RootPrimitiveValue[?, ?]): JsValue = field match
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
    case _ => serializationError("Unknown EntityFieldType")
    
def entityValue2json(entity: EntityValue): JsValue = entity match
    case EmptyValue(definition) => JsNull
    case prim: RootPrimitiveValue[?, ?] => primitive2json(prim)
    case SimpleObjectValue(id, value, typeDefinition) =>  JsObject(
        "id" -> id.map(id2json).getOrElse(JsNull),
        "value" -> values2json(value),
    )
    case ref: ReferenceValue => JsObject(
        "id" -> id2json(ref.value),
        "type" -> typeRef2json(ref.typeDefinition.valueType.referencedType),
        "value" -> option2json(ref.refValueOpt, entityd2json),
    )
    case ref: BackReferenceValue => JsObject(
        "id" -> id2json(ref.value),
        "type" -> typeRef2json(ref.typeDefinition.valueType.referencedType),
        "value" -> option2json(ref.refValueOpt, entities => JsArray(entities.map(entityd2json).toList)),
    )
    
def option2json[T](value: Option[T], mapper: T => JsValue): JsValue = value match
    case Some(value) => JsObject(
        "filled" -> JsTrue, 
        "value" -> mapper(value), 
    )
    case None => JsObject(
        "filled" -> JsFalse, 
    )
    
def values2json(value: ValueTypes): JsValue =  value match
    case prim: RootPrimitiveValue[?, ?] => primitive2json(prim)
    case items: Seq[ItemValue] => JsArray(items.map(entityValue2json).toVector)
    case fields:  Map[String, EntityValue] => JsObject(fields.map((fieldName, fieldValue) => fieldName -> entityValue2json(fieldValue)))

def entityd2json(entity: Entity[?, ?]): JsObject = JsObject(
        "id" -> id2json(entity.id), 
        "value" -> values2json(entity.value), 
        "type" -> typeRef2json(entity.typeDefinition),
    )
    
def typeRef2json(typeDef: AbstractNamedType): JsValue = JsObject(
    "name" -> JsString(typeDef.name), 
    "id" -> JsString(typeDef.getId.toString)
)
        

def json2field(json: JsValue): RootPrimitiveValue[?, ?] = json match 
    case JsString(value) => StringValue(value)
    case JsNumber(value) =>
        if (value.isValidLong) LongValue(value.toLong)
        else DoubleValue(value.toDouble)
    case JsBoolean(value) => BooleanValue(value)
//    case JsArray(value) => ??? //ArrayType(value.map(json2field).toList)
//    case JsObject(value) => ??? //ObjectType(value.map((fieldName, fieldValue) => fieldName -> json2field(fieldValue)))
    case _ => deserializationError("Unknown EntityFieldType")


implicit object EntityFieldTypeFormat extends RootJsonFormat[EntityValue]:
    def write(fieldType: EntityValue): JsValue = entityValue2json(fieldType)
    def read(json: JsValue): EntityValue = ???


implicit object EntityIdFormat extends RootJsonFormat[EntityId[?, ?]]:
    def write(id: EntityId[?, ?]): JsValue = id2json(id)
    def read(value: JsValue): EntityId[?, ?] = value match 
        case JsNumber(value) => LongId(value.toLong)
        case JsString(str) => StringId(str)
        case _ => deserializationError("EntityId expected")
    

implicit object EntityFormat extends RootJsonFormat[Entity[?, ?]]:    
    def write(entity: Entity[?, ?]): JsObject = entityd2json(entity) 
    def read(json: JsValue): Entity[?, ?] = json.asJsObject.getFields("id", "fields") match 
        case Seq(id, fields) => ???
        case _ => deserializationError("Entity expected")
    

implicit val entityListFormat: RootJsonFormat[Seq[Entity[?, ?]]] = new RootJsonFormat[Seq[Entity[?, ?]]]:
    def write(entities: Seq[Entity[?, ?]]): JsValue = JsArray(entities.map(entityd2json).toVector)
    def read(json: JsValue): Seq[Entity[?, ?]] = json match
        case JsArray(array) => array.map(_.convertTo[Entity[?, ?]]).toList
        case _ => deserializationError("List[Entity] expected")

