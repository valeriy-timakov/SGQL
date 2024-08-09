package my.valerii_timakov.sgql.entity.json

import akka.parboiled2.util.Base64
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.{AbstractEntityType, AbstractNamedEntityType, ArrayType, ArrayTypeDefinition, BinaryType, BinaryTypeDefinition, BooleanType, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeType, DateTimeTypeDefinition, DateType, DateTypeDefinition, DoubleType, DoubleTypeDefinition, Entity, EntityFieldType, EntityFieldTypeDefinition, EntityId, EntityIdTypeDefinition, EntityType, FloatType, FloatTypeDefinition, IntId, IntIdTypeDefinition, IntType, IntTypeDefinition, LongId, LongIdTypeDefinition, LongType, LongTypeDefinition, NamedEntitySuperType, ObjectType, ObjectTypeDefinition, PrimitiveFieldTypeDefinition, RootPrimitiveTypeDefinition, SimpleEntityType, SimpleTypeDefinitions, StringId, StringIdTypeDefinition, StringType, StringTypeDefinition, TimeType, TimeTypeDefinition, TypeReferenceDefinition, UUIDId, UUIDIdTypeDefinition}
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

//------definitions------

implicit val entityIdTypeDefinitionFormat: RootJsonFormat[EntityIdTypeDefinition] = new RootJsonFormat[EntityIdTypeDefinition]:    
    override def write(obj: EntityIdTypeDefinition): JsValue = JsString(obj.name)
    override def read(json: JsValue): EntityIdTypeDefinition = json match 
        case JsString(value) => value match
            case IntIdTypeDefinition.name => IntIdTypeDefinition
            case LongIdTypeDefinition.name => LongIdTypeDefinition
            case StringIdTypeDefinition.name => StringIdTypeDefinition
            case UUIDIdTypeDefinition.name => UUIDIdTypeDefinition
            case _ => deserializationError("EntityIdTypeDefinition expected")
        case _ => deserializationError("EntityIdTypeDefinition expected")

//TypeReferenceDefinition | RootPrimitiveTypeDefinition | ObjectTypeDefinition | ArrayTypeDefinition
//implicit val simpleTypeDefinitionsFormat: RootJsonFormat[SimpleTypeDefinitions] = new RootJsonFormat[SimpleTypeDefinitions]:
//    override def write(item: SimpleTypeDefinitions): JsValue = item match
//        case td: TypeReferenceDefinition => td.asInstanceOf[EntityFieldTypeDefinition].toJson
//        case td: RootPrimitiveTypeDefinition => td.asInstanceOf[EntityFieldTypeDefinition].toJson
//        case td: ObjectTypeDefinition => td.asInstanceOf[EntityFieldTypeDefinition].toJson
//        case td: ArrayTypeDefinition => td.asInstanceOf[EntityFieldTypeDefinition].toJson
//
//    override def read(json: JsValue): SimpleTypeDefinitions = json.asJsObject.getFields("idTypes", "fieldTypes") match
//        case Seq(JsArray(idTypes), JsObject(fieldTypes)) => SimpleTypeDefinitions(
//            idTypes.map(_.convertTo[EntityIdTypeDefinition]).toSet,
//            fieldTypes.map((fieldName, fieldType) => fieldName -> fieldType.convertTo[EntityFieldTypeDefinition]).toMap)
//        case _ => deserializationError("SimpleTypeDefinitions expected")

implicit val entityFieldTypeDefinitionFormat: RootJsonFormat[EntityFieldTypeDefinition] = new RootJsonFormat[EntityFieldTypeDefinition]:    

    private def writeEntityType(entityDef: AbstractEntityType): JsValue = entityDef match
        case namedEntityDef: AbstractNamedEntityType => JsString(namedEntityDef.name)
        case SimpleEntityType(valueType) => valueType.asInstanceOf[EntityFieldTypeDefinition].toJson

    override def write(obj: EntityFieldTypeDefinition): JsValue = obj match
        case CustomPrimitiveTypeDefinition(idOrParentType) => JsObject(
            "type" -> JsString("CustomPrimitive"),
            "parent" -> JsString(idOrParentType.toOption.map(_.name).orNull), 
            "id" -> JsString(idOrParentType.left.toOption.map(_._1.name).orNull)
        )
        case RootPrimitiveTypeDefinition(name) => JsString(name)
        case ArrayTypeDefinition(elementsType, parentType) => JsObject(
            "type" -> JsString("Array"), 
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "elements" -> JsArray(elementsType.map(writeEntityType).toVector))
        case ObjectTypeDefinition(fields, parentType) => JsObject(
            "type" -> JsString("Object"),
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "fields" -> JsObject(fields.map((fieldName, fieldType) => fieldName -> writeEntityType(fieldType))))
        case TypeReferenceDefinition(referencedType, refFieldOpt) => JsObject(
            "type" -> JsString("Reference"),
            "referencedType" -> JsString(referencedType.name),
            "refField" -> (refFieldOpt match 
                case Some(refField: String) => JsString(refField)
                case None => JsNull))
            
//    def readPrimitiveType(name: String): Option[PrimitiveFieldTypeDefinition] = name match
//        case "String" => Some(StringTypeDefinition)
//        case "Int" => Some(IntTypeDefinition)
//        case "Long" => Some(LongTypeDefinition)
//        case "Float" => Some(FloatTypeDefinition)
//        case "Double" => Some(DoubleTypeDefinition)
//        case "Boolean" => Some(BooleanTypeDefinition)
//        case "Date" => Some(DateTypeDefinition)
//        case "DateTime" => Some(DateTimeTypeDefinition)
//        case "Time" => Some(TimeTypeDefinition)
//        case "Binary" => Some(BinaryTypeDefinition)
//        case _ => None
//        
//    def readArrayType(elementTypes: Vector[JsValue]): ArrayTypeDefinition =
//        ArrayTypeDefinition(elementTypes.map(read).toSet)
        
    override def read(json: JsValue): EntityFieldTypeDefinition = ??? /* json match 
        case JsString(value) => readPrimitiveType(value) match
            case Some(typeValue) => typeValue
            case None => deserializationError("EntityFieldTypeDefinition expected")
        case JsArray(elementTypes) => readArrayType(elementTypes)
        case obj: JsObject =>obj.getFields("name", "fields") match
            case Seq(JsString(name), JsObject(fields)) =>                
                val parent = obj.fields.get("parent").map {
                    case JsString(parentStr) => PreParsedObjectTypeDefinitionHolder(parentStr)
                    case _ => deserializationError("Parent type name is not String!")
                }
                val fieldsMap = fields.map((fieldName, fieldType) => fieldName -> (fieldType match
                    case JsString(name) => readPrimitiveType(name) match
                        case Some(primitiveType) => SimpleEntityType(primitiveType)
                        case None => SimpleEntityType(PreParsedObjectTypeDefinitionHolder(name))
                    case JsArray(elementTypes) => SimpleEntityType(readArrayType(elementTypes))
                ))
                ObjectTypeDefinition(name, fieldsMap, parent)
            case _ => deserializationError("EntityType expected!")
        case _ => deserializationError("EntityFieldTypeDefinition expected")
*/

implicit val entityTypeFormat: RootJsonFormat[AbstractNamedEntityType] = new RootJsonFormat[AbstractNamedEntityType]:    
    override def write(obj: AbstractNamedEntityType): JsValue = obj match
        case EntityType(name, valueType) => JsObject(
            "name" -> JsString(name),
            "idType" -> valueType.idType.toJson,
            "valueType" -> valueType.toJson,
        )
        case superType: NamedEntitySuperType => JsObject(
            "name" -> JsString(superType.name),
            "valueType" -> superType.valueType.toJson,
        )    
    override def read(json: JsValue): EntityType = ??? /* json match
        case JsObject(fields) =>
            (fields("name"), fields("idType"), fields("valueType")) match
                case Seq(Some(JsString(name)), Some(JsString(idTypeName)), Some(fieldsType)) =>
                    EntityType(name, idType.convertTo[EntityIdTypeDefinition], fieldsType.convertTo[EntityFieldTypeDefinition])
        case _ => deserializationError("EntityType expected!")
        case _ => deserializationError("AbstractNamedEntityType expected!")*/


implicit val entityTypeListFormat: RootJsonFormat[Seq[AbstractNamedEntityType]] = new RootJsonFormat[Seq[AbstractNamedEntityType]]:
    def write(obj: Seq[AbstractNamedEntityType]): JsValue = JsArray(obj.map(_.toJson).toVector)
    def read(json: JsValue): Seq[AbstractNamedEntityType] = json match
        case JsArray(array) => array.map(_.convertTo[AbstractNamedEntityType]).toList
        case _ => deserializationError("List[Entity] expected!")

