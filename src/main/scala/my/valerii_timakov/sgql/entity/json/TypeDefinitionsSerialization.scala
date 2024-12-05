package my.valerii_timakov.sgql.entity.json

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityIdTypeDefinition, AbstractRootPrimitiveTypeDefinition, AbstractTypeDefinition, ArrayTypeDefinition, CustomPrimitiveTypeDefinition, EntityTypeDefinition, IntIdTypeDefinition, LongIdTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, ArrayEntitySuperType, EntitySuperType, EntityType, ObjectEntitySuperType, PrimitiveEntitySuperType}
import spray.json.{JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat, deserializationError, enrichAny}

def abstractEntityType2json(typeDef: AbstractEntityType[_, _, _]): JsValue =
    val kind = typeDef match
        case value: EntityType[_, _, _] => "EntityType"
        case value: ArrayEntitySuperType[_, _] => "ArrayEntitySuperType"
        case value: ObjectEntitySuperType[_, _] => "ObjectEntitySuperType"
        case value: PrimitiveEntitySuperType[_, _, _] => "PrimitiveEntitySuperType"
    val valueType = entityTypeDefinition2json(typeDef.valueType)
    JsObject(
        "kind" -> JsString(kind),
        "name" -> JsString(typeDef.name),
        "valueType" -> valueType
    )

def entityTypeDefinition2json(value: AbstractTypeDefinition): JsValue = value match
    case value: CustomPrimitiveTypeDefinition[_, _, _] => value.asInstanceOf[AbstractTypeDefinition].toJson
    case value: ArrayTypeDefinition[_, _] => value.asInstanceOf[AbstractTypeDefinition].toJson
    case value: ObjectTypeDefinition[_, _] => value.asInstanceOf[AbstractTypeDefinition].toJson
    case value: TypeBackReferenceDefinition[_] => value.asInstanceOf[AbstractTypeDefinition].toJson
    case value: TypeReferenceDefinition[_] => value.asInstanceOf[AbstractTypeDefinition].toJson

implicit val entityTypeFormat: RootJsonFormat[AbstractEntityType[_, _, _]] = new RootJsonFormat[AbstractEntityType[_, _, _]]:
    override def write(typeDef: AbstractEntityType[_, _, _]): JsValue =
        abstractEntityType2json(typeDef)

    override def read(json: JsValue): EntityType[_, _, _] = ??? /* json match
            case JsObject(fields) =>
                (fields("name"), fields("idType"), fields("valueType")) match
                    case Seq(Some(JsString(name)), Some(JsString(idTypeName)), Some(fieldsType)) =>
                        EntityType(name, idType.convertTo[EntityIdTypeDefinition[_]], fieldsType.convertTo[EntityFieldTypeDefinition])
            case _ => deserializationError("EntityType expected!")
            case _ => deserializationError("AbstractNamedEntityType expected!")*/


implicit val entityTypeListFormat: RootJsonFormat[Seq[AbstractEntityType[_, _, _]]] = new RootJsonFormat[Seq[AbstractEntityType[_, _, _]]]:
    def write(obj: Seq[AbstractEntityType[_, _, _]]): JsValue = JsArray(obj.map(abstractEntityType2json).toVector)
    def read(json: JsValue): Seq[AbstractEntityType[_, _, _]] = json match
        case JsArray(array) => array.map(_.convertTo[AbstractEntityType[_, _, _]]).toList
        case _ => deserializationError("List[Entity] expected!")

implicit val entityTypeDefinitionFormat: RootJsonFormat[EntityTypeDefinition[_, _, _]] = new RootJsonFormat[EntityTypeDefinition[_, _, _]]:
    def read(json: JsValue): EntityTypeDefinition[_, _, _] = ??? //json match
    def write(value: EntityTypeDefinition[_, _, _]): JsValue =
        entityTypeDefinition2json(value)

implicit val entityIdTypeDefinitionFormat: RootJsonFormat[AbstractEntityIdTypeDefinition[_]] = new RootJsonFormat[AbstractEntityIdTypeDefinition[_]]:
    override def write(obj: AbstractEntityIdTypeDefinition[_]): JsValue = JsString(obj.name)
    override def read(json: JsValue): AbstractEntityIdTypeDefinition[_] = json match
        case JsString(value) => value match
            case IntIdTypeDefinition.name => IntIdTypeDefinition
            case LongIdTypeDefinition.name => LongIdTypeDefinition
            case StringIdTypeDefinition.name => StringIdTypeDefinition
            case UUIDIdTypeDefinition.name => UUIDIdTypeDefinition
            case _ => deserializationError("EntityIdTypeDefinition expected")
        case _ => deserializationError("EntityIdTypeDefinition expected")


implicit val primitiveEntitySuperTypeFormat: RootJsonFormat[PrimitiveEntitySuperType[_, _, _]] = new RootJsonFormat[PrimitiveEntitySuperType[_, _, _]]:
    def write(obj: PrimitiveEntitySuperType[_, _, _]): JsValue = JsObject(
        "name" -> JsString(obj.name),
        "idType" -> obj.valueType.asInstanceOf[AbstractTypeDefinition].toJson,
    )

    def read(json: JsValue): PrimitiveEntitySuperType[_, _, _] = ??? //json match
//        case JsString(name) => PrimitiveEntitySuperType(RootPrimitiveTypeDefinition(name))
//        case _ => deserializationError("PrimitiveEntitySuperType expected!")


implicit val entityFieldTypeDefinitionFormat: RootJsonFormat[AbstractTypeDefinition] = new RootJsonFormat[AbstractTypeDefinition]:

    override def write(obj: AbstractTypeDefinition): JsValue = obj match
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
        case ArrayTypeDefinition(elementsType, parentType) => JsObject(
            "type" -> JsString("Array"),
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "elements" -> JsArray(elementsType.getOrElse(Set.empty).map(v => JsString(v.name)).toVector))
        case ObjectTypeDefinition(fields, parentType) => JsObject(
            "type" -> JsString("Object"),
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "fields" -> JsObject(fields.map((fieldName, fieldType) => fieldName -> write(fieldType.valueType))))
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
            "fields" -> JsObject(fields.map((fieldName, fieldType) => fieldName -> write(fieldType.valueType))))


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

    override def read(json: JsValue): AbstractTypeDefinition = ??? /* json match
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
                        case Some(primitiveType) => FieldTypeDefinitions(primitiveType)
                        case None => FieldTypeDefinitions(PreParsedObjectTypeDefinitionHolder(name))
                    case JsArray(elementTypes) => FieldTypeDefinitions(readArrayType(elementTypes))
                ))
                ObjectTypeDefinition(name, fieldsMap, parent)
            case _ => deserializationError("EntityType expected!")
        case _ => deserializationError("EntityFieldTypeDefinition expected")
*/
