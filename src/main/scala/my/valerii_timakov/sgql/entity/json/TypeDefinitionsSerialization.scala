package my.valerii_timakov.sgql.entity.json

import my.valerii_timakov.sgql.entity.domain.type_definitions.{EntityTypeDefinition, IntIdTypeDefinition, LongIdTypeDefinition, EntitySuperType, ObjectEntitySuperType, ObjectTypeDefinition, PrimitiveEntitySuperType, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, EntityType, EntityIdTypeDefinition, CustomPrimitiveTypeDefinition, ArrayTypeDefinition, ArrayEntitySuperType, AbstractTypeDefinition, AbstractEntityType}
import spray.json.{JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat, deserializationError, enrichAny}


implicit val entityTypeFormat: RootJsonFormat[AbstractEntityType] = new RootJsonFormat[AbstractEntityType]:
    override def write(typeDef: AbstractEntityType): JsValue =
        val kind = typeDef match
            case value: EntityType[?] => "EntityType"
            case value: ArrayEntitySuperType => "ArrayEntitySuperType"
            case value: ObjectEntitySuperType => "ObjectEntitySuperType"
            case value: PrimitiveEntitySuperType[?] => "PrimitiveEntitySuperType"
        val valueType = typeDef.valueType.toJson
        JsObject(
            "kind" -> JsString(kind),
            "name" -> JsString(typeDef.name),
            "valueType" -> typeDef.valueType.toJson
        )

    override def read(json: JsValue): EntityType[?] = ??? /* json match
            case JsObject(fields) =>
                (fields("name"), fields("idType"), fields("valueType")) match
                    case Seq(Some(JsString(name)), Some(JsString(idTypeName)), Some(fieldsType)) =>
                        EntityType(name, idType.convertTo[EntityIdTypeDefinition], fieldsType.convertTo[EntityFieldTypeDefinition])
            case _ => deserializationError("EntityType expected!")
            case _ => deserializationError("AbstractNamedEntityType expected!")*/


implicit val entityTypeListFormat: RootJsonFormat[Seq[AbstractEntityType]] = new RootJsonFormat[Seq[AbstractEntityType]]:
    def write(obj: Seq[AbstractEntityType]): JsValue = JsArray(obj.map(_.toJson).toVector)
    def read(json: JsValue): Seq[AbstractEntityType] = json match
        case JsArray(array) => array.map(_.convertTo[AbstractEntityType]).toList
        case _ => deserializationError("List[Entity] expected!")

implicit val entityTypeDefinitionFormat: RootJsonFormat[EntityTypeDefinition] = new RootJsonFormat[EntityTypeDefinition]:
    def write(value: EntityTypeDefinition): JsValue = value match
        case value: CustomPrimitiveTypeDefinition => value.asInstanceOf[AbstractTypeDefinition].toJson
        case value: ArrayTypeDefinition => value.asInstanceOf[AbstractTypeDefinition].toJson
        case value: ObjectTypeDefinition => value.asInstanceOf[AbstractTypeDefinition].toJson
        case value: TypeBackReferenceDefinition => value.asInstanceOf[AbstractTypeDefinition].toJson
        case value: TypeReferenceDefinition => value.asInstanceOf[AbstractTypeDefinition].toJson
    def read(json: JsValue): EntityTypeDefinition = ???

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


implicit val primitiveEntitySuperTypeFormat: RootJsonFormat[PrimitiveEntitySuperType[?]] = new RootJsonFormat[PrimitiveEntitySuperType[?]]:
    def write(obj: PrimitiveEntitySuperType[?]): JsValue = JsObject(
        "name" -> JsString(obj.name),
        "idType" -> obj.valueType.asInstanceOf[AbstractTypeDefinition].toJson,
    )

    def read(json: JsValue): PrimitiveEntitySuperType[?] = ??? //json match
//        case JsString(name) => PrimitiveEntitySuperType(RootPrimitiveTypeDefinition(name))
//        case _ => deserializationError("PrimitiveEntitySuperType expected!")


implicit val entityFieldTypeDefinitionFormat: RootJsonFormat[AbstractTypeDefinition] = new RootJsonFormat[AbstractTypeDefinition]:

    override def write(obj: AbstractTypeDefinition): JsValue = obj match
        case CustomPrimitiveTypeDefinition(idOrParentType) =>
            val parentName = idOrParentType.toOption.map(_.name).orElse(
                idOrParentType.left.toOption.map(_._2.name))
                .map(JsString)
                .getOrElse(JsNull)
            JsObject(
                "type" -> JsString("CustomPrimitive"),
                "parent" -> parentName,
                "id" -> idOrParentType.left.toOption.map(_._1.toJson).getOrElse(JsNull)
            )
        case RootPrimitiveTypeDefinition(name) => JsString(name)
        case ArrayTypeDefinition(elementsType, parentType) => JsObject(
            "type" -> JsString("Array"),
            "parent" -> parentType.map(p => JsString(p.name)).getOrElse(JsNull),
            "elements" -> JsArray(elementsType.map(v => JsString(v.name)).toVector))
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
