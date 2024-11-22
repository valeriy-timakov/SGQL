package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.TypesDefinitionsParseError
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, ArrayEntitySuperType, ArrayEntityType, ArrayItemTypeDefinition, ArrayTypeDefinition, CustomPrimitiveEntityType, CustomPrimitiveTypeDefinition, EntityIdTypeDefinition, EntitySuperType, EntityType, FieldTypeDefinition, ObjectEntitySuperType, ObjectEntityType, ObjectTypeDefinition, PrimitiveEntitySuperType, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, idTypesMap, primitiveFieldTypesMap}
import my.valerii_timakov.sgql.exceptions.*
import my.valerii_timakov.sgql.services.TypesDefinitionsParser.IdTypeRef

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader

trait TypesDefinitionsLoader:
    def load(typesDefinitionResourcePath: String): Map[String, AbstractEntityType]

object TypesDefinitionsLoader extends TypesDefinitionsLoader:
    private val specialConcreteSuffix = "Concrete"
    def load(typesDefinitionResourcePath: String): Map[String, AbstractEntityType] =
        val tdSource = Source.fromResource (typesDefinitionResourcePath)
        TypesDefinitionsParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: TypesRootPackageData) =>
                var rawTypesDataMap = packageData.toMap
                val parser = new AbstractTypesParser(rawTypesDataMap)

                val typesMapBuilder = Map.newBuilder[String, AbstractEntityType]
                val generatedConcreteTypes = mutable.Map[String, TypeData]()
                rawTypesDataMap.foreach((name, definition) =>
                    val typePrefix = typeNamespace(name)
                    definition match
                        case PrimitiveTypeData(_, idTypeOpt, parentTypeName, mandatoryConrete) =>
                            val parent = parser.parsePrimitiveSupertypesChain(name, idTypeOpt, parentTypeName, typePrefix)
                            if (mandatoryConrete) {
                                val concreteName = name + specialConcreteSuffix
                                typesMapBuilder += concreteName ->
                                    CustomPrimitiveEntityType(concreteName, CustomPrimitiveTypeDefinition(Right(parent)))
                                generatedConcreteTypes += concreteName -> 
                                    PrimitiveTypeData(concreteName, None, name, mandatoryConrete)
                            }
                        case ArrayTypeData(_, ArrayData(idOrParent, elementTypesNames), mandatoryConrete) =>
                            val parent = parser.parseArraySupertypesChain(name, idOrParent, typePrefix)
                            if (mandatoryConrete) {
                                val concreteName = name + specialConcreteSuffix
                                typesMapBuilder += concreteName ->
                                    ArrayEntityType(concreteName, ArrayTypeDefinition(Set.empty, Right(parent)))
                                generatedConcreteTypes += concreteName -> 
                                    ArrayTypeData(concreteName, ArrayData(Right(name), List()), mandatoryConrete)
                            }
                        case ObjectTypeData(_, ObjectData(idOrParent, _), mandatoryConrete) =>
                            val parent = parser.parseObjectSupertypeChain(name, idOrParent, typePrefix)
                            if (mandatoryConrete) {
                                val concreteName = name + specialConcreteSuffix
                                typesMapBuilder += concreteName ->
                                    ObjectEntityType(concreteName, ObjectTypeDefinition(Map.empty, Right(parent)))
                                generatedConcreteTypes += concreteName -> 
                                    ObjectTypeData(concreteName, ObjectData(Right(name), List()), mandatoryConrete)
                            }
                )
                rawTypesDataMap ++= generatedConcreteTypes
                typesMapBuilder ++= parser.allParsedTypesMap
                val typesMapPre = typesMapBuilder.result()
                val typesMap:  Map[String, AbstractEntityType] = typesMapPre
                    .map((typeFullName, typeEntityDef) =>
                        val children = getChildren(typeFullName, typesMapPre)
                        if (children.isEmpty) {
                            //convert super types without children to entity types
                            typeFullName -> (typeEntityDef match
                                case ArrayEntitySuperType(name, valueType) =>
                                    ArrayEntityType(name, valueType)
                                case ObjectEntitySuperType(name, valueType) =>
                                    ObjectEntityType(name, valueType)
                                case PrimitiveEntitySuperType(name, valueType) =>
                                    CustomPrimitiveEntityType(name, valueType)
                                case _ => typeEntityDef
                                )
                        } else {
                            typeFullName -> typeEntityDef
                        }
                    )
                typesMap.foreach((typeFullName, typeEntityDef) =>
                    typeEntityDef.valueType match
                        case arrDef: ArrayTypeDefinition =>
                            val typePrefix = typeNamespace(typeFullName)
                            rawTypesDataMap.get(typeFullName) match
                                case Some(ArrayTypeData(_, ArrayData(_, elementTypesNames), _)) =>
                                    val elementTypes = elementTypesNames.map(parser.parseArrayItemTypeDef(_, typePrefix, typesMap))
                                    arrDef.setChildren(elementTypes.toSet)
                                case _ => throw new NoTypeFound(typeFullName)
                        case objDef: ObjectTypeDefinition =>
                            val typePrefix = typeNamespace(typeFullName)
                            rawTypesDataMap.get(typeFullName) match
                                case Some(ObjectTypeData(_, ObjectData(_, fields), _)) =>
                                    val fieldsMap = fields.map(fieldRaw =>
                                        fieldRaw.name -> parser.parseAnyTypeDef(fieldRaw, typePrefix, typesMap)).toMap
                                    objDef.setChildren(fieldsMap)
                                case _ => throw new NoTypeFound(typeFullName)
                        case _ => // do nothing
                )
                typesMap
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)
    
    private def getChildren(parentName: String, typesMap: Map[String, AbstractEntityType]): List[AbstractEntityType] =
        typesMap.values.filter(typeDef => typeDef match
            case entityType: EntityType[?, ?] =>
                entityType.valueType.parent.exists(parentName == _.name)
            case namedEntitySuperType: EntitySuperType =>
                namedEntitySuperType.valueType.parent.exists(parentName == _.name) 
        ).toList 


def typeNamespace(typeName: String): Option[String] =
    val prefix = typeName.replaceFirst("\\w+$", "")
    if (prefix.isEmpty)
        None
    else if (prefix.endsWith(TypesDefinitionsParser.NAMESPACES_DILIMITER.toString))
        Some(prefix)
    else
        throw new WrongTypeNameException(typeName)



private class AbstractTypesParser(rawTypesDataMap: Map[String, TypeData]):
    private val typePredefsMap = primitiveFieldTypesMap
    private val primitiveSuperTypesMap = mutable.Map[String, PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]]()
    private val arraySuperTypesMap = mutable.Map[String, ArrayEntitySuperType]()
    private val objectSuperTypesMap: mutable.Map[String, ObjectEntitySuperType] = mutable.Map[String, ObjectEntitySuperType]()

    def allParsedTypesMap: mutable.Map[String, EntitySuperType] =
        primitiveSuperTypesMap ++ arraySuperTypesMap ++ objectSuperTypesMap

    def parsePrimitiveSupertypesChain (typeName: String, idType: Option[String], parentTypeName: String,
                                       typePrefixOpt: Option[String]): PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition] =
        primitiveSuperTypesMap.getOrElse(typeName, {

            val parent = typePredefsMap.get(parentTypeName) match
                case Some(rootPrimitiveType) => 
                    idType match
                        case Some(idTypeName) =>
                            Left( (parseIdType(idTypeName), rootPrimitiveType) )
                        case None =>
                            throw new NoIdentityForRootSupetType(typeName)                    
                case None =>
                    if (idType.isDefined) {
                        throw new IdentitySetForNonRootType(typeName)
                    }
                    findTypeDefinition(parentTypeName, typePrefixOpt) match
                        case (typeFullName, PrimitiveTypeData(_, parentIdType, superParentTypeName, _)) =>
                            Right( parsePrimitiveSupertypesChain(parentTypeName, parentIdType, superParentTypeName, typeNamespace(typeFullName)) )
                        case (typeFullName, _) =>
                            throw new NotPrimitiveAbstractTypeException(typeFullName)
            val result = PrimitiveEntitySuperType(typeName, CustomPrimitiveTypeDefinition(parent))
            primitiveSuperTypesMap += ((result.name, result))
            result
        })

    def parseArraySupertypesChain(typeName: String, idOrParent: Either[IdTypeRef, String], typePrefixOpt: Option[String]): ArrayEntitySuperType =
        arraySuperTypesMap.getOrElse(typeName, {
            val idOrParentType: Either[EntityIdTypeDefinition, ArrayEntitySuperType] = idOrParent match
                case Left(idType) => Left(parseIdType(idType.name))
                case Right(parentTypeName) =>
                    findTypeDefinition(parentTypeName, typePrefixOpt) match
                        case (parentTypeFullName, ArrayTypeData(_, ArrayData(parentIdOrParent, _), _)) =>
                            Right( parseArraySupertypesChain(parentTypeFullName, parentIdOrParent, typeNamespace(parentTypeFullName)) )
                        case (typeFullName, _) =>
                            throw new NotArrayAbstractTypeException(typeFullName)
            val result = ArrayEntitySuperType(typeName, ArrayTypeDefinition(Set.empty, idOrParentType))
            arraySuperTypesMap += ((result.name, result))
            result
        })


    def parseObjectSupertypeChain(typeName: String, idOrParent: Either[IdTypeRef, String], typePrefixOpt: Option[String]): ObjectEntitySuperType =
        objectSuperTypesMap.getOrElse(typeName, {
            val idOrParentType: Either[EntityIdTypeDefinition, ObjectEntitySuperType] = idOrParent match
                case Left(idType) =>
                    Left(parseIdType(idType.name))
                case Right(parentTypeName) =>
                    Right( findOrParseObjectSuperType(parentTypeName, typePrefixOpt) )
            val result = ObjectEntitySuperType(typeName, ObjectTypeDefinition(Map.empty, idOrParentType))
            objectSuperTypesMap += ((result.name, result))
            result
        })

    private def findOrParseObjectSuperType(typeName: String, typePrefixOpt: Option[String]): ObjectEntitySuperType =
        findTypeDefinition(typeName, typePrefixOpt) match
            case (parentTypeFullName, ObjectTypeData(_, ObjectData(idOrParent, _), _)) =>
                parseObjectSupertypeChain(parentTypeFullName, idOrParent, typeNamespace(parentTypeFullName))
            case (typeFullName, _) =>
                throw new NotObjectAbstractTypeException(typeFullName)

    private def parseIdType(name: String) = idTypesMap.getOrElse(name, throw new NoIdTypeFound(name))

    private def findTypeDefinition(typeName: String, typePrefixOpt: Option[String]): (String, TypeData) =
        rawTypesDataMap.get(typeName)
            .map((typeName, _))
            .orElse(
                findInPackagesUpstears(typeName, typePrefixOpt, rawTypesDataMap)
            )
            .getOrElse(throw new NoTypeFound(typeName))

    def parseAnyTypeDef(
                           fieldData: FieldData,
                           typePrefix: Option[String],
                           typesMap: Map[String, AbstractEntityType],
    ): FieldTypeDefinition =
        fieldData.typeDef match
            case refData: ReferenceData =>
                FieldTypeDefinition(parseReferenceType(refData, typePrefix, typesMap), fieldData.isNullable)
            case typeDefRaw: SimpleObjectData => 
                parseObjectSimpleType(typeDefRaw, typePrefix, typesMap, fieldData.isNullable)
                
    def parseArrayItemTypeDef(rowData: ReferenceData,
                              typePrefix: Option[String],
                              typesMap: Map[String, AbstractEntityType]): ArrayItemTypeDefinition =
        parseReferenceType(rowData, typePrefix, typesMap) match
            case refData: TypeReferenceDefinition =>
                ArrayItemTypeDefinition(refData)
            case refData: RootPrimitiveTypeDefinition =>
                ArrayItemTypeDefinition(refData)
            case _ =>
                throw new ConsistencyException("Only reference or root primitive types could be array items! " +
                    s"Type ${rowData.refTypeName} is trying to be array item!")

    private def parseReferenceType(refData: ReferenceData,
                                   typePrefix: Option[String],
                                   typesMap: Map[String, AbstractEntityType]
                                  ): RootPrimitiveTypeDefinition | TypeReferenceDefinition | TypeBackReferenceDefinition =
        val referencedTypeOpt: Option[AbstractEntityType] =
            typesMap.get(refData.refTypeName)
                .orElse(
                    findInPackagesUpstears(refData.refTypeName, typePrefix, typesMap)
                        .map(_._2)
                )
        refData.refFieldName match
            case Some(refFieldName) => 
                val referencedType: AbstractEntityType = referencedTypeOpt.getOrElse(
                    if (typePredefsMap.contains(refData.refTypeName))
                        throw new ConsistencyException("Root primitive type couldnot be referenced by as back " +
                            s"reference! Type ${refData.refTypeName} is trying to be referenced by ${refData.refFieldName}!")
                    else
                        throw new NoTypeFound(refData.refTypeName))
                referencedType.valueType match
                    case _: ObjectTypeDefinition =>
                        TypeBackReferenceDefinition(referencedType, refFieldName)
                    case anyTypeDefinition: AbstractEntityType =>
                        throw new ConsistencyException("Only object types could be referenced by back reference! " +
                            s"Type ${refData.refTypeName} is trying to be referenced by ${refData.refFieldName}!")
            case None =>
                referencedTypeOpt match
                    case Some(anyTypeDefinition) =>
                        TypeReferenceDefinition(anyTypeDefinition)
                    case None =>
                        typePredefsMap.get(refData.refTypeName) match
                            case Some(valueType) =>
                                valueType
                            case None =>
                                throw new NoTypeFound(refData.refTypeName)
                

    private def parseObjectSimpleType(
                                         rawType: SimpleObjectData,
                                         typePrefixOpt: Option[String],
                                         typesMap: Map[String, AbstractEntityType],
                                         isNullable: Boolean, 
    ): FieldTypeDefinition =
        FieldTypeDefinition(SimpleObjectTypeDefinition(rawType.fields.map(fieldRaw =>
            fieldRaw.name -> parseAnyTypeDef(fieldRaw, typePrefixOpt, typesMap)).toMap,
            rawType.parent.map(parentTypeName =>
                findOrParseObjectSuperType(parentTypeName, typePrefixOpt)
            )
        ), isNullable)


    @tailrec
    private def findInPackagesUpstears[T](typeName: String, 
                                          typePrefixOpt: Option[String], 
                                          map: Map[String, T]): Option[(String, T)] =
        typePrefixOpt match
            case None => None
            case Some(typePrefix) =>
                map.get(typePrefix + typeName) match
                    case Some(value) => Some(typePrefix + typeName, value)
                    case None =>
                        val nextPrefix = typeNamespace(typePrefix.replaceFirst("\\.$", ""))
                        findInPackagesUpstears(typeName, nextPrefix, map)


