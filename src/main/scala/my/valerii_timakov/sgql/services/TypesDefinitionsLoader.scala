package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.*
import my.valerii_timakov.sgql.services.TypesDefinitionsParser.IdTypeRef

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader

trait TypesDefinitionsLoader:
    def load(typesDefinitionResourcePath: String): Map[String, AbstractNamedEntityType]

object TypesDefinitionsLoader extends TypesDefinitionsLoader:
    private val specialConcreteSuffix = "Concrete"
    def load(typesDefinitionResourcePath: String): Map[String, AbstractNamedEntityType] =
        val tdSource = Source.fromResource (typesDefinitionResourcePath)
        TypesDefinitionsParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: TypesRootPackageData) =>
                val rawTypesDataMap = packageData.toMap
                val parser = new AbstractTypesParser(rawTypesDataMap)

                val typesMapBuilder = Map.newBuilder[String, AbstractNamedEntityType]
                rawTypesDataMap.foreach((name, definition) =>
                    val typePrefix = typeNamespace(name)
                    definition match
                        case PrimitiveTypeData(typeName, idTypeOpt, parentTypeName, mandatoryConrete) =>
                            val parent = parser.parsePrimitiveSupertypesChain(name, idTypeOpt, parentTypeName, typePrefix)
                            if (mandatoryConrete) {
                                typesMapBuilder += (name + specialConcreteSuffix) -> 
                                    EntityType(name, CustomPrimitiveTypeDefinition(Right(parent)))
                            }
                        case ArrayTypeData(typeName, ArrayData(idOrParent, elementTypesNames), mandatoryConrete) =>
                            val parent = parser.parseArraySupertypesChain(name, idOrParent, typePrefix)
                            if (mandatoryConrete) {
                                typesMapBuilder += (name + specialConcreteSuffix) -> 
                                    EntityType(name, ArrayTypeDefinition(Set.empty, Right(parent)))
                            }
                        case ObjectTypeData(typeName, ObjectData(idOrParent, _), mandatoryConrete) =>
                            val parent = parser.parseObjectSupertypeChain(name, idOrParent, typePrefix)
                            if (mandatoryConrete) {
                                typesMapBuilder += (name + specialConcreteSuffix) -> 
                                    EntityType(name, ObjectTypeDefinition(Map.empty, Right(parent)))
                            }
                )
                typesMapBuilder ++= parser.allParsedTypesMap
                val typesMapPre = typesMapBuilder.result()
                val typesMap = typesMapPre
                    .map((typeFullName, typeEntityDef) =>
                        val children = getChildren(typeFullName, typesMapPre)
                        if (children.isEmpty) {
                            typeFullName -> EntityType(typeEntityDef.name, typeEntityDef.valueType)
                        } else {
                            typeFullName -> typeEntityDef
                        }
                    )
                typesMap.foreach((typeFullName, typeEntityDef) =>
                    typeEntityDef.valueType match
                        case arrDef: ArrayTypeDefinition =>
                            val typePrefix = typeNamespace(typeFullName)
                            rawTypesDataMap.get(typeFullName) match
                                case Some(ArrayTypeData(_, _, ArrayData(_, elementTypesNames))) =>
                                    val elementTypes = elementTypesNames.map(parser.parseAnyTypeDef(_, typePrefix, typesMap))
                                    arrDef.setChildren(elementTypes.toSet)
                                case _ => throw new NoTypeFound(typeFullName)
                        case objDef: ObjectTypeDefinition =>
                            val typePrefix = typeNamespace(typeFullName)
                            rawTypesDataMap.get(typeFullName) match
                                case Some(ObjectTypeData(_, _, ObjectData(_, fields))) =>
                                    val fieldsMap = fields.map(fieldRaw =>
                                        fieldRaw.name -> parser.parseAnyTypeDef(fieldRaw.typeDef, typePrefix, typesMap)).toMap
                                    objDef.setChildren(fieldsMap)
                                case _ => throw new NoTypeFound(typeFullName)
                        case _ => // do nothing
                )
                typesMap
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)
    
    private def getChildren(parentName: String, typesMap: Map[String, AbstractNamedEntityType]): List[AbstractNamedEntityType] =
        typesMap.values.filter(typeDef => typeDef match
            case EntityType(_, valueType) => 
                valueType.parent.exists(parentName == _.name) 
            case namedEntitySuperType: NamedEntitySuperType =>
                namedEntitySuperType.valueType.parent.exists(parentName == _.name) 
        ).toList 


def typeNamespace(typeName: String): Option[String] =
    val prefix = typeName.replaceFirst("\\w+$", "")
    if (prefix.isEmpty)
        None
    else if (prefix.endsWith("."))
        Some(prefix)
    else
        throw new WrongTypeNameException(typeName)



private class AbstractTypesParser(rawTypesDataMap: Map[String, TypeData]):
    private val typePredefsMap = primitiveFieldTypesMap
        .map((name, typeDef) => name -> PrimitiveEntitySuperType(name, typeDef))
    private val primitiveSuperTypesMap = mutable.Map[String, PrimitiveEntitySuperType[CustomPrimitiveTypeDefinition]]()
    private val arraySuperTypesMap = mutable.Map[String, ArrayEntitySuperType]()
    private val objectSuperTypesMap: mutable.Map[String, ObjectEntitySuperType] = mutable.Map[String, ObjectEntitySuperType]()

    def allParsedTypesMap: mutable.Map[String, NamedEntitySuperType] =
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
                case Left(idType) => Left(parseIdType(idType.name))
                case Right(parentTypeName) =>
                    findTypeDefinition(parentTypeName, typePrefixOpt) match
                        case (parentTypeFullName, ObjectTypeData(_, ObjectData(parentIdOrParent, _), _)) =>
                            Right( parseObjectSupertypeChain(parentTypeFullName, parentIdOrParent, typeNamespace(parentTypeFullName)) )
                        case (typeFullName, _) =>
                            throw new NotObjectAbstractTypeException(typeFullName)
            val result = ObjectEntitySuperType(typeName, ObjectTypeDefinition(Map.empty, idOrParentType))
            objectSuperTypesMap += ((result.name, result))
            result
        })      

    private def parseIdType(name: String) = idTypesMap.getOrElse(name, throw new NoIdTypeFound(name))

    private def findTypeDefinition(typeName: String, typePrefixOpt: Option[String]): (String, TypeData) =
        rawTypesDataMap.get(typeName)
            .map((typeName, _))
            .orElse(
                findInPackagesUpstears(typeName, typePrefixOpt, rawTypesDataMap)
            )
            .getOrElse(throw new NoTypeFound(typeName))

    def parseAnyTypeDef(rowData: AnyTypeDef,
                        typePrefix: Option[String],
                        typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        rowData match
            case refData: ReferenceData => 
                typesMap.get(refData.refTypeName)
                    .orElse(
                        findInPackagesUpstears(refData.refTypeName, typePrefix, typesMap)
                            .map(_._2)
                    )
                    .getOrElse(throw new NoTypeFound(refData.refTypeName)) match
                        case PrimitiveEntitySuperType(name, valueType: RootPrimitiveTypeDefinition) =>
                            SimpleEntityType(valueType)
                        case entityType: EntityType =>
                            SimpleEntityType(TypeReferenceDefinition(entityType, refData.refFieldName))
                        case objectEntitySuperType: ObjectEntitySuperType =>
                            SimpleEntityType(TypeReferenceDefinition(objectEntitySuperType, refData.refFieldName))
                        case anyTypeDefinition: AbstractNamedEntityType => 
                            SimpleEntityType(TypeReferenceDefinition(anyTypeDefinition, None))
            case typeDefRaw: ObjectData => parseObjectSimpleType(typeDefRaw, typePrefix, typesMap)



    private def parseObjectSimpleType(rawType: ObjectData,
                                      typePrefix: Option[String],
                                      typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        SimpleEntityType(ObjectTypeDefinition(rawType.fields.map(fieldRaw =>
            fieldRaw.name -> parseAnyTypeDef(fieldRaw.typeDef, typePrefix, typesMap)).toMap,
            rawType.idOrParent
                .left.map(idTypeData => parseIdType(idTypeData.name))
                .map(parentTypeName => objectSuperTypesMap.getOrElse(parentTypeName, throw new NoTypeFound(parentTypeName))
            )
        ))


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


