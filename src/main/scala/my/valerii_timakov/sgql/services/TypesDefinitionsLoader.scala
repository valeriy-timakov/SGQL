package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.*

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader

trait TypesDefinitionsLoader:
    def load(typesDefinitionResourcePath: String): Map[String, AbstractNamedEntityType]

object TypesDefinitionsLoader extends TypesDefinitionsLoader:
    def load(typesDefinitionResourcePath: String): Map[String, AbstractNamedEntityType] =
        val tdSource = Source.fromResource (typesDefinitionResourcePath)
        TypesDefinitionsParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: RootPackageData) =>
                val rawTypesDataMap = packageData.toMap
                val parser = new AbstractTypesParser(rawTypesDataMap)

                val typesMapBuilder = Map.newBuilder[String, AbstractNamedEntityType]
                rawTypesDataMap.foreach((name, definition) =>
                    val typePrefix = typeNamespace(name)
                    definition match
                        case PrimitiveTypeData(typeName, idTypeOpt, parentTypeName) =>
                            val parentType = parser.parsePrimitiveSupertypesChain(parentTypeName, typePrefix)
                            idTypeOpt match
                                case Some(idType) => typesMapBuilder += name -> EntityType(name, parseIdType(idType),
                                    CustomPrimitiveTypeDefinition(parentType))
                                case None => parser.parsePrimitiveSupertypesChain(name, typePrefix)
                        case ArrayTypeData(typeName, idTypeOpt, ArrayData(parentTypeNameOpt, elementTypesNames)) =>
                            val parentTypeOpt = parser.parseArraySupertypesChain(parentTypeNameOpt, typePrefix)
                            idTypeOpt match
                                case Some(idType) => typesMapBuilder += name -> EntityType(name, parseIdType(idType),
                                    ArrayTypeDefinition(Set.empty, parentTypeOpt))
                                case None => parser.parseArraySupertypesChain(Some(name), typePrefix)
                        case ObjectTypeData(typeName, idTypeOpt, ObjectData(parentTypeNameOpt, _)) =>
                            val parentTypeOpt = parser.parseObjectSupertypeChain(parentTypeNameOpt, typePrefix)
                            idTypeOpt match
                                case Some(idType) => typesMapBuilder += name -> EntityType(name, parseIdType(idType),
                                    ObjectTypeDefinition(Map.empty, parentTypeOpt))
                                case None => parser.parseObjectSupertypeChain(Some(name), typePrefix)
                )
                typesMapBuilder ++= parser.allParsedTypesMap
                val typesMap = typesMapBuilder.result()
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
    private def parseIdType(name: String) = idTypesMap.getOrElse(name, throw new NoIdTypeFound(name))


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
    private val primitiveSuperTypesMap = mutable.Map[String, PrimitiveEntitySuperType]()
    primitiveSuperTypesMap ++= typePredefsMap
    private val arraySuperTypesMap = mutable.Map[String, ArrayEntitySuperType]()
    private val objectSuperTypesMap: mutable.Map[String, ObjectEntitySuperType] = mutable.Map[String, ObjectEntitySuperType]()

    def allParsedTypesMap: mutable.Map[String, NamedEntitySuperType] =
        primitiveSuperTypesMap ++ arraySuperTypesMap ++ objectSuperTypesMap

    def parsePrimitiveSupertypesChain(typeName: String, typePrefixOpt: Option[String]): PrimitiveEntitySuperType =
        primitiveSuperTypesMap.getOrElse(typeName, {
            val result = findTypeDefinition(typeName, typePrefixOpt) match
                case (typeFullName, PrimitiveTypeData(_, None, parentTypeName)) =>
                    PrimitiveEntitySuperType(typeFullName, CustomPrimitiveTypeDefinition(
                        parsePrimitiveSupertypesChain(parentTypeName, typeNamespace(typeFullName))))
                case (typeFullName, PrimitiveTypeData(_, Some(_), parentTypeName)) =>
                    throw new NotAbstractParentTypeException(typeFullName)
                case (typeFullName, _) =>
                    throw new NotPrimitiveAbstractTypeException(typeFullName)
            primitiveSuperTypesMap += ((result.name, result))
            result
        })

    def parseArraySupertypesChain(typeNameOpt: Option[String], typePrefixOpt: Option[String]): Option[ArrayEntitySuperType] =
        typeNameOpt match
            case Some(ArrayTypeDefinition.name) => None
            case Some(typeName) =>
                arraySuperTypesMap
                    .get(typeName)
                    .orElse({
                        val result = findTypeDefinition(typeName, typePrefixOpt) match
                            case (typeFullName, ArrayTypeData(_, None, ArrayData(parentTypeNameOpt, _))) =>
                                val parent = parseArraySupertypesChain(parentTypeNameOpt, typeNamespace(typeFullName))
                                ArrayEntitySuperType(typeFullName, ArrayTypeDefinition(Set.empty, parent))
                            case (typeFullName, ArrayTypeData(_, Some(_), _)) =>
                                throw new NotAbstractParentTypeException(typeFullName)
                            case (typeFullName, _) =>
                                throw new NotArrayAbstractTypeException(typeFullName)
                        arraySuperTypesMap += ((result.name, result))
                        Some(result)
                    })
            case None => None


    def parseObjectSupertypeChain(typeNameOpt: Option[String], typePrefixOpt: Option[String]): Option[ObjectEntitySuperType] =
        typeNameOpt match
            case Some(ObjectTypeDefinition.name) => None
            case Some(typeName) =>
                objectSuperTypesMap
                    .get(typeName)
                    .orElse( {
                        val result = findTypeDefinition(typeName, typePrefixOpt) match
                            case (typeFullName, ObjectTypeData(_, None, ObjectData(parentTypeNameOpt, _))) =>
                                val parent = parseObjectSupertypeChain(parentTypeNameOpt, typeNamespace(typeFullName))
                                ObjectEntitySuperType(typeFullName, ObjectTypeDefinition(Map.empty, parent))
                            case (typeFullName, ObjectTypeData(_, Some(_), _)) =>
                                throw new NotAbstractParentTypeException(typeFullName)
                            case (typeFullName, _) =>
                                throw new NotObjectAbstractTypeException(typeFullName)
                        objectSuperTypesMap += ((result.name, result))
                        Some(result)
                    })
            case None => None

    private def findTypeDefinition(typeName: String, typePrefixOpt: Option[String]): (String, TypeData) =
        rawTypesDataMap.get(typeName)
            .map((typeName, _))
            .orElse(
                findUpstears(typeName, typePrefixOpt, rawTypesDataMap)
            )
            .getOrElse(throw new NoTypeFound(typeName))

    private def parseArraySimpleType(rawType: ArrayData,
                                     typePrefix: Option[String],
                                     typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        SimpleEntityType(ArrayTypeDefinition(rawType.elementTypesNames.map(parseAnyTypeDef(_, typePrefix, typesMap)).toSet,
            rawType.parentTypeName.map(parentTypeName =>
                arraySuperTypesMap.getOrElse(parentTypeName, throw new NoTypeFound(parentTypeName)))))

    private def parseObjectSimpleType(rawType: ObjectData,
                                      typePrefix: Option[String],
                                      typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        SimpleEntityType(ObjectTypeDefinition(rawType.fields.map(fieldRaw =>
            fieldRaw.name -> parseAnyTypeDef(fieldRaw.typeDef, typePrefix, typesMap)).toMap,
            rawType.parentTypeName.map(parentTypeName =>
                objectSuperTypesMap.getOrElse(parentTypeName, throw new NoTypeFound(parentTypeName)))))

    def parseAnyTypeDef(rowData: AnyTypeDef,
                        typePrefix: Option[String],
                        typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        rowData match
            case typeName: String => SimpleEntityType(
                typesMap.get(typeName)
                    .orElse(
                        findUpstears(typeName, typePrefix, typesMap)
                            .map(_._2)
                    )
                    .getOrElse(throw new NoTypeFound(typeName)).valueType)
            case typeDefRaw: ArrayData => parseArraySimpleType(typeDefRaw, typePrefix, typesMap)
            case typeDefRaw: ObjectData => parseObjectSimpleType(typeDefRaw, typePrefix, typesMap)


    @tailrec
    private def findUpstears[T](typeName: String, typePrefixOpt: Option[String], map: Map[String, T]): Option[(String, T)] =
        typePrefixOpt match
            case None => None
            case Some(typePrefix) =>
                map.get(typePrefix + typeName) match
                    case Some(value) => Some(typePrefix + typeName, value)
                    case None =>
                        val nextPrefix = typeNamespace(typePrefix.replaceFirst("\\.$", ""))
                        findUpstears(typeName, nextPrefix, map)


