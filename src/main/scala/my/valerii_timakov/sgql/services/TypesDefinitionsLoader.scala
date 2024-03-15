package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.*

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
                val parser = new AbstractTypesParser

                val typesMapBuilder = Map.newBuilder[String, AbstractNamedEntityType]
                rawTypesDataMap.foreach((name, definition) =>
                    definition match
                        case PrimitiveTypeData(typeName, idTypeOpt, parentTypeName) =>
                            val parentType = parser.parsePrimitiveSupertypesChain(parentTypeName, rawTypesDataMap)
                            idTypeOpt match
                                case Some(idType) => typesMapBuilder += name -> EntityType(name, parseIdType(idType),
                                    CustomPrimitiveTypeDefinition(parentType))
                                case None => parser.parsePrimitiveSupertypesChain(name, rawTypesDataMap)
                        case ArrayTypeData(typeName, idTypeOpt, ArrayData(parentTypeNameOpt, elementTypesNames)) =>
                            val parentTypeOpt = parser.parseArraySupertypesChain(parentTypeNameOpt, rawTypesDataMap)
                            idTypeOpt match
                                case Some(idType) => typesMapBuilder += name -> EntityType(name, parseIdType(idType),
                                    ArrayTypeDefinition(Set.empty, parentTypeOpt))
                                case None => parser.parseArraySupertypesChain(Some(name), rawTypesDataMap)
                        case ObjectTypeData(typeName, idTypeOpt, ObjectData(parentTypeNameOpt, _)) =>
                            val parentTypeOpt = parser.parseObjectSupertypeChain(parentTypeNameOpt, rawTypesDataMap)
                            idTypeOpt match
                                case Some(idType) => typesMapBuilder += name -> EntityType(name, parseIdType(idType),
                                    ObjectTypeDefinition(Map.empty, parentTypeOpt))
                                case None => parser.parseObjectSupertypeChain(Some(name), rawTypesDataMap)
                )
                typesMapBuilder ++= parser.allParsedTypesMap
                val typesMap = typesMapBuilder.result()
                typesMap.values.foreach(typeEntityDef =>
                    typeEntityDef.valueType match
                        case arrDef: ArrayTypeDefinition =>
                            rawTypesDataMap.get(typeEntityDef.name) match
                                case Some(ArrayTypeData(_, _, ArrayData(_, elementTypesNames))) =>
                                    val elementTypes = elementTypesNames.map(parser.parseAnyTypeDef(_, typesMap))
                                    arrDef.setChildren(elementTypes.toSet)
                                case _ => throw new NoTypeFound(typeEntityDef.name)
                        case objDef: ObjectTypeDefinition =>
                            rawTypesDataMap.get(typeEntityDef.name) match
                                case Some(ObjectTypeData(_, _, ObjectData(_, fields))) =>
                                    val fieldsMap = fields.map(fieldRaw =>
                                        fieldRaw.name -> parser.parseAnyTypeDef(fieldRaw.typeDef, typesMap)).toMap
                                    objDef.setChildren(fieldsMap)
                                case _ => throw new NoTypeFound(typeEntityDef.name)
                        case _ => // do nothing
                )
                typesMap
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)
    private def parseIdType(name: String) = idTypesMap.getOrElse(name, throw new NoIdTypeFound(name))





private class AbstractTypesParser:
    private val typePredefsMap = primitiveFieldTypesMap
        .map((name, typeDef) => name -> PrimitiveEntitySuperType(name, typeDef))
    private val primitiveSuperTypesMap = mutable.Map[String, PrimitiveEntitySuperType]()
    primitiveSuperTypesMap ++= typePredefsMap
    private val arraySuperTypesMap = mutable.Map[String, ArrayEntitySuperType]()
    private val objectSuperTypesMap: mutable.Map[String, ObjectEntitySuperType] = mutable.Map[String, ObjectEntitySuperType]()

    def allParsedTypesMap = primitiveSuperTypesMap ++ arraySuperTypesMap ++ objectSuperTypesMap

    def parsePrimitiveSupertypesChain(typeName: String, typesDataMap: Map[String, TypeData]): PrimitiveEntitySuperType =
        primitiveSuperTypesMap.getOrElseUpdate(typeName,
            typesDataMap.get(typeName) match
                case Some(PrimitiveTypeData(_, None, parentTypeName)) =>
                    PrimitiveEntitySuperType(typeName, CustomPrimitiveTypeDefinition(
                        parsePrimitiveSupertypesChain(parentTypeName, typesDataMap)))
                case Some(PrimitiveTypeData(_, Some(_), parentTypeName)) =>
                    throw new NotAbstractParentTypeException(parentTypeName)
                case Some(_) => throw new NotPrimitiveAbstractTypeException(typeName)
                case None => throw new NoTypeFound(typeName)
        )

    def parseArraySupertypesChain(typeNameOpt: Option[String], typesDataMap: Map[String, TypeData]): Option[ArrayEntitySuperType] =
        typeNameOpt match
            case Some(ArrayTypeDefinition.name) => None
            case Some(typeName) =>
                Some(arraySuperTypesMap.getOrElseUpdate(typeName, typesDataMap.get(typeName) match
                    case Some(ArrayTypeData(typeName, None, ArrayData(parentTypeNameOpt, _))) =>
                        val parent = parseArraySupertypesChain(parentTypeNameOpt, typesDataMap)
                            ArrayEntitySuperType(typeName, ArrayTypeDefinition(Set.empty, parent))
                    case Some(ArrayTypeData(_, Some(_), _)) =>
                        throw new NotAbstractParentTypeException(typeName)
                    case Some(_) => throw new NotArrayAbstractTypeException(typeName)
                    case None => throw new NoTypeFound(typeName)
                ))
            case None => None

    def parseObjectSupertypeChain(typeNameOpt: Option[String], typesDataMap: Map[String, TypeData]): Option[ObjectEntitySuperType] =
        typeNameOpt match
            case Some(ObjectTypeDefinition.name) => None
            case Some(typeName) =>
                Some(objectSuperTypesMap.getOrElseUpdate(typeName, typesDataMap.get(typeName) match
                    case Some(ObjectTypeData(typeName, None, ObjectData(parentTypeNameOpt, _))) =>
                        val parent = parseObjectSupertypeChain(parentTypeNameOpt, typesDataMap)
                            ObjectEntitySuperType(typeName, ObjectTypeDefinition(Map.empty, parent))
                    case Some(ObjectTypeData(_, Some(_), _)) =>
                        throw new NotAbstractParentTypeException(typeName)
                    case Some(_) => throw new NotObjectAbstractTypeException(typeName)
                    case None => throw new NoTypeFound(typeName)
                ))
            case None => None

    private def parseArraySimpleType(rawType: ArrayData, typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        SimpleEntityType(ArrayTypeDefinition(rawType.elementTypesNames.map(parseAnyTypeDef(_, typesMap)).toSet,
            rawType.parentTypeName.map(parentTypeName =>
                arraySuperTypesMap.getOrElse(parentTypeName, throw new NoTypeFound(parentTypeName)))))

    private def parseObjectSimpleType(rawType: ObjectData, typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        SimpleEntityType(ObjectTypeDefinition(rawType.fields.map(fieldRaw =>
            fieldRaw.name -> parseAnyTypeDef(fieldRaw.typeDef, typesMap)).toMap,
            rawType.parentTypeName.map(parentTypeName =>
                objectSuperTypesMap.getOrElse(parentTypeName, throw new NoTypeFound(parentTypeName)))))

    def parseAnyTypeDef(rowData: AnyTypeDef, typesMap: Map[String, AbstractNamedEntityType]): SimpleEntityType =
        rowData match
            case typeName: String => SimpleEntityType(
                typesMap.getOrElse(typeName, throw new NoTypeFound(typeName)).valueType)
            case typeDefRaw: ArrayData => parseArraySimpleType(typeDefRaw, typesMap)
            case typeDefRaw: ObjectData => parseObjectSimpleType(typeDefRaw, typesMap)
