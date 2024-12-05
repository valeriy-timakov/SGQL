package my.valerii_timakov.sgql.services

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.TypesDefinitionsParseError
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityIdTypeDefinition, AbstractEntityType, AbstractObjectEntityType, AbstractRootPrimitiveTypeDefinition, ArrayEntitySuperType, ArrayEntityType, ArrayItemTypeDefinition, ArrayTypeDefinition, CustomPrimitiveEntityType, CustomPrimitiveTypeDefinition, EntityIdTypeDefinition, EntitySuperType, EntityType, FieldTypeDefinition, FieldValueTypeDefinition, FixedStringIdTypeDefinition, FixedStringTypeDefinition, ObjectEntitySuperType, ObjectEntityType, ObjectTypeDefinition, PrimitiveEntitySuperType, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, idTypesMap, primitiveFieldTypesMap}
import my.valerii_timakov.sgql.exceptions.*
import my.valerii_timakov.sgql.services.TypesDefinitionsParser.IdTypeRef

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader

trait TypesDefinitionsLoader:
    def load(typesDefinitionResourcePath: String): Map[String, AbstractEntityType[_, _, _]]

class TypesDefinitionsLoaderImpl(conf: Config) extends TypesDefinitionsLoader:
    private val specialConcreteSuffix = conf.getString("super-type-default-implementation-suffix")
    private val defaultFixedStringLength = conf.getInt("fixed-string-length-default")
    def load(typesDefinitionResourcePath: String): Map[String, AbstractEntityType[_, _, _]] =
        val tdSource = Source.fromResource (typesDefinitionResourcePath)
        TypesDefinitionsParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: TypesRootPackageData) =>
                var rawTypesDataMap = packageData.toMap
                val parser = new AbstractTypesParser(rawTypesDataMap, defaultFixedStringLength)

                val typesMapBuilder = Map.newBuilder[String, AbstractEntityType[_, _, _]]
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
                                    ArrayEntityType(concreteName, ArrayTypeDefinition(None, Right(parent)))
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
                val typesMap:  Map[String, AbstractEntityType[_, _, _]] = typesMapPre
                    .map((typeFullName, typeEntityDef) =>
                        val children = getChildren(typeFullName, typesMapPre)
                        if (children.isEmpty) {
                            //convert super types without children to entity types
                            val leafType = typeEntityDef match
                                case ArrayEntitySuperType(name, valueType) =>
                                    ArrayEntityType(name, valueType)
                                case ObjectEntitySuperType(name, valueType) =>
                                    ObjectEntityType(name, valueType)
                                case PrimitiveEntitySuperType(name, valueType) =>
                                    CustomPrimitiveEntityType(name, valueType)
                                case _ => typeEntityDef
                            typeFullName -> leafType
                        } else {
                            typeFullName -> typeEntityDef
                        }
                    )
                //initialize children - fields or array items
                typesMap.foreach((typeFullName, typeEntityDef) =>
                    typeEntityDef.valueType match
                        case arrDef: ArrayTypeDefinition[_, _] =>
                            val typePrefix = typeNamespace(typeFullName)
                            rawTypesDataMap.get(typeFullName) match
                                case Some(ArrayTypeData(_, ArrayData(_, elementTypesNames), _)) =>
                                    val elementTypes = elementTypesNames.map(parser.parseArrayItemTypeDef(_, typePrefix, typesMap))
                                    arrDef.setChildren(elementTypes.toSet)
                                case _ => throw new NoTypeFound(typeFullName)
                        case objDef: ObjectTypeDefinition[_, _] =>
                            val typePrefix = typeNamespace(typeFullName)
                            rawTypesDataMap.get(typeFullName) match
                                case Some(ObjectTypeData(_, ObjectData(_, fields), _)) =>
                                    val fieldsMap = fields.map(fieldRaw =>
                                        fieldRaw.name -> parser.parseAnyTypeDef(fieldRaw, typePrefix, typesMap)).toMap
                                    objDef.setChildren(fieldsMap)
                                case _ => throw new NoTypeFound(typeFullName)
                        case _ => // do nothing
                )
                //check back references
                typesMap.foreach((typeFullName, entityType) =>
                    entityType.valueType match
                        case objDef: ObjectTypeDefinition[_, _] =>
                            objDef.fields.foreach((fieldName, fieldDef) =>
                                fieldDef.valueType match
                                    case TypeBackReferenceDefinition(backReferencedType, refFieldName) =>
                                        val refFieldDef = backReferencedType.valueType.fields.getOrElse(refFieldName,
                                            throw new ConsistencyException(s"Field $fieldName not found in back " +
                                                s"referenced type ${backReferencedType.name}!"))
                                        refFieldDef.valueType match
                                            case TypeReferenceDefinition(referencedType) =>
                                                if (referencedType.name != entityType.name)
                                                    throw new ConsistencyException(s"Found reference " +
                                                        s"${backReferencedType.name}.$refFieldName: refFieldDef$refFieldDef " +
                                                        s"in back referenced type does not reference to current type ${entityType.name}!")
                                            case _ => throw new ConsistencyException(s"Field $fieldName in back " +
                                                s"referenced type ${backReferencedType.name} is not of reference type! " +
                                                s"Type: ${refFieldDef.valueType}")
                                    case _ => // do nothing
                            )
                        case _ => // do nothing
                )
                typesMap
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)
    
    private def getChildren(parentName: String, typesMap: Map[String, AbstractEntityType[_, _, _]]): List[AbstractEntityType[_, _, _]] =
        typesMap.values.filter(typeDef => typeDef match
            case entityType: EntityType[_, _, _] =>
                entityType.valueType.parent.exists(parentName == _.name)
            case namedEntitySuperType: EntitySuperType[_, _, _] =>
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



private class AbstractTypesParser(rawTypesDataMap: Map[String, TypeData], defaultFixedStringLength: Int):
    private val typePredefsMap = primitiveFieldTypesMap
    private val primitiveSuperTypesMap = mutable.Map[String, PrimitiveEntitySuperType[_, _, _]]()
    private val arraySuperTypesMap = mutable.Map[String, ArrayEntitySuperType[_, _]]()
    private val objectSuperTypesMap: mutable.Map[String, ObjectEntitySuperType[_, _]] = mutable.Map[String, ObjectEntitySuperType[_, _]]()

    def allParsedTypesMap: mutable.Map[String, EntitySuperType[_, _, _]] =
        primitiveSuperTypesMap ++ arraySuperTypesMap ++ objectSuperTypesMap

    def parsePrimitiveSupertypesChain (typeName: String, idTypeNameOpt: Option[String], parentTypeName: String,
                                       typePrefixOpt: Option[String]): PrimitiveEntitySuperType[_, _, _] =
        primitiveSuperTypesMap.getOrElse(typeName, {

            val result: PrimitiveEntitySuperType[_, _, _] =
                getPrimitiveType(parentTypeName) match
                    case Some(rootPrimitiveType) =>
                        idTypeNameOpt match
                            case Some(idTypeName) =>
                                val idType: EntityIdTypeDefinition[_] = parseIdType(idTypeName)
                                PrimitiveEntitySuperType(typeName, CustomPrimitiveTypeDefinition(
                                    Left( (idType, rootPrimitiveType) )
                                ))
                            case None =>
                                throw new NoIdentityForRootSupetType(typeName)
                    case None =>
                        if (idTypeNameOpt.isDefined) {
                            throw new IdentitySetForNonRootType(typeName)
                        }
                        findTypeDefinition(parentTypeName, typePrefixOpt) match
                            case (typeFullName, PrimitiveTypeData(_, parentIdType, superParentTypeName, _)) =>
                                val parentType = parsePrimitiveSupertypesChain(parentTypeName, parentIdType,
                                    superParentTypeName, typeNamespace(typeFullName))
                                PrimitiveEntitySuperType(typeName, CustomPrimitiveTypeDefinition( Right( parentType ) ))
                            case (typeFullName, _) =>
                                throw new NotPrimitiveAbstractTypeException(typeFullName)
            primitiveSuperTypesMap += ((result.name, result))
            result
        })

    def parseArraySupertypesChain(typeName: String, idOrParent: Either[IdTypeRef, String], typePrefixOpt: Option[String]): ArrayEntitySuperType[_, _] =
        arraySuperTypesMap.getOrElse(typeName, {
            val result: ArrayEntitySuperType[_, _] = idOrParent match
                case Left(idType) =>
                    val idType_ = parseIdType(idType.name)
                    ArrayEntitySuperType(typeName, ArrayTypeDefinition(None, Left(idType_) ))
                case Right(parentTypeName) =>
                    findTypeDefinition(parentTypeName, typePrefixOpt) match
                        case (parentTypeFullName, ArrayTypeData(_, ArrayData(parentIdOrParent, _), _)) =>
                            val parentType = parseArraySupertypesChain(parentTypeFullName, parentIdOrParent, typeNamespace(parentTypeFullName))
                            ArrayEntitySuperType(typeName, ArrayTypeDefinition(None, Right(parentType)))
                        case (typeFullName, _) =>
                            throw new NotArrayAbstractTypeException(typeFullName)
            arraySuperTypesMap += ((result.name, result))
            result
        })


    def parseObjectSupertypeChain(typeName: String, idOrParent: Either[IdTypeRef, String], typePrefixOpt: Option[String]): ObjectEntitySuperType[_, _] =
        objectSuperTypesMap.getOrElse(typeName, {
            val result: ObjectEntitySuperType[_, _] = idOrParent match
                case Left(idType) =>
                    val idType_ = parseIdType(idType.name)
                    ObjectEntitySuperType(typeName, ObjectTypeDefinition(Map.empty, Left(idType_)))
                case Right(parentTypeName) =>
                    val parentType = findOrParseObjectSuperType(parentTypeName, typePrefixOpt)
                    ObjectEntitySuperType(typeName, ObjectTypeDefinition(Map.empty, Right(parentType)))
            objectSuperTypesMap += ((result.name, result))
            result
        })

    private def findOrParseObjectSuperType(typeName: String, typePrefixOpt: Option[String]): ObjectEntitySuperType[_, _] =
        findTypeDefinition(typeName, typePrefixOpt) match
            case (parentTypeFullName, ObjectTypeData(_, ObjectData(idOrParent, _), _)) =>
                parseObjectSupertypeChain(parentTypeFullName, idOrParent, typeNamespace(parentTypeFullName))
            case (typeFullName, _) =>
                throw new NotObjectAbstractTypeException(typeFullName)

    private def parseIdType(name: String): EntityIdTypeDefinition[_] =
        idTypesMap.getOrElse(name, throw new NoIdTypeFound(name)) match
            //TODO: add support for fixed string customized length
            case FixedStringIdTypeDefinition => FixedStringIdTypeDefinition(defaultFixedStringLength)
            case entityIdDef => entityIdDef.asInstanceOf[EntityIdTypeDefinition[_]]

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
                           typesMap: Map[String, AbstractEntityType[_, _, _]],
    ): FieldTypeDefinition[_] =
        fieldData.typeDef match
            case refData: ReferenceData =>
                FieldTypeDefinition(parseReferenceType(refData, typePrefix, typesMap), fieldData.isNullable)
            case typeDefRaw: SimpleObjectData => 
                parseObjectSimpleType(typeDefRaw, typePrefix, typesMap, fieldData.isNullable)
                
    def parseArrayItemTypeDef(rowData: ReferenceData,
                              typePrefix: Option[String],
                              typesMap: Map[String, AbstractEntityType[_, _, _]]): ArrayItemTypeDefinition =
        parseReferenceType(rowData, typePrefix, typesMap) match
            case refData: TypeReferenceDefinition[_] =>
                ArrayItemTypeDefinition(refData)
            case refData: RootPrimitiveTypeDefinition[_] =>
                ArrayItemTypeDefinition(refData)
            case _ =>
                throw new ConsistencyException("Only reference or root primitive types could be array items! " +
                    s"Type ${rowData.refTypeName} is trying to be array item!")
                
    private def getPrimitiveType(name: String): Option[RootPrimitiveTypeDefinition[_]] =
        typePredefsMap.get(name)
            .map {
                case rootPrimitiveType: RootPrimitiveTypeDefinition[_] =>
                    rootPrimitiveType
                case FixedStringTypeDefinition =>
                    //TODO: add support for fixed string customized length
                    FixedStringTypeDefinition(defaultFixedStringLength)
                case _ =>
                    throw new WrongStateExcetion(s"Type $name is not root primitive type!")
            }

    private def parseReferenceType(refData: ReferenceData,
                                   typePrefix: Option[String],
                                   typesMap: Map[String, AbstractEntityType[_, _, _]]
                                  ): FieldValueTypeDefinition[_] =
        val referencedTypeOpt: Option[AbstractEntityType[_, _, _]] =
            typesMap.get(refData.refTypeName)
                .orElse(
                    findInPackagesUpstears(refData.refTypeName, typePrefix, typesMap)
                        .map(_._2)
                )
        refData.refFieldName match
            case Some(refFieldName) => 
                val referencedType: AbstractEntityType[_, _, _] = referencedTypeOpt.getOrElse(
                    if (typePredefsMap.contains(refData.refTypeName))
                        throw new ConsistencyException("Root primitive type couldnot be referenced by as back " +
                            s"reference! Type ${refData.refTypeName} is trying to be referenced by ${refData.refFieldName}!")
                    else
                        throw new NoTypeFound(refData.refTypeName))
                TypeBackReferenceDefinition(referencedType, refFieldName, refData.refTypeName)
            case None =>
                referencedTypeOpt match
                    case Some(referencedType) =>
                        TypeReferenceDefinition(referencedType, refData.refTypeName)
                    case None =>
                        getPrimitiveType(refData.refTypeName) match
                            case Some(valueType) =>
                                valueType
                            case None =>
                                throw new NoTypeFound(refData.refTypeName)
                

    private def parseObjectSimpleType(
                                         rawType: SimpleObjectData,
                                         typePrefixOpt: Option[String],
                                         typesMap: Map[String, AbstractEntityType[_, _, _]],
                                         isNullable: Boolean, 
    ): FieldTypeDefinition[_] = {
        val parentOrId: Option[ObjectEntitySuperType[_, _]] = rawType.parent.map(parentTypeName =>
            findOrParseObjectSuperType(parentTypeName, typePrefixOpt)
        )
        FieldTypeDefinition(SimpleObjectTypeDefinition(
            parentOrId,
            rawType.fields.map(fieldRaw => fieldRaw.name -> parseAnyTypeDef(fieldRaw, typePrefixOpt, typesMap)).toMap
        ), isNullable)
    }


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


