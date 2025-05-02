package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_definitions.{ArrayItemTypeDefinition, EntityIdTypeDefinition, FieldsContainer, FixedStringTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, StringTypeDefinition, TypeDefinition, TypeReferenceDefinition}
import my.valerii_timakov.sgql.entity.domain.type_values.{EntityId, FixedStringId, StringId}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractArrayEntityType, AbstractEntityType, AbstractObjectEntityType, AbstractPrimitiveEntityType, ArrayEntitySuperType, EntitySuperType, GlobalTypesMap, ObjectEntitySuperType, ObjectEntityType}
import my.valerii_timakov.sgql.entity.read_modiriers.{AbstractObjectGetFieldsDescriptor, AllGetFieldsDescriptor, CombinedSearchCondition, FieldPathChainCell, GetFieldsDescriptor, GlobalConstants, IsOfTypeRawSearchCondition, IsOfTypeSearchCondition, LikeRawSearchCondition, LikeSearchCondition, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, NotSearchCondition, ObjectGetFieldsDescriptor, RawSearchCondition, RawSearchConditionSelfConstructable, SearchCondition, SingleFieldSearchCondition, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
import my.valerii_timakov.sgql.entity.{GetFieldsFieldValidateError, GetFieldsFieldsValidateError, GetFieldsParseError, SearchConditionParseError}

import scala.annotation.tailrec
import scala.collection.immutable.Map
import scala.util.{Success, Try}

trait TypesDefinitionProviderInitializer:
    def init(version: Short, globalTypesMap: GlobalTypesMap): TypesDefinitionProvider
    def typesToTablesMap: Map[String, String]
    def getAllPersistenceData: Seq[TypePersistenceDataFinal]

trait TypesDefinitionProvider:
    def getType(name: String): Option[AbstractEntityType[_, _, _]]
    def getAllTypes: Seq[AbstractEntityType[_, _, _]]
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal]
    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal]
    def getAllLeafObjectsSubtypesTyped[ID <: EntityId[_, ID]](entityType: ObjectEntitySuperType[ID, _]): Set[ObjectEntityType[ID, _]]
    def getAllLeafObjectsSubtypes(entityType: ObjectEntitySuperType[_, _]): Set[ObjectEntityType[_, _]]
    def validateGetFieldsDescriptor(descriptor: ObjectGetFieldsDescriptor, entityType: AbstractEntityType[_, _, _]):
        Either[entity.Error, Unit]
    def validateAndParseSearchCondition(
                                           condition: RawSearchCondition,
                                           entityType: AbstractEntityType[_, _, _]
                                       ): Either[SearchConditionParseError, SearchCondition]

object TypesDefinitionProvider:

    private val typesDefinitionResourcePath = "type_definitions/types.td"
    private val typesPersistenceConfigResourcePath = "type_definitions/types.ps"

    def loadInitializer(
        definitionsLoader: TypesDefinitionsLoader,
        persistenceConfigLoader: PersistenceConfigLoader,
    ): TypesDefinitionProviderInitializer =
        val typesDefinitionsMap = definitionsLoader.load(typesDefinitionResourcePath)
        val typesPersistenceData = persistenceConfigLoader.load(typesPersistenceConfigResourcePath, typesDefinitionsMap)
        TypesDefinitionProviderInitializerImpl(persistenceConfigLoader.getTypeToTableMap, typesDefinitionsMap, typesPersistenceData)

class TypesDefinitionProviderInitializerImpl(
                                                val typesToTablesMap: Map[String, String],
                                                typesDefinitionsMap: Map[String, AbstractEntityType[_, _, _]],
                                                typesPersistenceData: Map[String, TypePersistenceDataFinal],
) extends TypesDefinitionProviderInitializer:

    def init(version: Short, globalTypesMap: GlobalTypesMap): TypesDefinitionProvider =
        globalTypesMap.init(typesDefinitionsMap.values.toSeq, version, typesPersistenceData)
        TypesDefinitionProviderImpl(globalTypesMap)

    def getAllPersistenceData: Seq[TypePersistenceDataFinal] =
        typesPersistenceData.values.toSeq

class TypesDefinitionProviderImpl(globalTypesMap: GlobalTypesMap) extends TypesDefinitionProvider:
    def getType(name: String): Option[AbstractEntityType[_, _, _]] =
        globalTypesMap.getTypeByName(name)

    def getAllTypes: Seq[AbstractEntityType[_, _, _]] =
        globalTypesMap.getAllTypes

    def getPersistenceData(name: String): Option[TypePersistenceDataFinal] =
        globalTypesMap.getTypeByName(name).map(_.persistenceData)

    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal] =
        globalTypesMap.getAllTypes.map(entityType => entityType.name -> entityType.persistenceData).toMap

    def getAllLeafObjectsSubtypesTyped[ID <: EntityId[_, ID]](entityType: ObjectEntitySuperType[ID, _]): Set[ObjectEntityType[ID, _]] = 
        globalTypesMap.getAllLeafObjectsSubtypes(entityType).asInstanceOf[Set[ObjectEntityType[ID, _]]]

    def getAllLeafObjectsSubtypes(entityType: ObjectEntitySuperType[_, _]): Set[ObjectEntityType[_, _]] =
        globalTypesMap.getAllLeafObjectsSubtypes(entityType)

    def validateGetFieldsDescriptor(
                                    descriptor: ObjectGetFieldsDescriptor,
                                    entityType: AbstractEntityType[_, _, _]
                                ): Either[entity.Error, Unit] =
        descriptor match
            case ObjectGetFieldsDescriptor(Left(AllGetFieldsDescriptor)) =>
                Right(Success(()))
            case descriptor: ObjectGetFieldsDescriptor =>
                entityType match
                    case objectEntityType: AbstractObjectEntityType[_, _] =>
                        validateObjectGetFieldsDescriptor(descriptor, objectEntityType)
                    case _ =>
                        Left(GetFieldsParseError(s"Cannot use GetFieldsDescriptor $descriptor for non object type $entityType!"))

    def validateAndParseSearchCondition(
                                           condition: RawSearchCondition,
                                           entityType: AbstractEntityType[_, _, _]
                               ): Either[SearchConditionParseError, SearchCondition] =
        validateAndParseSingleSearchConditionOnAbstractType(Some(condition.field), condition, entityType, "")

    private def validateObjectGetFieldsDescriptor(
                                                     descriptor: AbstractObjectGetFieldsDescriptor,
                                                     entityType: AbstractObjectEntityType[_, _]
                                                 ): Either[entity.Error, Unit] =
        descriptor.fields match
            case Left(AllGetFieldsDescriptor) =>
                Right(Success(()))
            case Right(fields) =>
                val res = fields.map(fieldDescriptor => validateNestedGetFieldsDescriptor(fieldDescriptor, entityType))
                if (res.contains(Left(_)))
                    Left( GetFieldsFieldsValidateError( res.collect({ case Left(error) => error } )) )
                else
                    Right(Success(()))

    private def validateNestedGetFieldsDescriptor(
                                                     descriptor: NestedGetFieldsDescriptor,
                                                     entityType: AbstractObjectEntityType[_, _]
                                                 ): Either[entity.Error, Unit] =
        def checkFieldPresent(fieldName: String): Either[entity.Error, Unit] =
            if (entityType.typeDefinition.allFields.contains(fieldName))
                Right(Success(()))
            else
                Left(GetFieldsFieldValidateError(s"GetFieldDescriptor field $fieldName not present in corresponding " +
                    s"type $entityType!"))
        descriptor match
            case SingleGetFieldsDescriptor(fieldName, _, _) =>
                checkFieldPresent(fieldName)
            case ListGetFieldsDescriptor(fieldName, _, _) =>
                checkFieldPresent(fieldName)
            case descriptor: SubObjectGetFieldsDescriptor =>
                entityType.typeDefinition.allFields.get(descriptor.fieldName)
                    .map {
                        case objectFieldType: AbstractObjectEntityType[_, _] =>
                            validateObjectGetFieldsDescriptor(descriptor, objectFieldType)
                        case _ =>
                            Left(GetFieldsFieldValidateError(s"Cannot use GetFieldsDescriptor $descriptor for non object type $entityType!"))
                    }
                    .getOrElse(Left(GetFieldsFieldValidateError(s"GetFieldDescriptor field ${descriptor.fieldName} not " +
                        s"present in corresponding type $entityType!")))
                
    private def validateAndParseSingleSearchConditionOnAbstractType(
                                                 fieldsChainOpt: Option[FieldPathChainCell],
                                                 ownerCondition: RawSearchCondition,
                                                 entityType: AbstractEntityType[_, _, _],
                                                 typePrefix: String
                                       ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        (entityType, fieldsChainOpt, ownerCondition) match
            case (entitySuperType: ArrayEntitySuperType[_, _], None, condition: IsOfTypeRawSearchCondition) =>
                validateAndParseIsOfType(condition, entitySuperType, true, ownerCondition)
            case (entitySuperType: EntitySuperType[_, _, _], None, condition: IsOfTypeRawSearchCondition) =>
                validateAndParseIsOfType(condition, entitySuperType, false, ownerCondition)
            case (arrayType: AbstractArrayEntityType[_, _], None | Some(FieldPathChainCell(GlobalConstants.entityIdFieldNameForDsc, None)), _) =>
                validateAndParseOneValueTypeCondition(ownerCondition, entityType.typeDefinition.idType, true)
            case (entityType: AbstractEntityType[_, _, _], Some(FieldPathChainCell(GlobalConstants.entityIdFieldNameForDsc, None)), _) =>
                validateAndParseOneValueTypeCondition(ownerCondition, entityType.typeDefinition.idType, false)
            case (arrayType: AbstractArrayEntityType[_, _], None | Some(FieldPathChainCell(GlobalConstants.primitiveTypeValueFieldNameForDsc, None)), _) =>
                validateAndParseArrayValueTypeCondition(ownerCondition, arrayType.typeDefinition.elementTypes)
            case (primType: AbstractPrimitiveEntityType[_, _, _], None | Some(FieldPathChainCell(GlobalConstants.primitiveTypeValueFieldNameForDsc, None)), _) =>
                validateAndParseOneValueTypeCondition(ownerCondition, primType.typeDefinition.rootType, false)
            case (objDef: AbstractObjectEntityType[_, _], Some(fieldsChain), _) =>
                validateAndParseSingleSearchConditionOnObjectContent(fieldsChain, ownerCondition, objDef.typeDefinition, s" $typePrefix${objDef.name}")
            case _ =>
                Left(SearchConditionParseError(s"SearchCondition field $fieldsChainOpt is not compatible with " +
                    s"type $entityType!"))

    private def validateAndParseIsOfType(
                                            condition: IsOfTypeRawSearchCondition,
                                            entitySuperType: EntitySuperType[_, _, _],
                                            isSet: Boolean,
                                            ownerCondition: RawSearchCondition,
                                        ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        getType(condition.entityTypeName)
            .toRight(SearchConditionParseError(s"Type ${condition.entityTypeName} not found for " +
                s"IsOfTypeSearchCondition! Condition: $ownerCondition"))
            .filterOrElse(entityType => entityType.isChildOfRaw(entitySuperType),
                SearchConditionParseError(s"Type ${condition.entityTypeName} is not child of ${entitySuperType.name}! " +
                    s"Condition: $ownerCondition"))
            .map (condition.createForOneValue(_, isSet))

    @tailrec
    private def validateAndParseSingleSearchConditionOnObjectContent(
                                                 fieldsChain: FieldPathChainCell,
                                                 ownerCondition: RawSearchCondition,
                                                 fieldsContainer: FieldsContainer,
                                                 typeName: String, 
                                             ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        (fieldsContainer.allFields.get(fieldsChain.fieldName).map(_.valueType), fieldsChain.nextCell) match
            case (Some(soDef: SimpleObjectTypeDefinition[_]), Some(nextField)) =>
                validateAndParseSingleSearchConditionOnObjectContent(nextField, ownerCondition, soDef,
                    s" $typeName.${fieldsChain.fieldName}[_]")
            case (Some(refDef: TypeReferenceDefinition[_]), _) =>
                validateAndParseSingleSearchConditionOnAbstractType(fieldsChain.nextCell, ownerCondition,
                    refDef.referencedType, s" $typeName.${fieldsChain.fieldName}->")
            case (Some(primDef: RootPrimitiveTypeDefinition[_]), None) =>
                validateAndParseOneValueTypeCondition(ownerCondition, primDef, false)
            case (Some(field), _) =>
                Left(SearchConditionParseError(s"Incompatible combination of search condition field $fieldsChain and " +
                    s"found field $field in type $typeName!"))
            case (None, _) =>
                Left(SearchConditionParseError(s"Search condition field ${fieldsChain.fieldName} not present in " +
                    s"corresponding type $typeName!"))

    private def validateAndParseOneValueTypeCondition(
        ownerCondition: RawSearchCondition,
        valueType: TypeDefinition,
        isSet: Boolean
    ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        ownerCondition match
            case condConstr: RawSearchConditionSelfConstructable =>
                val parser: String => Either[SearchConditionParseError, Any] =
                    (value: String) => valueType.parse(value).left.map( error =>
                        SearchConditionParseError(s"Cannot parse value $value to ID type " +
                            s"${valueType.name} for condition $ownerCondition!")
                    )
                condConstr.parseValueAndCreate(parser, isSet)
            case likeCond: LikeRawSearchCondition =>
                Right(likeCond.createForOneValue(valueType.isString, isSet))

    private def validateAndParseArrayValueTypeCondition(
                                                         ownerCondition: RawSearchCondition,
                                                         elementTypes: Set[ArrayItemTypeDefinition]
                                                     ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        ownerCondition match
            case condConstr: RawSearchConditionSelfConstructable =>
                val parser: String => Either[SearchConditionParseError, Any] =
                    (value: String) => valueType.parse(value).left.map( error =>
                        SearchConditionParseError(s"Cannot parse value $value to ID type " +
                            s"${valueType.name} for condition $ownerCondition!")
                    )
                condConstr.parseValueAndCreate(parser, true)
            case likeCond: LikeRawSearchCondition =>
                Right(likeCond.createForOneValue(valueType.isString, true))


