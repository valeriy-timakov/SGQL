package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_definitions.{FieldsContainer, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeReferenceDefinition}
import my.valerii_timakov.sgql.entity.domain.type_values.EntityId
import my.valerii_timakov.sgql.entity.domain.types.{AbstractArrayEntityType, AbstractEntityType, AbstractObjectEntityType, AbstractPrimitiveEntityType, GlobalTypesMap, ObjectEntitySuperType, ObjectEntityType}
import my.valerii_timakov.sgql.entity.read_modiriers.{AbstractObjectGetFieldsDescriptor, AllGetFieldsDescriptor, CombinedSearchCondition, GetFieldsDescriptor, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, NotSearchCondition, ObjectGetFieldsDescriptor, SearchCondition, FieldPathChainCell, SingleFieldSearchCondition, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
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
    def validateSearchCondition(condition: SearchCondition, entityType: AbstractEntityType[_, _, _]):
        Either[SearchConditionParseError, Unit]

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
    def getType(name: String): Option[AbstractEntityType[_, _, _]] = globalTypesMap.getTypeByName(name)
    def getAllTypes: Seq[AbstractEntityType[_, _, _]] = globalTypesMap.getAllTypes
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal] = globalTypesMap.getTypeByName(name).map(_.persistenceData)
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

    def validateSearchCondition(
                                   condition: SearchCondition, 
                                   entityType: AbstractEntityType[_, _, _]
                               ): Either[SearchConditionParseError, Unit] =
        condition match
            case combined: CombinedSearchCondition =>
                combined.conditions.map(validateSearchCondition(_, entityType)).collectFirst({ case Left(error) => error })
                    .map(Left(_))
                    .getOrElse(Right(Success(())))
            case not: NotSearchCondition =>
                validateSearchCondition(not.condition, entityType)
            case single: SingleFieldSearchCondition =>
                validateSingleSearchCondition(Some(single.field), entityType, "")
                
    private def validateSingleSearchCondition(
                                                 fieldsChainOpt: Option[FieldPathChainCell],
                                                 entityType: AbstractEntityType[_, _, _],
                                                 typePrefix: String
                                       ): Either[SearchConditionParseError, Unit] =
        (entityType, fieldsChainOpt) match
            case (objDef: ObjectEntityType[_, _], Some(fieldsChain)) =>
                validateSingleSearchCondition(fieldsChain, objDef.typeDefinition, s" $typePrefix${objDef.name}")
            case (objDef: ObjectEntitySuperType[_, _], Some(fieldsChain)) =>
                fieldsChain.subType match
                    case Some(subTypeName) =>
                        globalTypesMap.getTypeByName(subTypeName) match
                            case Some(subType: ObjectEntityType[_, _]) =>
                                if (subType.isChildOf(objDef))
                                    validateSingleSearchCondition(fieldsChain, subType.typeDefinition, s" $typePrefix${subType.name}")
                                else
                                    Left(SearchConditionParseError(s"Type $subTypeName is not subtype of $entityType!"))
                            case None =>
                                Left(SearchConditionParseError(s"Sub type $subTypeName in condition $fieldsChain not found!"))
                    case None =>
                        validateSingleSearchCondition(fieldsChain, objDef.typeDefinition, s" $typePrefix${objDef.name}")
            case (objDef: AbstractPrimitiveEntityType[_, _, _], Some(FieldPathChainCell("value", None, None))) =>
                Right(Success(()))
            case (objDef: AbstractPrimitiveEntityType[_, _, _], None) =>
                Right(Success(()))
            case (objDef: AbstractArrayEntityType[_, _], None) =>
                Right(Success(()))
            case (objDef: AbstractArrayEntityType[_, _], Some(FieldPathChainCell("value", None, None))) =>
                Right(Success(()))
            case _ =>
                Left(SearchConditionParseError(s"SearchCondition field $fieldsChainOpt is not compatible with type $entityType!"))

    @tailrec
    private def validateSingleSearchCondition(
                                                 fieldsChain: FieldPathChainCell,
                                                 fieldsContainer: FieldsContainer,
                                                 typeName: String, 
                                             ): Either[SearchConditionParseError, Unit] =
        (fieldsContainer.allFields.get(fieldsChain.fieldName).map(_.valueType), fieldsChain.nextCell) match
            case (Some(soDef: SimpleObjectTypeDefinition[_]), Some(nextField)) =>
                validateSingleSearchCondition(nextField, soDef, s" $typeName.${fieldsChain.fieldName}[_]")
            case (Some(refDef: TypeReferenceDefinition[_]), _) =>
                validateSingleSearchCondition(fieldsChain.nextCell, refDef.referencedType, s" $typeName.${fieldsChain.fieldName}->")
            case (Some(primDef: RootPrimitiveTypeDefinition[_]), None) =>
                Right(Success(()))
            case (Some(field), _) =>
                Left(SearchConditionParseError(s"Incompatible combination of search condition field $fieldsChain and found field $field in type $typeName!"))
            case (None, _) =>
                Left(SearchConditionParseError(s"Search condition field ${fieldsChain.fieldName} not present in corresponding type $typeName!"))

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
