package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.{GetFieldsFieldValidateError, GetFieldsFieldsValidateError, GetFieldsParseError, SearchConditionParseError}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, AbstractObjectEntityType, GlobalTypesMap}
import my.valerii_timakov.sgql.entity.read_modiriers.{AbstractObjectGetFieldsDescriptor, AllGetFieldsDescriptor, GetFieldsDescriptor, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, ObjectGetFieldsDescriptor, RootGetFieldsDescriptor, SearchCondition, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}

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
    def validateGetFieldsDescriptor(descriptor: RootGetFieldsDescriptor, entityType: AbstractEntityType[_, _, _]):
        Either[entity.Error, Unit]
    def parseSearchCondition(condition: Option[String], entityType: AbstractEntityType[_, _, _]):
        Try[Either[SearchConditionParseError, SearchCondition]]

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
        globalTypesMap.init(typesDefinitionsMap.values.toSeq, version)
        TypesDefinitionProviderImpl(typesPersistenceData, globalTypesMap)

    def getAllPersistenceData: Seq[TypePersistenceDataFinal] =
        typesPersistenceData.values.toSeq

class TypesDefinitionProviderImpl(
    typesPersistenceData: Map[String, TypePersistenceDataFinal],
    globalTypesMap: GlobalTypesMap,
) extends TypesDefinitionProvider:
    def getType(name: String): Option[AbstractEntityType[_, _, _]] = globalTypesMap.getTypeByName(name)
    def getAllTypes: Seq[AbstractEntityType[_, _, _]] = globalTypesMap.getAllTypes
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal] = typesPersistenceData.get(name)
    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal] = typesPersistenceData
    def validateGetFieldsDescriptor(
                                    descriptor: RootGetFieldsDescriptor,
                                    entityType: AbstractEntityType[_, _, _]
                                ): Either[entity.Error, Unit] =
        descriptor match
            case AllGetFieldsDescriptor =>
                Right(Success(()))
            case descriptor: ObjectGetFieldsDescriptor =>
                entityType match
                    case objectEntityType: AbstractObjectEntityType[_, _] =>
                        validateObjectGetFieldsDescriptor(descriptor, objectEntityType)
                    case _ =>
                        Left(GetFieldsParseError(s"Cannot use GetFieldsDescriptor $descriptor for non object type $entityType!"))

    def parseSearchCondition(condition: Option[String], entityType: AbstractEntityType[_, _, _]): Try[Either[SearchConditionParseError, SearchCondition]] = ???

    private def validateObjectGetFieldsDescriptor(
                                               descriptor: AbstractObjectGetFieldsDescriptor,
                                               entityType: AbstractObjectEntityType[_, _]
                                           ): Either[entity.Error, Unit] =
        val res = descriptor.fields.map(fieldDescriptor => validateNestedGetFieldsDescriptor(fieldDescriptor, entityType))
        if (res.contains(Left(_)))
            Left( GetFieldsFieldsValidateError( res.collect({ case Left(error) => error } )) )
        else
            Right(Success(()))

    @tailrec
    private def validateNestedGetFieldsDescriptor(
                                                     descriptor: NestedGetFieldsDescriptor,
                                                     entityType: AbstractObjectEntityType[_, _]
                                                 ): Either[entity.Error, Unit] =
        descriptor match
            case SingleGetFieldsDescriptor(fieldName) =>
                if (entityType.valueType.fields.contains(fieldName))
                    Right(Success(()))
                else
                    Left(GetFieldsFieldValidateError(s"GetFieldDescriptor field $fieldName not present in corresponding " +
                        s"type $entityType!"))
            case descriptor: SubObjectGetFieldsDescriptor =>
                entityType.valueType.fields.get(descriptor.fieldName)
                    .map {
                        case objectFieldType: AbstractObjectEntityType[_, _] =>
                            validateObjectGetFieldsDescriptor(descriptor, objectFieldType)
                        case _ =>
                            Left(GetFieldsFieldValidateError(s"Cannot use GetFieldsDescriptor $descriptor for non object type $entityType!"))
                    }
                    .getOrElse(Left(GetFieldsFieldValidateError(s"GetFieldDescriptor field ${descriptor.fieldName} not " +
                        s"present in corresponding type $entityType!")))
            case ListGetFieldsDescriptor(repeatedField, _, _) =>
                validateNestedGetFieldsDescriptor(repeatedField, entityType)
