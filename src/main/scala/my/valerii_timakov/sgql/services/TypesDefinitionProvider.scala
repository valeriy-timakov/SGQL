package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.{GetFieldsParseError, SearchConditionParseError}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, GlobalTypesMap}
import my.valerii_timakov.sgql.entity.read_modiriers.{AllGetFieldsDescriptor, GetFieldsDescriptor, SearchCondition}

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
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractEntityType[_, _, _]):
        Try[Either[GetFieldsParseError, GetFieldsDescriptor]]
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
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractEntityType[_, _, _]): Try[Either[GetFieldsParseError, GetFieldsDescriptor]] = Success(Right(AllGetFieldsDescriptor))
    def parseSearchCondition(condition: Option[String], entityType: AbstractEntityType[_, _, _]): Try[Either[SearchConditionParseError, SearchCondition]] = ???
