package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.{GetFieldsParseError, SearchConditionParseError}
import my.valerii_timakov.sgql.entity.domain.type_definitions.AbstractEntityType
import my.valerii_timakov.sgql.entity.read_modiriers.{AllGetFieldsDescriptor, GetFieldsDescriptor, SearchCondition}

import scala.util.{Success, Try}

trait TypesDefinitionProvider:
    def getType(name: String): Option[AbstractEntityType]
    def getAllTypes: Seq[AbstractEntityType]
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal]
    def getAllPersistenceData: Seq[TypePersistenceDataFinal]
    def getTypesToTalesMap: Map[String, String]
    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal]
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractEntityType): 
        Try[Either[GetFieldsParseError, GetFieldsDescriptor]]
    def parseSearchCondition(condition: Option[String], entityType: AbstractEntityType): 
        Try[Either[SearchConditionParseError, SearchCondition]]
    
object TypesDefinitionProvider:
    def create(implicit 
        definitionsLoader: TypesDefinitionsLoader, 
        persistenceConfigLoader: PersistenceConfigLoader
    ): TypesDefinitionProvider = 
        TypesDefinitionProviderImpl(definitionsLoader, persistenceConfigLoader)

class TypesDefinitionProviderImpl(
    definitionsLoader: TypesDefinitionsLoader, 
    persistenceConfigLoader: PersistenceConfigLoader
) extends TypesDefinitionProvider:

    private val typesDefinitionResourcePath = "type_definitions/types.td"
    private val typesPersistenceConfigResourcePath = "type_definitions/types.ps"
    
    private val typesDefinitionsMap = definitionsLoader.load(typesDefinitionResourcePath)
    private val typesPersistenceData = persistenceConfigLoader.load(typesPersistenceConfigResourcePath, typesDefinitionsMap)    

          
        

    def getType(name: String): Option[AbstractEntityType] = typesDefinitionsMap.get(name) 
    def getAllTypes: Seq[AbstractEntityType] = typesDefinitionsMap.values.toSeq
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal] = typesPersistenceData.get(name)
    def getAllPersistenceData: Seq[TypePersistenceDataFinal] = typesPersistenceData.values.toSeq
    def getTypesToTalesMap: Map[String, String] = persistenceConfigLoader.getTypeToTableMap
    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal] = typesPersistenceData
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractEntityType): Try[Either[GetFieldsParseError, GetFieldsDescriptor]] = Success(Right(AllGetFieldsDescriptor))
    def parseSearchCondition(condition: Option[String], entityType: AbstractEntityType): Try[Either[SearchConditionParseError, SearchCondition]] = ???
