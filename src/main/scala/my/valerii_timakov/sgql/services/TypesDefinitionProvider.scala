package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*

import scala.util.{Success, Try}

trait TypesDefinitionProvider:
    def getType(name: String): Option[AbstractNamedEntityType]
    def getAllTypes: Seq[AbstractNamedEntityType]
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal]
    def getAllPersistenceData: Seq[TypePersistenceDataFinal]
    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal]
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractNamedEntityType): 
        Try[Either[GetFieldsParseError, GetFieldsDescriptor]]
    def parseSearchCondition(condition: Option[String], entityType: AbstractNamedEntityType): 
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

          
        

    def getType(name: String): Option[AbstractNamedEntityType] = typesDefinitionsMap.get(name) 
    def getAllTypes: Seq[AbstractNamedEntityType] = typesDefinitionsMap.values.toSeq
    def getPersistenceData(name: String): Option[TypePersistenceDataFinal] = typesPersistenceData.get(name)
    def getAllPersistenceData: Seq[TypePersistenceDataFinal] = typesPersistenceData.values.toSeq
    def getAllPersistenceDataMap: Map[String, TypePersistenceDataFinal] = typesPersistenceData
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractNamedEntityType): Try[Either[GetFieldsParseError, GetFieldsDescriptor]] = Success(Right(AllGetFieldsDescriptor))
    def parseSearchCondition(condition: Option[String], entityType: AbstractNamedEntityType): Try[Either[SearchConditionParseError, SearchCondition]] = ???
