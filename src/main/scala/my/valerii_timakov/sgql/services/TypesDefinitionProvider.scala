package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*

import scala.util.{Success, Try}

trait TypesDefinitionProvider:
    def getType(name: String): Try[Option[EntityType]]
    def getAllTypes: Try[Seq[EntityType]]
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: EntityType): Try[Either[GetFieldsParseError, GetFieldsDescriptor]]
    def parseSearchCondition(condition: Option[String], entityType: EntityType): Try[Either[SearchConditionParseError, SearchCondition]]

class TypesDefinitionProviderImpl extends TypesDefinitionProvider:
    def getType(name: String): Try[Option[EntityType]] = Success(Some(EntityType(name, IntIdTypeDefinition, StringTypeDefinition)))
    def getAllTypes: Try[Seq[EntityType]] = Success(Seq(EntityType("TestType", IntIdTypeDefinition, StringTypeDefinition)))
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: EntityType): Try[Either[GetFieldsParseError, GetFieldsDescriptor]] = Success(Right(AllGetFieldsDescriptor))
    def parseSearchCondition(condition: Option[String], entityType: EntityType): Try[Either[SearchConditionParseError, SearchCondition]] = ???
