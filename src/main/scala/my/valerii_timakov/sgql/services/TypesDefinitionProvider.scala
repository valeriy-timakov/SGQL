package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.*

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader
import scala.util.{Success, Try}

trait TypesDefinitionProvider:
    def getType(name: String): Try[Option[AbstractNamedEntityType]]
    def getAllTypes: Try[Seq[AbstractNamedEntityType]]
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractNamedEntityType): Try[Either[GetFieldsParseError, GetFieldsDescriptor]]
    def parseSearchCondition(condition: Option[String], entityType: AbstractNamedEntityType): Try[Either[SearchConditionParseError, SearchCondition]]
    
object TypesDefinitionProvider:
    def create(implicit loader: TypesDefinitionsLoader): TypesDefinitionProvider = TypesDefinitionProviderImpl(loader)

class TypesDefinitionProviderImpl(loader: TypesDefinitionsLoader) extends TypesDefinitionProvider:

    private val typesDefinitionResourcePath = "type_definitions/types.td"
    
    private val _typesDefinitionsMap = loader.load(typesDefinitionResourcePath)
    
    private val larr = EntityType("long_array", LongIdTypeDefinition, ArrayTypeDefinition(Set(SimpleEntityType(LongTypeDefinition)), None))
    private val date = EntityType("date", LongIdTypeDefinition, DateTypeDefinition)
    private val ob1_t = ObjectTypeDefinition(Map(
        "a1" -> SimpleEntityType(IntTypeDefinition),
        "b2" -> larr,
        "c3" -> SimpleEntityType(StringTypeDefinition),
    ), None)
    private val ob1 = EntityType("ob1", UUIDIdTypeDefinition, ob1_t)
    private val ob1_a = ObjectEntitySuperType("obj1_a", ob1_t)
    private val typesDefinitionsMap = Seq(
        EntityType("int", LongIdTypeDefinition, IntTypeDefinition),
        EntityType("long", StringIdTypeDefinition, LongTypeDefinition),
        EntityType("double", IntIdTypeDefinition, DoubleTypeDefinition),
        EntityType("float", UUIDIdTypeDefinition, FloatTypeDefinition),
        EntityType("bool", LongIdTypeDefinition, BooleanTypeDefinition),
        date,
        EntityType("datetime", LongIdTypeDefinition, DateTimeTypeDefinition),
        EntityType("time", LongIdTypeDefinition, TimeTypeDefinition),
        EntityType("bin", LongIdTypeDefinition, BinaryTypeDefinition),
        EntityType("string", LongIdTypeDefinition, StringTypeDefinition),
        EntityType("string_array", LongIdTypeDefinition, ArrayTypeDefinition(Set(SimpleEntityType(StringTypeDefinition)), None)),
        larr,
        EntityType("double-bool_array", LongIdTypeDefinition, ArrayTypeDefinition(Set(SimpleEntityType(DoubleTypeDefinition),
            SimpleEntityType(BooleanTypeDefinition)), None)),
        ob1,
        EntityType("obj2", LongIdTypeDefinition, ObjectTypeDefinition(Map(
            "dd" -> SimpleEntityType(DateTypeDefinition),
            "cc" -> larr,
        ), Some(ob1_a))),
        EntityType("obj4", IntIdTypeDefinition, ObjectTypeDefinition(Map(
            "dd1" -> SimpleEntityType(DateTypeDefinition),
            "cc2" -> larr,
        ), Some(ob1_a))),
        EntityType("obj3", LongIdTypeDefinition, ObjectTypeDefinition( Map(
            "ee" -> SimpleEntityType(DateTypeDefinition),
            "ff" -> larr,
            "gg" -> ob1
        ), None)),
    ).map(t => t.name -> t).toMap ++ _typesDefinitionsMap
    
    

    
                
        

    def getType(name: String): Try[Option[AbstractNamedEntityType]] = Success(typesDefinitionsMap.get(name)) 
    def getAllTypes: Try[Seq[AbstractNamedEntityType]] = Success(typesDefinitionsMap.values.toSeq)
    def parseGetFieldsDescriptor(descriptor: Option[String], entityType: AbstractNamedEntityType): Try[Either[GetFieldsParseError, GetFieldsDescriptor]] = Success(Right(AllGetFieldsDescriptor))
    def parseSearchCondition(condition: Option[String], entityType: AbstractNamedEntityType): Try[Either[SearchConditionParseError, SearchCondition]] = ???
