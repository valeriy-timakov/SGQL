package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.{AbstractNamedEntityType, BinaryTypeDefinition, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntityType, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, PrimitiveFieldTypeDefinition, RootPrimitiveTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypesDefinitionsParseError, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.{NoTypeFound, TypesLoadExceptionException}

import scala.io.Source
import scala.util.parsing.input.StreamReader

trait PersistanceConfigLoader:
    def load(dataResourcePath: String, typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, AbstractTypePersistanceData]


val primitiveTypeDefinitionToFieldType: Map[RootPrimitiveTypeDefinition, FieldType] = Map(
    LongTypeDefinition -> LongFieldType,
    IntTypeDefinition -> IntFieldType,
    StringTypeDefinition -> StringFieldType(0),
    DoubleTypeDefinition -> DoubleFieldType,
    FloatTypeDefinition -> FloatFieldType,
    BooleanTypeDefinition -> BooleanFieldType,
    DateTypeDefinition -> DateFieldType,
    DateTimeTypeDefinition -> DateTimeFieldType,
    TimeTypeDefinition -> TimeFieldType,
    UUIDTypeDefinition -> UUIDFieldType,
    BinaryTypeDefinition -> BLOBFieldType)

val idTypeDefinitionToFieldType: Map[EntityIdTypeDefinition, FieldType] = Map(
    LongIdTypeDefinition -> LongFieldType,
    IntIdTypeDefinition -> IntFieldType,
    StringIdTypeDefinition -> StringFieldType(0),
    UUIDIdTypeDefinition -> UUIDFieldType)


object PersistanceConfigLoader extends PersistanceConfigLoader:
    override def load(dataResourcePath: String, typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, AbstractTypePersistanceData] =
        val tdSource = Source.fromResource (dataResourcePath)
        PersistenceConfigParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: RootPackagePersistanceData) =>
                val typesDataPersistenceMap: Map[String, AbstractTypePersistanceData] = packageData.toMap

                typesDefinitionsMap.map {
                    case (typeName, typeDef) =>
                        typesDataPersistenceMap.get(typeName) match
                            case Some(persistanceData) =>
                                typeName -> persistanceData
                            case None =>
                                throw new TypesLoadExceptionException(s"Type $typeName not found in persistance data")
                }
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)
                
    private def createTypePersistanceData(typeName: String, typeDef: AbstractNamedEntityType): AbstractTypePersistanceData =
        typeDef match
            case EntityType(name, idType, valueType) => valueType match
                case RootPrimitiveTypeDefinition(typeName) =>
                    val idColumn = SimpleValuePersistenceData(
                        Some("id"), 
                        Some(idTypeDefinitionToFieldType.getOrElse(idType, throw new NoTypeFound(idType.name)))
                    )
                    val valueColumn = valueType match
                        case td: RootPrimitiveTypeDefinition =>
                            SimpleValuePersistenceData(
                                Some("value"), 
                                Some(primitiveTypeDefinitionToFieldType.getOrElse(td, throw new NoTypeFound(name)))
                            )
                        case td: PrimitiveFieldTypeDefinition => 
                            SimpleValuePersistenceData(
                                Some("value"),
                                Some(primitiveTypeDefinitionToFieldType.getOrElse(td.rootType, throw new NoTypeFound(name)))
                            )
                        case _ =>
                            throw new TypesLoadExceptionException(s"Type $typeName not supported")
                    SimpleTypePersistanceData(typeName, Some(tableNameFromTypeName(typeName)), Some(idColumn), Some(valueColumn))
                    
                    
    private def tableNameFromTypeName(typeName: String): String = typeName.toLowerCase().replace(".", "_")
    private def columnNameFromFieldName(fieldName: String): String = camelCaseToSnakeCase(fieldName)
    private def camelCaseToSnakeCase(name: String): String = name.replaceAll("[A-Z]", "_$0").toLowerCase()
                
