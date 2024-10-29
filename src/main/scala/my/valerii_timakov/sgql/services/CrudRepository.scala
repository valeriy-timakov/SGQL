package my.valerii_timakov.sgql.services


import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.*

import scala.util.{Success, Try}


trait CrudRepository:
    def create(entityType: EntityType, data: EntityFieldType): Try[Entity]

    def update(entityType: EntityType, entity: Entity): Try[Option[Entity]]

    def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]]

    def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]]

    def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Seq[Entity]]

class TestCrudRepository extends CrudRepository:
    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] =
        Success(Some(Entity(id, StringType("test"))))

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???


import scalikejdbc._

class PostgresCrudRepository(conf: Config) extends CrudRepository:
        
    val availableTypes: Map[RootPrimitiveTypeDefinition, Array[String]] = Map(
        StringTypeDefinition -> Array("TEXT"),
        IntTypeDefinition -> Array("INTEGER"),
        LongTypeDefinition -> Array("BIGINT"),
        FloatTypeDefinition -> Array("FLOAT"),
        DoubleTypeDefinition -> Array("DOUBLE PRECISION"),
        BooleanTypeDefinition -> Array("BOOLEAN"),
        DateTypeDefinition -> Array("DATE"),
        DateTimeTypeDefinition -> Array("TIMESTAMP"),
        TimeTypeDefinition -> Array("TIME"),
        UUIDTypeDefinition -> Array("UUID"),
        BinaryTypeDefinition -> Array("BYTEA"),
    )
    
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"jdbc:postgresql://${conf.getString("host")}:${conf.getInt("port")}/${conf.getString("database")}", 
        conf.getString("username"), conf.getString("password"))
    
    var typesPersistenceData: Map[AbstractNamedEntityType, TypePersistenceData] = Map()
    
    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit = {

        // Виконання операцій з базою даних
        implicit val session: DBSession = AutoSession
        
        typesDefinitionsProvider.getAllPersistenceData.foreach {
            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                val createTableSQL = s"CREATE TABLE $tableName (${idColumn.columnName} UUID PRIMARY KEY, ${valueColumn.columnName} TEXT)"
                SQL(createTableSQL).execute.apply()
            case _ =>
        }
        
    }

    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] =
        Success(Some(Entity(id, StringType("test"))))

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???