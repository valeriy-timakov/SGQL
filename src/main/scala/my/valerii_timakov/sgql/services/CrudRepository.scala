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
        
    val availableTypes: Map[PersistenceFieldType, Array[String]] = Map(
        StringFieldType -> Array("VARCHAR"),
        TextFieldType -> Array("TEXT"),
        IntFieldType -> Array("INTEGER"),
        LongFieldType -> Array("BIGINT"),
        FloatFieldType -> Array("FLOAT"),
        DoubleFieldType -> Array("DOUBLE PRECISION"),
        BooleanFieldType -> Array("BOOLEAN"),
        DateFieldType -> Array("DATE"),
        DateTimeFieldType -> Array("TIMESTAMP"),
        TimeFieldType -> Array("TIME"),
        UUIDFieldType -> Array("UUID"),
        BLOBFieldType -> Array("BYTEA"),
    )
    
    def getFieldType(persistenceFieldType: PersistenceFieldType): String = 
        val refType = persistenceFieldType match
            case StringFieldType(size) => availableTypes(StringFieldType).head + s"($size)"
            case other => availableTypes(other).head
    
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
        
        typesDefinitionsProvider.getAllPersistenceData.foreach { persistenceData =>
            persistenceData match {
                case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                    val idType = availableTypes(idColumn.columnType)
                    val sql = s"CREATE TABLE IF NOT EXISTS $tableName (${idColumn.columnName} UUID PRIMARY KEY, ${valueColumn.columnName} ${availableTypes(persistenceData.entityType)});"
                    
                    DB.autoCommit { implicit session =>
                        SQL(sql).execute().apply()
                    }
                case _ =>
            }
        }
        
    }

    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] =
        Success(Some(Entity(id, StringType("test"))))

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???