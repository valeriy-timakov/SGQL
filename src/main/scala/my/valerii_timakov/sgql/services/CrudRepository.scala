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
        
    val availableTypes: Map[PersistenceFieldType, String] = Map(
        StringFieldType -> "VARCHAR",
        TextFieldType -> "TEXT",
        IntFieldType -> "INTEGER",
        LongFieldType -> "BIGINT",
        FloatFieldType -> "FLOAT",
        DoubleFieldType -> "DOUBLE PRECISION",
        BooleanFieldType -> "BOOLEAN",
        DateFieldType -> "DATE",
        DateTimeFieldType -> "TIMESTAMP",
        TimeFieldType -> "TIME",
        UUIDFieldType -> "UUID",
        BLOBFieldType -> "BYTEA",
    )
    
    def getFieldType(persistenceFieldType: PersistenceFieldType): String = persistenceFieldType match
        case StringFieldType(size) => availableTypes(StringFieldType) + s"($size)"
        case other => availableTypes(other)
    
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"jdbc:postgresql://${conf.getString("host")}:${conf.getInt("port")}/${conf.getString("database")}", 
        conf.getString("username"), conf.getString("password"))
    
    var typesPersistenceData: Map[AbstractNamedEntityType, TypePersistenceData] = Map()
    
    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit = {

        // Виконання операцій з базою даних
        implicit val session: DBSession = AutoSession

//        val existingTableNames = getTableNames().toSet
        
        typesDefinitionsProvider.getAllPersistenceData.foreach {
            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                val createTableSQL = sql"""
                        CREATE TABLE $tableName (
                            ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                            ${valueColumn.columnName} ${getFieldType(valueColumn.columnType)}
                        )
                    """.execute.apply()
            case ArrayTypePersistenceDataFinal(items) =>
                items.foreach {
                    case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                        valueColumn match
                            case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) =>
                                sql"""
                                    CREATE TABLE $tableName (
                                        ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                                        $valueColumnName ${getFieldType(valueColumnType)}
                                    )
                                """.execute.apply()
//                            case ref: ReferenceValuePersistenceDataFinal =>
//                                val createTableSQL = sql"""
//                                    CREATE TABLE $tableName (
//                                        ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
//                                        ${ref.columnName} ${getFieldType(ref.valueColumn.columnType)}
//                                    )
//                                """.execute.apply()
                }
//            case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
//                val createTableSQL = sql"""
//                    CREATE TABLE $tableName (
//                        ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
//                        ${fields.map { case (name, field) => s"$name ${getFieldType(field.columnType)}" }.mkString(", ")}
//                    )
//                """.execute.apply()
        }
        
    }

    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] =
        Success(Some(Entity(id, StringType("test"))))

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???


    private def getTableNames()(implicit session: DBSession): List[String] = {
        val conn = session.connection // Отримуємо з'єднання з базою даних
        val metaData = conn.getMetaData

        // Отримання списку таблиць
        val rs = metaData.getTables(null, null, "%", Array("TABLE"))
        var tables = List[String]()

        while (rs.next()) {
            val tableName = rs.getString("TABLE_NAME")
            tables = tableName :: tables
        }

        rs.close()
        tables.reverse // Повертаємо таблиці у правильному порядку
    }

    private def getTableColumns(tableName: String)(implicit session: DBSession): List[(String, String)] = {
        val conn = session.connection
        val metaData = conn.getMetaData

        // Отримання інформації про колонки таблиці
        val rs = metaData.getColumns(null, null, tableName, "%")
        var columns = List[(String, String)]()

        while (rs.next()) {
            val columnName = rs.getString("COLUMN_NAME")
            val columnType = rs.getString("TYPE_NAME")
            columns = (columnName, columnType) :: columns
        }

        rs.close()
        columns.reverse // Повертаємо колонки у правильному порядку
    }