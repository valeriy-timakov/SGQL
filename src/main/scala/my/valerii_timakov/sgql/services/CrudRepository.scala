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

    private class RefData(
        val tableName: String,
        val columnName: String,
        val refTableData: TableReferenceData
    )
    
    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit = {

        implicit val session: DBSession = AutoSession

//        val existingTableNames = getTableNames().toSet
        
        typesDefinitionsProvider.getAllPersistenceData.foreach {
            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                sql"""
                    CREATE TABLE $tableName (
                        ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                        ${valueColumn.columnName} ${getFieldType(valueColumn.columnType)}
                    )
                """.execute.apply()
            case ArrayTypePersistenceDataFinal(items) =>
                items.foreach {
                    case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                        val (valueColumnName, valueColumnType) = valueColumn match
                            case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) =>
                                (valueColumnName, valueColumnType)
                            case ref: ReferenceValuePersistenceDataFinal =>
                                (ref.columnName, ref.refTableData.idColumnType)
                        val sql = s"""
                            CREATE TABLE $tableName (
                                ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                                $valueColumnName ${getFieldType(valueColumnType)}
                            )"""
                        SQL(sql).execute.apply()
                }
            case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                def getFieldsSql(fields: Map[String, ValuePersistenceDataFinal]): String = fields.values.map {
                    case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
                        s"$columnName ${getFieldType(columnType)}"
                    case ref: ReferenceValuePersistenceDataFinal =>
                        s"${ref.columnName} ${getFieldType(ref.refTableData.idColumnType)}"
                    case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                        val parentSql = parent.map { parentTableRef =>
                            s"${parentTableRef.columnName} ${getFieldType(parentTableRef.refTableData.idColumnType)}, "
                        }.getOrElse("")
                        parentSql + getFieldsSql(fields)
                }.mkString(", ")
                        
                val parentSql = parent.map( parentTableRef =>
                    s", ${parentTableRef.columnName} ${getFieldType(parentTableRef.refTableData.idColumnType)}"
                ).getOrElse("")
                
                sql"""
                    CREATE TABLE $tableName (
                        ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                        ${getFieldsSql(fields)} + $parentSql
                    )
                """.execute.apply()
        }


        typesDefinitionsProvider.getAllPersistenceData.foreach { typeData =>
            (typeData match
                case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) => Nil
                case ArrayTypePersistenceDataFinal(items) =>
                    items.view.flatMap {
                        case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                            valueColumn match
                                case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) => Nil
                                case ref: ReferenceValuePersistenceDataFinal =>
                                    ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList
                    }.toList
                case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                    def getObjectRefData(
                            fields: Map[String, ValuePersistenceDataFinal],
                            parent: Option[ReferenceValuePersistenceDataFinal]
                        ): List[RefData] =
                            val parentRef = parent.flatMap(parentTableRef =>
                                parentTableRef.refTableData.data.map(RefData(tableName, parentTableRef.columnName, _))
                            ).toList
                            getAllRefDatas(fields).appendedAll(parentRef)
                    def getAllRefDatas(fields: Map[String, ValuePersistenceDataFinal]): List[RefData] =
                        fields.values.view.flatMap {
                            case PrimitiveValuePersistenceDataFinal(columnName, columnType) => List()
                            case ref: ReferenceValuePersistenceDataFinal =>
                                ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList
                            case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                                getObjectRefData(fields, parent)
                        }.toList
                    getObjectRefData(fields, parent)
            ).foreach { ref =>
                sql"""
                    ALTER TABLE ${ref.tableName} ADD CONSTRAINT ${foreignKeyName(ref.tableName, ref.columnName, ref.refTableData.tableName)}
                        FOREIGN KEY (${ref.columnName}) REFERENCES ${ref.refTableData.tableName}(${ref.refTableData.idColumn})
                    )
                """.execute.apply()
            }
        }
        
    }

    private def foreignKeyName(tableName: String, columnName: String, refTableName: String): String =
        s"fk_${tableName}_${columnName}_$refTableName"

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