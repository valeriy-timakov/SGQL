package my.valerii_timakov.sgql.services


import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.{DbTableMigrationException, PersistenceRepositoryTypeNotFoundException}

import java.sql.{DatabaseMetaData, ResultSet}
import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.immutable.Set
import scala.collection.mutable.Builder
import scala.util.{Success, Try, Using}

object CrudRepositoriesFactory:
    def createRopository(conf: Config): CrudRepository =
        if conf.hasPath("postgres") then
            new PostgresCrudRepository(conf.getConfig("postgres"), conf.getConfig("utils"))
        else
            throw new PersistenceRepositoryTypeNotFoundException


trait CrudRepository:
    def create(entityType: EntityType, data: EntityFieldType): Try[Entity]

    def update(entityType: EntityType, entity: Entity): Try[Option[Entity]]

    def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]]

    def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]]

    def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Seq[Entity]]

    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit


import scalikejdbc._

class PostgresCrudRepository(connectionConf: Config, utilsConf: Config) extends CrudRepository:
        
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

    private val dbUtils = PostgresDBInitUtils(utilsConf)

    private def getFieldType(persistenceFieldType: PersistenceFieldType): String = persistenceFieldType match
        case StringFieldType(size) => availableTypes(StringFieldType) + s"($size)"
        case other => availableTypes(other)

    private val typesSchemaName = connectionConf.getString("schema")
    private val primaryKeySuffix = connectionConf.getString("primary-key-suffix")
    private val foreignKeySuffix = connectionConf.getString("foreign-key-suffix")
    private val archivedColumnNameSuffix = connectionConf.getString("archived-column-name-suffix")



    private val typesSchemaParam = if typesSchemaName != null then s"?currentSchema=$typesSchemaName." else ""
    
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(s"jdbc:postgresql://${connectionConf.getString("host")}:${connectionConf.getInt("port")}/" +
        s"${connectionConf.getString("database")}typesSchemaParam",
        connectionConf.getString("username"), connectionConf.getString("password"))

    private var typesPersistenceData: Map[AbstractNamedEntityType, TypePersistenceData] = Map()

    private class RefData(
        val tableName: String,
        val columnName: String,
        val refTableData: TableReferenceData
    )

    private case class ColumnData(
        name: String,
        columnType: String,
        isPrimaryKey: Boolean,
        isNullable: Boolean,
        defaultValue: Option[String]
    )

    private case class PrimaryKeyData(
        keyName: String,
        tableName: String,
        columnNames: Set[String]
    )

    private case class ForeignKeyData(
        keyName: String,
        tableName: String,
        columnName: String,
        refTableName: String,
        refColumnName: String
    )

    
    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit =

        implicit val session: DBSession = AutoSession

        val existingTableNames = getTableNames

        val allTypesData = typesDefinitionsProvider.getAllPersistenceData


        createNotExistingTables(allTypesData, existingTableNames)

//        def createSingleValueTable(
//            tableName: String,
//            idColumn: PrimitiveValuePersistenceDataFinal,
//            valueColumnName: String,
//            valueColumnType: PersistenceFieldType,
//        ): Unit =
//            val sql = s"""
//                CREATE TABLE $tableName (
//                    ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
//                    ${valueColumnName} ${getFieldType(valueColumnType)}
//                )
//            """
//            SQL(sql).execute.apply()
//
//        typesDefinitionsProvider.getAllPersistenceData.foreach {
//            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
//                createSingleValueTable(tableName, idColumn, valueColumn.columnName, valueColumn.columnType)
//            case ArrayTypePersistenceDataFinal(items) =>
//                items.foreach {
//                    case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
//                        val (valueColumnName, valueColumnType) = valueColumn match
//                            case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) =>
//                                (valueColumnName, valueColumnType)
//                            case ref: ReferenceValuePersistenceDataFinal =>
//                                (ref.columnName, ref.refTableData.idColumnType)
//                        createSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType)
//                }
//            case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
//                def getFieldsSql(fields: Map[String, ValuePersistenceDataFinal]): String = fields.values.map {
//                    case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
//                        s"$columnName ${getFieldType(columnType)}"
//                    case ref: ReferenceValuePersistenceDataFinal =>
//                        s"${ref.columnName} ${getFieldType(ref.refTableData.idColumnType)}"
//                    case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
//                        val parentSql = parent.map { parentTableRef =>
//                            s"${parentTableRef.columnName} ${getFieldType(parentTableRef.refTableData.idColumnType)}, "
//                        }.getOrElse("")
//                        parentSql + getFieldsSql(fields)
//                }.mkString(", ")
//
//                val parentSql = parent.map( parentTableRef =>
//                    s", ${parentTableRef.columnName} ${getFieldType(parentTableRef.refTableData.idColumnType)}"
//                ).getOrElse("")
//
//                val sql = s"""
//                    CREATE TABLE $tableName (
//                        ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
//                        ${getFieldsSql(fields)} + $parentSql
//                    )
//                """
//                SQL(sql).execute.apply()
//        }


        allTypesData.foreach { typeData =>
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

    private def createNotExistingTables(
                                           typesPersistenceData: Seq[TypePersistenceDataFinal],
                                           existingTables: Set[String]
                                       )(implicit session: DBSession): Unit =

        def createSingleValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
        ): Unit =
            SQL(s"""
                CREATE TABLE $tableName (
                    ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                    $valueColumnName ${getFieldType(valueColumnType)}, 
                    CONSTRAINT $tableName$primaryKeySuffix PRIMARY KEY (${idColumn.columnName})
                )
            """).execute.apply()

        def renameColumn(tableName: String, prevColumnName: String, newColumnName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName RENAME COLUMN $prevColumnName TO $newColumnName
            """).execute.apply()
            dbUtils.addTableRenamingData(tableName, prevColumnName, newColumnName)

        def dropConstraint(tableName: String, constraintName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName DROP CONSTRAINT $constraintName
            """).execute.apply()

        def createPrimaryKey(tableName: String, columnName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName ADD CONSTRAINT $tableName$primaryKeySuffix
                    PRIMARY KEY ($columnName);
            """).execute.apply()

        def addPrimaryKeyColumn(tableName: String, columnName: String, columnType: String): Unit =
            addColumn(tableName, columnName, columnType)
            SQL(s"""
                ALTER TABLE $tableName ADD CONSTRAINT $tableName$primaryKeySuffix PRIMARY KEY ($columnName)
            """).execute.apply()

        def addColumn(tableName: String, columnName: String, columnType: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName ADD COLUMN $columnName $columnType
            """).execute.apply()
            
        def getRenamedArchivedColumnName(columnName: String, existingColumns: Map[String, ColumnData]) =
            val result = columnName + archivedColumnNameSuffix
            var i = 1
            while existingColumns.contains(result + i) do i += 1
            result + i
            
        def checkAndFixExistingTableIdColumn(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            existingColumns: Map[String, ColumnData], 
        ): Unit =
            val pkDataOption = getTablePrimaryKeys(tableName)
            if pkDataOption.forall(_.columnNames.size <= 1) then
                try
                    if existingColumns.contains(idColumn.columnName) then
                        if pkDataOption.isDefined then
                            val pkData = pkDataOption.get
                            val pkColumn = pkData.columnNames.head
                            if pkColumn == idColumn.columnName then
                                if existingColumns(pkColumn).columnType != getFieldType(idColumn.columnType) then
                                    //ignoring else - ID column with PK of same type already exists - no actions
                                    dropConstraint(tableName, pkData.keyName)
                                    renameColumn(tableName, pkColumn, getRenamedArchivedColumnName(pkColumn, existingColumns))
                                    addPrimaryKeyColumn(tableName, idColumn.columnName, getFieldType(idColumn.columnType))
                                    dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName)
                            else
                                dropConstraint(tableName, pkData.keyName)
                                createPrimaryKey(tableName, idColumn.columnName)
                                dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName)
                        else
                            createPrimaryKey(tableName, idColumn.columnName)
                            dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName)
                    else if pkDataOption.isEmpty then
                        addPrimaryKeyColumn(tableName, idColumn.columnName, getFieldType(idColumn.columnType))
                        dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName)
                    else
                        val pkData = pkDataOption.get
                        val pkColumn = pkData.columnNames.head
                        if existingColumns(pkColumn).columnType == getFieldType(idColumn.columnType) then
                            renameColumn(tableName, pkColumn, idColumn.columnName)
                        else
                            dropConstraint(tableName, pkData.keyName)
                            addPrimaryKeyColumn(tableName, idColumn.columnName, getFieldType(idColumn.columnType))
                            dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName)
                catch
                    case e: Exception => throw DbTableMigrationException(tableName, e)
            else
                throw DbTableMigrationException(tableName)
                
        def checkAndFixExistingTableValueColumn(
            tableName: String,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
            existingColumns: Map[String, ColumnData],
        ): Unit =
            if existingColumns.contains(valueColumnName) then
                if existingColumns(valueColumnName).columnType != getFieldType(valueColumnType) then
                    //ignoring else - Value column of same type already exists - no actions
                    renameColumn(tableName, valueColumnName, getRenamedArchivedColumnName(valueColumnName, existingColumns))
                    addColumn(tableName, valueColumnName, getFieldType(valueColumnType))
                    dbUtils.addTableRenamingData(tableName, null, valueColumnName)
            else
                addColumn(tableName, valueColumnName, getFieldType(valueColumnType))
                dbUtils.addTableRenamingData(tableName, null, valueColumnName)

        def checkAndFixExistingSingleValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
        ): Unit =
            val existingColumns: Map[String, ColumnData] =  getTableColumnsDataMap(tableName)
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns)
            checkAndFixExistingTableValueColumn(tableName, valueColumnName, valueColumnType, existingColumns)

        def createObjectValueTalbe(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            fields: Map[String, ValuePersistenceDataFinal],
            parent: Option[ReferenceValuePersistenceDataFinal],
        ): Unit =
            val parentSql = parent.map( parentTableRef =>
                s", ${parentTableRef.columnName} ${getFieldType(parentTableRef.refTableData.idColumnType)}"
            ).getOrElse("")

            SQL(s"""
                CREATE TABLE $tableName (
                    ${idColumn.columnName} ${getFieldType(idColumn.columnType)} PRIMARY KEY,
                    ${getFieldsSql(fields)} + $parentSql
                )
            """).execute.apply()

        def checkAndFixExistingObjectValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            fields: Map[String, ValuePersistenceDataFinal],
            parent: Option[ReferenceValuePersistenceDataFinal],
        ): Unit =
            val existingColumns: Map[String, ColumnData] =  getTableColumnsDataMap(tableName)
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns)

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

        typesPersistenceData.foreach {
            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                if !existingTables.contains(tableName)
                    then createSingleValueTable(tableName, idColumn, valueColumn.columnName, valueColumn.columnType)
                    else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumn.columnName, valueColumn.columnType)
            case ArrayTypePersistenceDataFinal(items) =>
                items.foreach {
                    case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                        val (valueColumnName, valueColumnType) = valueColumn match
                            case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) =>
                                (valueColumnName, valueColumnType)
                            case ref: ReferenceValuePersistenceDataFinal =>
                                (ref.columnName, ref.refTableData.idColumnType)
                        if !existingTables.contains(tableName)
                            then createSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType)
                            else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType)
                }
            case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                if !existingTables.contains(tableName)
                    then createObjectValueTalbe(tableName, idColumn, fields, parent)
                    else checkAndFixExistingObjectValueTable(tableName, idColumn, fields, parent)
        }


    private def foreignKeyName(tableName: String, columnName: String, refTableName: String): String =
        s"fk_${tableName}_${columnName}_$refTableName"

    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] =
        Success(Some(Entity(id, StringType("test"))))

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???


    private def getTableNames: Set[String] =
        DB.autoCommit { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(metaData.getTables(null, null, "%", Array("TABLE"))) { rs =>
                var tables = Set[String]()

                while (rs.next()) {
                    val tableName = rs.getString("TABLE_NAME")
                    tables += tableName
                }

                tables
            }
        }

    private def getTableColumnsDataMap(tableName: String): Map[String, ColumnData] =
        val primaryKeyColumns = getTablePrimaryKeys(tableName).map(_.columnNames).getOrElse(Set())
        DB.autoCommit { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(metaData.getColumns(null, null, tableName, "%")) { rs =>
                val columns = Map.newBuilder[String, ColumnData]

                while (rs.next()) {
                    val columnName = rs.getString("COLUMN_NAME")
                    val columnType = rs.getString("TYPE_NAME")
                    val isNullable = rs.getString("IS_NULLABLE") == "YES"
                    val defaultValue = Option(rs.getString("COLUMN_DEF"))
                    columns += columnName -> ColumnData(columnName, columnType, primaryKeyColumns.contains(columnName),
                        isNullable, defaultValue)
                }

                columns.result()
            }
        }

    private def getTablePrimaryKeys(tableName: String): Option[PrimaryKeyData] =
        val res = getMetadata(
            metaData => metaData.getPrimaryKeys(null, null, tableName),
            rs =>(rs.getString("PK_NAME"), rs.getString("COLUMN_NAME")),
            Set.newBuilder[(String, String)])
        if res.nonEmpty
            then Some(PrimaryKeyData(res.head._1, tableName, res.map(_._2)))
            else None

    private def getForeignKeys(tableName: String): Set[ForeignKeyData] =
        getMetadata(
            metaData => metaData.getImportedKeys(null, null, tableName),
            rs => ForeignKeyData(rs.getString("FK_NAME"), rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
                rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")),
            Set.newBuilder[ForeignKeyData])

    private def getMetadata[R, CC](
        query: DatabaseMetaData => ResultSet,
        extractor: ResultSet => R,
        builder: mutable.Builder[R, CC]
    ): CC =
        DB.autoCommit { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(query(metaData)) { rs =>
                builder += extractor(rs)
            }

            builder.result()
        }


private class PostgresDBInitUtils(conf: Config):
    def init(): Unit =
        val utilsSchemaName = conf.getString("utils-schema")
        val trc = conf.getConfig("tables-altering")
        DB.autoCommit { implicit session =>
            SQL(
                s"""CREATE SCHEMA IF NOT EXISTS $utilsSchemaName"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString("renaming-columns-table-name")} (
                    ${trc.getString("id-column")} SERIAL PRIMARY KEY,
                    ${trc.getString("altering-table-column")} ${trc.getString("db-names-column-type")} NOT NULL,
                    ${trc.getString("previous-name-column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("new-name-column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("operation-date-column")} ${trc.getString("operation-date-column-type")} NOT NULL,
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString("primary-key-altering-table-name")} (
                    ${trc.getString("id-column")} SERIAL PRIMARY KEY,
                    ${trc.getString("altering-table-column")} ${trc.getString("db-names-column-type")} NOT NULL,
                    ${trc.getString("previous_primary_key_column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("new_primary_key_column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("operation-date-column")} ${trc.getString("operation-date-column-type")} NOT NULL,
                )"""
            ).execute.apply()
        }

    def addTableRenamingData(alteringTableName: String, prevColumnName: String, newColumnName: String): Unit =
        addItemChangeData(alteringTableName, prevColumnName, newColumnName, "previous-name-column", "new-name-column")

    def addPrimaryKeyAlteringData(alteringTableName: String, prevColumnName: String, newColumnName: String): Unit =
        addItemChangeData(alteringTableName, prevColumnName, newColumnName, "previous_primary_key_column", "new_primary_key_column")

    private def addItemChangeData(
        alteringTableName: String,
        prevValue: String,
        newValue: String,
        prevValueColConfigName: String,
        newValueColConfigName: String
    ): Unit =
        val utilsSchemaName = conf.getString("utils-schema")
        val trc = conf.getConfig("tables-altering")
        DB.autoCommit { implicit session =>
            SQL(
                s"""INSERT INTO $utilsSchemaName.${trc.getString("renaming-columns-table-name")} (
                        ${trc.getString("altering-table-column")},
                        ${trc.getString(prevValueColConfigName)},
                        ${trc.getString(newValueColConfigName)},
                        ${trc.getString("operation-date-column")}
                    ) VALUES ($$1, $$2, $$3, $$4)"""
            )
                .bind(alteringTableName, prevValue, newValue, LocalDateTime.now())
                .update.apply()
        }



