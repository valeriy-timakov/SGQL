package my.valerii_timakov.sgql.services.repositories.postres

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.DbTableMigrationException
import my.valerii_timakov.sgql.services.*
import scala.util.Try
import scalikejdbc._
import scala.collection.mutable

class PostgresCrudRepository(connectionConf: Config, utilsConf: Config) extends CrudRepository:

    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] = ???

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???


    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit =

        Class.forName("org.postgresql.Driver")
        ConnectionPool.singleton(s"jdbc:postgresql://${connectionConf.getString("host")}:${connectionConf.getInt("port")}/" +
            s"${connectionConf.getString("database")}typesSchemaParam",
            connectionConf.getString("username"), connectionConf.getString("password"))
        DB.localTx(implicit session =>
            val allTypesData = typesDefinitionsProvider.getAllPersistenceData
            createOrMigrateTables(allTypesData)
        )


    private val availableTypes: Map[PersistenceFieldType, String] = Map(
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
    private val metadataUtils = MetadataUtils()

    private def getFieldType(persistenceFieldType: PersistenceFieldType): String = persistenceFieldType match
        case StringFieldType(size) => availableTypes(StringFieldType) + s"($size)"
        case other => availableTypes(other)

    private val typesSchemaName = connectionConf.getString("schema")
    private val primaryKeySuffix = connectionConf.getString("primary-key-suffix")
    private val foreignKeySuffix = connectionConf.getString("foreign-key-suffix")
    private val archivedColumnNameSuffix = connectionConf.getString("archived-column-name-suffix")



    private val typesSchemaParam = if typesSchemaName != null then s"?currentSchema=$typesSchemaName." else ""

    private var typesPersistenceData: Map[AbstractNamedEntityType, TypePersistenceData] = Map()

    private class RefData(
        val tableName: String,
        val columnName: String,
        val refTableData: TableReferenceData
    )

        
    private def createOrUpdateForeignKeys(
        allTypesData: Seq[TypePersistenceDataFinal]
    )(implicit session: DBSession): Unit =

        val foreignKeysCache = mutable.Map[String, Map[String, ForeignKeyData]]()

        def getForeignKeysMap(tableName: String): Map[String, ForeignKeyData] =
            foreignKeysCache.getOrElseUpdate(tableName, metadataUtils.getForeignKeys(tableName)
                .map(fk => (fk.columnName, fk)).toMap)

        def getForeignKey(tableName: String, columnName: String): Option[ForeignKeyData] =
            getForeignKeysMap(tableName).get(columnName)

        def foreignKeyName(tableName: String, columnName: String, refTableName: String): String =
            s"fk_${tableName}_${columnName}_$refTableName"


        def getObjectRefData(tableName: String,
                             fields: Map[String, ValuePersistenceDataFinal],
                             parent: Option[ReferenceValuePersistenceDataFinal],
        ): List[RefData] =
            val parentRef = parent.flatMap(parentTableRef =>
                parentTableRef.refTableData.data.map(RefData(tableName, parentTableRef.columnName, _))
            ).toList
            getAllRefDatas(tableName, fields).appendedAll(parentRef)

        def getAllRefDatas(tableName: String,
                           fields: Map[String, ValuePersistenceDataFinal],
        ): List[RefData] =
            fields.values.view.flatMap {
                case PrimitiveValuePersistenceDataFinal(columnName, columnType) => List()
                case ref: ReferenceValuePersistenceDataFinal =>
                    ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList
                case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                    getObjectRefData(tableName, fields, parent)
            }.toList

        def createForeignKey(tableName: String, columnName: String, refTableName: String, refColumnName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName ADD CONSTRAINT ${foreignKeyName(tableName, columnName, refTableName)}
                    FOREIGN KEY ($columnName) REFERENCES $refTableName($refColumnName)
            """).execute.apply()

        def dropForeignKey(tableName: String, keyName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName DROP CONSTRAINT $keyName
            """).execute.apply()

        allTypesData.foreach { typeData =>
            (typeData match
                case PrimitiveTypePersistenceDataFinal(_, _, _) => Nil
                case ArrayTypePersistenceDataFinal(items) =>
                    items.view.flatMap {
                        case ItemTypePersistenceDataFinal(tableName, _, valueColumn) =>
                            valueColumn match
                                case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) => Nil
                                case ref: ReferenceValuePersistenceDataFinal =>
                                    ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList
                    }.toList
                case ObjectTypePersistenceDataFinal(tableName, _, fields, parent) =>
                    getObjectRefData(tableName, fields, parent)
                ).foreach { ref =>
                //TODO check not actual foreing keys and investigate if FK should be dropped on tables altering
                    getForeignKey(ref.tableName, ref.columnName) match
                        case Some(fk) =>
                            if fk.refTableName != ref.refTableData.tableName || fk.refColumnName != ref.refTableData.idColumn.columnName then
                                dropForeignKey(ref.tableName, fk.keyName)
                                createForeignKey(ref.tableName, ref.columnName, ref.refTableData.tableName,
                                    ref.refTableData.idColumn.columnName)
                        case None =>
                            createForeignKey(ref.tableName, ref.columnName, ref.refTableData.tableName,
                                ref.refTableData.idColumn.columnName)
                }
        }

    private def createOrMigrateTables(
        typesPersistenceData: Seq[TypePersistenceDataFinal],
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
            val pkDataOption = metadataUtils.getTablePrimaryKeys(tableName)
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
            val existingColumns: Map[String, ColumnData] =  metadataUtils.getTableColumnsDataMap(tableName)
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns)
            checkAndFixExistingTableValueColumn(tableName, valueColumnName, valueColumnType, existingColumns)


        def checkAndFixExistingSimpleObjectValueTable(
            tableName: String,
            fields: Map[String, ValuePersistenceDataFinal],
            parent: Option[ReferenceValuePersistenceDataFinal],
            existingColumns: Map[String, ColumnData],
        ): Unit =
            parent.foreach(parentTableRef =>
                checkAndFixExistingTableValueColumn(tableName, parentTableRef.columnName, 
                    parentTableRef.refTableData.idColumnType, existingColumns)
            )
            fields.foreach((fieldName, fieldData) =>
                fieldData match
                    case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
                        checkAndFixExistingTableValueColumn(tableName, columnName, columnType, existingColumns)
                    case ref: ReferenceValuePersistenceDataFinal =>
                        checkAndFixExistingTableValueColumn(tableName, ref.columnName, ref.refTableData.idColumnType, 
                            existingColumns)
                    case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                        checkAndFixExistingSimpleObjectValueTable(tableName, fields, parent, existingColumns)
            )

        def checkAndFixExistingObjectValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            fields: Map[String, ValuePersistenceDataFinal],
            parent: Option[ReferenceValuePersistenceDataFinal],
            existingColumnsOption: Option[Map[String, ColumnData]] = None, 
        ): Unit =
            val existingColumns: Map[String, ColumnData] = existingColumnsOption.getOrElse( 
                metadataUtils.getTableColumnsDataMap(tableName) )
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns)
            checkAndFixExistingSimpleObjectValueTable(tableName, fields, parent, existingColumns)
            
        def createObjectValueTable(
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


        val existingTableNames = metadataUtils.getTableNames

        typesPersistenceData.foreach {
            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                if !existingTableNames.contains(tableName)
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
                        if !existingTableNames.contains(tableName)
                            then createSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType)
                            else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType)
                }
            case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                if !existingTableNames.contains(tableName)
                    then createObjectValueTable(tableName, idColumn, fields, parent)
                    else checkAndFixExistingObjectValueTable(tableName, idColumn, fields, parent)
        }


