package my.valerii_timakov.sgql.services.repositories.postres

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.DbTableMigrationException
import my.valerii_timakov.sgql.services.*
import scala.util.Try
import scalikejdbc._
import scala.collection.mutable

class PostgresCrudRepository(connectionConf: Config, persistenceConf: Config) extends CrudRepository:

    override def create(entityType: EntityType, data: EntityFieldType): Try[Entity] = ???

    override def update(entityType: EntityType, entity: Entity): Try[Option[Entity]] = ???

    override def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]] = ???

    override def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]] = ???

    override def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity]] = ???


    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit =

        val typesSchemaParam = if typesSchemaName != null then s"?currentSchema=$typesSchemaName" else ""
        Class.forName("org.postgresql.Driver")
        ConnectionPool.singleton(s"jdbc:postgresql://${connectionConf.getString("host")}:" +
            s"${connectionConf.getInt("port")}/${connectionConf.getString("database")}$typesSchemaParam",
            connectionConf.getString("username"), connectionConf.getString("password"))

        DB.autoCommit { implicit session =>
            SQL(
                s"""CREATE SCHEMA IF NOT EXISTS $typesSchemaName"""
            ).execute.apply()
        }

        DB.localTx(implicit session =>
            val allTypesData = typesDefinitionsProvider.getAllPersistenceData
            createOrMigrateTables(allTypesData)
        )

    private val specificIdTypes: Map[PersistenceFieldType, Set[String]] = Map(
        LongFieldType -> Set("BIGSERIAL", "SERIAL8"),
        IntFieldType -> Set("SERIAL", "SERIAL4"),
        ShortIntFieldType -> Set("SMALLSERIAL", "SERIAL2"),
    )

    private val availableTypes: Map[PersistenceFieldType, Set[String]] = Map(
        BLOBFieldType -> Set("BYTEA"),
        DoubleFieldType -> Set("FLOAT8", "DOUBLE PRECISION"),
        FloatFieldType -> Set("FLOAT4", "REAL"),
        DecimalFieldType -> Set("NUMERIC", "DECIMAL"),
        LongFieldType -> Set("BIGINT", "INT8"),
        IntFieldType -> Set("INTEGER", "INT", "INT4"),
        ShortIntFieldType -> Set("SMALLINT", "INT2"),
        BooleanFieldType -> Set("BOOLEAN", "BOOL"),
        FixedStringFieldType -> Set("CHARACTER", "CHAR"),
        StringFieldType -> Set("VARCHAR", "CHARACTER VARYING"),
        DateFieldType -> Set("DATE"),
        TextFieldType -> Set("TEXT"),
        TimeFieldType -> Set("TIME"),
        TimeWithTimeZoneFieldType -> Set("TIMETZ", "TIME WITH TIME ZONE"),
        DateTimeFieldType -> Set("TIMESTAMP"),
        DateTimeWithTimeZoneFieldType -> Set("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"),
        UUIDFieldType -> Set("UUID"),
    )


    private val dbUtils = PostgresDBInitUtils(persistenceConf.getConfig("utils"))
    private val metadataUtils = MetadataUtils()

    private def getFieldType(persistenceFieldType: PersistenceFieldType): String = persistenceFieldType match
        case StringFieldType(size) => availableTypes(StringFieldType).head + s"($size)"
        case other => availableTypes(other).head

    private def getIdFieldType(persistenceFieldType: PersistenceFieldType): String =
        specificIdTypes.get(persistenceFieldType).map(_.head)
            .getOrElse(getFieldType(persistenceFieldType))

    private def isSameType(persistenceType: PersistenceFieldType, dbTypeName: String): Boolean =
        availableTypes.get(persistenceType).exists(_.contains(dbTypeName))

    private def isSameIdType(persistenceType: PersistenceFieldType, dbTypeName: String): Boolean =
        specificIdTypes.get(persistenceType)
            .orElse(availableTypes.get(persistenceType))
            .exists(_.contains(dbTypeName))

    private val typesSchemaName = connectionConf.getString("schema")
    private val primaryKeySuffix = persistenceConf.getString("primary-key-suffix")
    private val foreignKeySuffix = persistenceConf.getString("foreign-key-suffix")
    private val archivedColumnNameSuffix = persistenceConf.getString("archived-column-name-suffix")

    private var typesPersistenceData: Map[AbstractNamedEntityType, TypePersistenceData] = Map()

    private class RefData(
        val tableName: String,
        val columnName: String,
        val refTableData: TableReferenceData
    )


    private def createOrMigrateTables(
        typesPersistenceData: Seq[TypePersistenceDataFinal],
    )(implicit session: DBSession): Unit =

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

        def createSingleValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
        ): Unit =
            SQL(s"""
                CREATE TABLE $tableName (
                    ${idColumn.columnName} ${getIdFieldType(idColumn.columnType)},
                    $valueColumnName ${getFieldType(valueColumnType)}, 
                    CONSTRAINT $tableName$primaryKeySuffix PRIMARY KEY (${idColumn.columnName})
                )
            """).execute.apply()

        def renameColumn(tableName: String, prevColumnName: String, newColumnName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName RENAME COLUMN $prevColumnName TO $newColumnName
            """).execute.apply()
            dbUtils.addTableRenamingData(tableName, prevColumnName, newColumnName)

        def dropPrimaryKey(pk: PrimaryKeyData): Unit =
            dropConstraint(pk.tableName, pk.keyName, true)

        def dropForeignKey(fk: ForeignKeyData): Unit =
            dropConstraint(fk.tableName, fk.keyName, false)

        def dropConstraint(tableName: String, keyName: String, cascade: Boolean): Unit =
            val cascadeSql = if cascade then " CASCADE" else ""
            SQL(s"""
                ALTER TABLE $tableName DROP CONSTRAINT $keyName $cascadeSql
            """).execute.apply()

        def createPrimaryKey(tableName: String, columnName: String): Unit =
            SQL(s"""
                ALTER TABLE $tableName ADD CONSTRAINT $tableName$primaryKeySuffix PRIMARY KEY ($columnName)
            """).execute.apply()

        def addPrimaryKeyColumn(tableName: String, columnName: String, columnType: String): Unit =
            addColumn(tableName, columnName, columnType)
            createPrimaryKey(tableName, columnName)

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
                                if isSameIdType(idColumn.columnType, existingColumns(pkColumn).columnType) then
                                    //ignoring else - ID column with PK of same type already exists - no actions
                                    dropPrimaryKey(pkData)
                                    renameColumn(tableName, pkColumn, getRenamedArchivedColumnName(pkColumn,
                                        existingColumns))
                                    addPrimaryKeyColumn(tableName, idColumn.columnName,
                                        getIdFieldType(idColumn.columnType))
                                    dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName)
                            else
                                dropPrimaryKey(pkData)
                                createPrimaryKey(tableName, idColumn.columnName)
                                dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName)
                        else
                            createPrimaryKey(tableName, idColumn.columnName)
                            dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName)
                    else if pkDataOption.isEmpty then
                        addPrimaryKeyColumn(tableName, idColumn.columnName, getIdFieldType(idColumn.columnType))
                        dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName)
                    else
                        val pkData = pkDataOption.get
                        val pkColumn = pkData.columnNames.head
                        if isSameIdType(idColumn.columnType, existingColumns(pkColumn).columnType) then
                            renameColumn(tableName, pkColumn, idColumn.columnName)
                        else
                            dropPrimaryKey(pkData)
                            addPrimaryKeyColumn(tableName, idColumn.columnName, getIdFieldType(idColumn.columnType))
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
                if isSameType(valueColumnType, existingColumns(valueColumnName).columnType) then
                    //ignoring else - Value column of same type already exists - no actions
                    renameColumn(tableName, valueColumnName, getRenamedArchivedColumnName(valueColumnName,
                        existingColumns))
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
                s"${parentTableRef.columnName} ${getFieldType(parentTableRef.refTableData.idColumnType)}, "
            ).getOrElse("")

            SQL(s"""
                CREATE TABLE $tableName (
                    ${idColumn.columnName} ${getIdFieldType(idColumn.columnType)},
                    $parentSql ${getFieldsSql(fields)}
                    CONSTRAINT $tableName$primaryKeySuffix PRIMARY KEY (${idColumn.columnName})
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


        val existingTableNames = metadataUtils.getTableNames(typesSchemaName)

        val refData =
            typesPersistenceData.flatMap {
                case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                    if !existingTableNames.contains(tableName)
                        then createSingleValueTable(tableName, idColumn, valueColumn.columnName, valueColumn.columnType)
                        else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumn.columnName,
                            valueColumn.columnType)
                    Nil
                case ArrayTypePersistenceDataFinal(items) =>
                    items.view.flatMap {
                        case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                            val (valueColumnName, valueColumnType, refData) = valueColumn match
                                case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType) =>
                                    (valueColumnName, valueColumnType, Nil)
                                case ref: ReferenceValuePersistenceDataFinal =>
                                    (ref.columnName, ref.refTableData.idColumnType,
                                        ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList)
                            if !existingTableNames.contains(tableName)
                                then createSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType)
                                else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumnName,
                                    valueColumnType)
                            refData
                    }
                case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                    if !existingTableNames.contains(tableName)
                        then createObjectValueTable(tableName, idColumn, fields, parent)
                        else checkAndFixExistingObjectValueTable(tableName, idColumn, fields, parent)
                    getObjectRefData(tableName, fields, parent)
            }

        val refDataByTable = refData.groupBy(_.tableName)
        metadataUtils.getForeignKeys(null).foreach (  fk =>
            val notFoundRelation =
                if fk.links.size != 1 then
                    true
                else
                    val fkLink = fk.links.head
                    val refData = refDataByTable.getOrElse(fk.tableName, Nil)
                    !refData.exists(ref =>
                        ref.tableName == fk.tableName &&
                            ref.columnName == fkLink.columnName &&
                            ref.refTableData.tableName == fk.refTableName &&
                            ref.refTableData.idColumn.columnName == fkLink.refColumnName
                    )

            if notFoundRelation then
                dropForeignKey(fk)
        )

        val foreignKeysCacheByTable = mutable.Map[String, mutable.Map[String, ForeignKeyData]]()
        metadataUtils.getForeignKeys(null).foreach(fk =>
            foreignKeysCacheByTable.getOrElseUpdate(fk.tableName, mutable.Map()).put(
                fk.links.map(_.columnName).mkString(","), fk)
        )
        refData.foreach { ref =>
            if foreignKeysCacheByTable.get(ref.tableName).flatMap(_.get(ref.columnName)).isEmpty then
                createForeignKey(ref.tableName, ref.columnName, ref.refTableData.tableName,
                    ref.refTableData.idColumn.columnName)
        }


