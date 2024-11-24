package my.valerii_timakov.sgql.services.repositories.postres

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, EntityType}
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId, EntityValue, ValueTypes}
import my.valerii_timakov.sgql.entity.read_modiriers.{GetFieldsDescriptor, SearchCondition}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, DbTableMigrationException, NotInitializedException}
import my.valerii_timakov.sgql.services.*

import scala.util.{Failure, Success, Try}
import scalikejdbc.*

import scala.collection.mutable

class PostgresCrudRepository(
                                connectionConf: Config,
                                persistenceConf: Config,
                                typeNameMaxLength: Short,
                                fieldMaxLength: Short,
                            ) extends CrudRepository:
    private val dbUtils = PostgresDBInitUtils(persistenceConf, typeNameMaxLength, fieldMaxLength)
    private val metadataUtils = MetadataUtils()
    private var currentTypesToTablesMap: Map[String, String] = Map()
    private var previousTypesToTablesMap: Map[String, String] = Map()
    private var savedTablesIdsMap: Map[String, Long] = Map()
    private var typesDefinitionsProviderContainer: Option[TypesDefinitionProvider] = None

    private def typesDefinitionsProvider: TypesDefinitionProvider =
        if typesDefinitionsProviderContainer.isEmpty then throw NotInitializedException("PostgresCrudRepository", "typesDefinitionsProvider")
        typesDefinitionsProviderContainer.get

    override def create(entityType: EntityType[?, ?], data: ValueTypes): Try[Entity[?, ?]] = ???
//        val persistenceData = typesDefinitionsProvider.getPersistenceData(entityType.name).getOrElse(
//            throw new ConsistencyException(s"Type persistence data not found for ${entityType.name}!"))
//        persistenceData match
//            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
//                val id = DB.autoCommit { implicit session =>
//                    SQL(
//                        s"""INSERT INTO $typesSchemaName.$tableName (
//                            ${esc(idColumn.columnName)},
//                            ${esc(valueColumn.columnName)}
//                        ) VALUES (?, ?)"""
//                    )
//                    .bind(data.id, data.value)
//                    .updateAndReturnGeneratedKey(esc(idColumn.columnName)).apply()
//                }
//                Success(Entity(entityType, id, data))
//        DB.autoCommit { implicit session =>
//            SQL(
//                s"""INSERT INTO $utilsSchemaName.${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)} (
//                        ${trc.getString(TYPE_NAME_COLUMN)},
//                        ${trc.getString(TABLE_NAME_COLUMN)},
//                        ${trc.getString(VERSION_REF_COLUMN)}
//                    ) VALUES (?, ?)"""
//            )
//            .bind(typeName, tableName, versionId)
//            .updateAndReturnGeneratedKey(trc.getString(ID_COLUMN_NAME)).apply()
//        }

    override def update(entityType: EntityType[?, ?], entity: Entity[?, ?]): Try[Option[Unit]] = ???//Try {
//        entity.value match
//            case CustomPrimitiveType(rootValue, definition) =>
//                getEntityPersistendeData(definition) match
//                    case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
//                        mapColColuntResult( DB.autoCommit { implicit session =>
//                            SQL(
//                                s"""UPDATE $typesSchemaName.$tableName
//                                    SET ${esc(valueColumn.columnName)} = ?
//                                    WHERE ${esc(idColumn.columnName)} = ?"""
//                            )
//                                .bind(rootValue.value, entity.id)
//                                .update.apply()
//                        },  s"Multiple entities updated for id: ${entity.id}!")
//                    case persData => throw new ConsistencyException(
//                        s"Entity value is Primitive, but found persistence data is not Primitive! $persData")
//            case ObjectType(filedValuesMap, definition) =>
//                getEntityPersistendeData(definition) match
//                    case ObjectTypePersistenceDataFinal(tableName, idColumn, fieldsPersistenceData, parent) =>
//                        val fieldsData = filedValuesMap.map { case (fieldName, fieldValue) =>
//                            fieldsPersistenceData(fieldName) match
//                                case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
//                                    (columnName, getAsConcreteValue[PrimitiveFieldType[?]](fieldValue, "").value)
//                                case ref: ReferenceValuePersistenceDataFinal =>
//                                    (ref.columnName, getAsConcreteValue[ReferenceFieldType](fieldValue, "").id)
//                                case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
//                                    val parentData = parent.map(parent =>
//                                        (parent.columnName, fieldValue.asInstanceOf[ObjectFieldType].fields(parent.columnName).asInstanceOf[ReferenceFieldType].id)
//                                    ).toList
//                                    val fieldsData = fields.map { case (fieldName, field) =>
//                                        fields(fieldName) match
//                                            case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
//                                                (columnName, field.asInstanceOf[PrimitiveFieldType[?]].value)
//                                            case ref: ReferenceValuePersistenceDataFinal =>
//                                                (ref.columnName, field.asInstanceOf[ReferenceFieldType].id)
//                                            case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
//                                                throw new ConsistencyException("Nested objects are not supported!")
//                                    }
//                                    parentData ++ fieldsData
//                        }
//                        mapColColuntResult( DB.autoCommit { implicit session =>
//                            SQL(
//                                s"""UPDATE $typesSchemaName.$tableName
//                                    SET ${fieldsData.map { case (columnName, _) => s"${esc(columnName)} = ?" }.mkString(", ")}
//                                    WHERE ${esc(idColumn.columnName)} = ?"""
//                            )
//                                .bind(fieldsData.map(_._2) :+ entity.id: _*)
//                                .update.apply()
//                        },  s"Multiple entities updated for id: ${entity.id}!")
//                value
//            case ArrayType(value, definition) => 
//                value
//            case _ => throw new ConsistencyException(s"Entity value is not known! $entity")
//
//        entity.value.typeDefinition
//        val persistenceData = getEntityPersistendeData(entityType)
//        persistenceData match
//            case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
//                val value = entity.value match
//                    case primitive: PrimitiveFieldType[?] => primitive.value
//                    case _ => throw new ConsistencyException("Entity value is not Primitive!")
//                DB.autoCommit { implicit session =>
//                    SQL(
//                        s"""UPDATE $typesSchemaName.$tableName
//                            SET ${esc(valueColumn.columnName)} = ?
//                            WHERE ${esc(idColumn.columnName)} = ?"""
//                    )
//                        .bind(value, entity.id)
//                        .update.apply()
//                }
//                Some(())
//            //TODO: fix id & parent to it | parent in persistenceData
//            case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
//                val fieldsData = entity.value match
//                    case ObjectType(fieldsValuesMap) =>
//                        fieldsValuesMap.map { case (fieldName, field) =>
//                            fields(fieldName) match
//                                case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
//                                    (columnName, field.asInstanceOf[PrimitiveFieldType[?]].value)
//                                case ref: ReferenceValuePersistenceDataFinal =>
//                                    (ref.columnName, field.asInstanceOf[ReferenceFieldType].id)
//                                case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
//                                    val parentData = parent.map(parent =>
//                                        (parent.columnName, field.asInstanceOf[ObjectFieldType].fields(parent.columnName).asInstanceOf[ReferenceFieldType].id)
//                                    ).toList
//                                    val fieldsData = fields.map { case (fieldName, field) =>
//                                        fields(fieldName) match
//                                            case PrimitiveValuePersistenceDataFinal(columnName, columnType) =>
//                                                (columnName, field.asInstanceOf[PrimitiveFieldType[?]].value)
//                                            case ref: ReferenceValuePersistenceDataFinal =>
//                                                (ref.columnName, field.asInstanceOf[ReferenceFieldType].id)
//                                            case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
//                                                throw new ConsistencyException("Nested objects are not supported!")
//                                    }
//                                    parentData ++ fieldsData
//                        }
//                    case _ => throw new ConsistencyException("Entity value is not Object!")
//                DB.autoCommit { implicit session =>
//                    SQL(
//                        s"""UPDATE $typesSchemaName.$tableName
//                            SET ${fieldsData.map { case (columnName, _) => s"${esc(columnName)} = ?" }.mkString(", ")}
//                            WHERE ${esc(idColumn.columnName)} = ?"""
//                    )
//                        .bind(fieldsData.map(_._2) :+ entity.id: _*)
//                        .update.apply()
//                }
//                Some(())
//                
//    }
    
    private def getAsConcreteValue[T <: ValueTypes](entityValue: ValueTypes, errorMessage: String): T =
        entityValue match
            case value: T => value
            case _ => throw new ConsistencyException(errorMessage)

    override def delete(entityType: EntityType[?, ?], id: EntityId[?, ?]): Try[Option[Unit]] = 
        Try {
            val persistenceData = getEntityPersistendeData(entityType)
            val tableData = persistenceData match
                case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                    (tableName, idColumn.columnName)
                case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                    (tableName, idColumn.columnName)
            mapColColuntResult( DB.autoCommit { implicit session =>
                SQL(
                    s"""UPDATE $typesSchemaName.${tableData._1} SET $archivedEntityColumnName = ?
                        WHERE ${esc(tableData._2)} = ?"""
                )
                    .bind(true, id)
                    .update.apply()
            },  s"Multiple entities archived for id: $id!")
        }
        
    private def mapColColuntResult(count: Int, nonUniqueErrorMessage: String): Option[Unit] =
        if count == 0 then None
        else if count == 1 then Some(())
        else throw new ConsistencyException(nonUniqueErrorMessage)


    private def getEntityPersistendeData(entityType: EntityType[?, ?]) = {
        typesDefinitionsProvider.getPersistenceData(entityType.name).getOrElse(
            throw new ConsistencyException(s"Type persistence data not found for ${entityType.name}!"))
    }

    override def get(entityType: EntityType[?, ?], id: EntityId[?, ?], getFields: GetFieldsDescriptor): Try[Option[Entity[?, ?]]] = ???

    override def find(entityType: EntityType[?, ?], query: SearchCondition, getFields: GetFieldsDescriptor): Try[Vector[Entity[?, ?]]] = ???


    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit =

        this.typesDefinitionsProviderContainer = Some(typesDefinitionsProvider)

        initConnectionPoolAndTypesSchema()

        dbUtils.init()

        currentTypesToTablesMap = typesDefinitionsProvider.getTypesToTalesMap

        DB.localTx(implicit session =>
            val lastVersion = dbUtils.getLatestVersion
            lastVersion.foreach(version =>
                previousTypesToTablesMap = dbUtils.getTypesToTablesMap(version.id)
            )
            val version = dbUtils.addVersion("Version." + lastVersion.map(_.id + 1).getOrElse(1L))
            savedTablesIdsMap = saveTypesToTablesMap(currentTypesToTablesMap, version)
            renameChangedTables(version)
            createOrMigrateTables(typesDefinitionsProvider.getAllPersistenceData, version)
        )

    private def initConnectionPoolAndTypesSchema() =

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

    private def saveTypesToTablesMap(typeToTableMap: Map[String, String], version: Version): Map[String, Long] =
        typeToTableMap.map { case (typeName, tableName) =>
            tableName -> dbUtils.addTypeToTableMapEntry(typeName, tableName, version.id)
        }

    private def renameChangedTables(version: Version)(implicit session: DBSession): Unit =

        def renameTable(currTableName: String, newTableName: String): Unit =
            SQL(s"""
                ALTER TABLE ${esc(currTableName)} RENAME TO ${esc(newTableName)}
            """).execute.apply()

        val existingTableNames = metadataUtils.getTableNames(typesSchemaName)

        currentTypesToTablesMap.foreach { case (typeName, currTableName) =>
            previousTypesToTablesMap.get(typeName).foreach(prevTableName =>
                if (prevTableName != currTableName && existingTableNames.contains(prevTableName))
                    renameTable(prevTableName, currTableName)
                    dbUtils.addTableRenamingData(prevTableName, currTableName, version.id)
            )
        }

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

    private final val DB_KEYWORDS_SET = Set("ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC",
        "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE",
        "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE",
        "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FOR", "FOREIGN", "FREEZE", "FROM", "FULL", "GRANT",
        "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "LEADING",
        "LEFT", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NEW", "NOT", "NOTNULL", "NULL", "OFF", "OFFSET",
        "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT",
        "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION",
        "UNIQUE", "USER", "USING", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH", "BIGINT", "BIT", "BOOLEAN", "CHAR",
        "CHARACTER", "DOUBLE", "FLOAT", "INT", "INTEGER", "INTERVAL", "NATIONAL", "NUMERIC", "REAL", "SERIAL", "SMALLINT",
        "TEXT", "TIMESTAMP", "UUID", "VARCHAR", "VARYING", "ADD", "ALTER", "DROP", "EXISTS", "EXPLAIN", "INDEX", "INSERT",
        "INHERITS", "LANGUAGE", "LISTEN", "LOAD", "LOCK", "MOVE", "PREPARE", "REASSIGN", "REINDEX", "RELEASE", "RESET",
        "REVOKE", "ROLLBACK", "SAVEPOINT", "SECURITY", "SEQUENCE", "SET", "SHOW", "TABLESPACE", "TRUNCATE", "UNLISTEN",
        "UPDATE", "VACUUM", "VALUES", "VIEW")

    private def esc(name: String): String =
        if DB_KEYWORDS_SET.contains(name.toUpperCase) then s""""$name""""
        else name

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
    private val archivedEntityColumnName = persistenceConf.getString("archived-entity-column")

    private var typesPersistenceData: Map[AbstractEntityType, TypePersistenceData] = Map()

    private class RefData(
        val tableName: String,
        val columnName: String,
        val refTableData: TableReferenceData
    )


    private def createOrMigrateTables(
        typesPersistenceData: Seq[TypePersistenceDataFinal],
        version: Version,
    )(implicit session: DBSession): Unit =

        def foreignKeyName(tableName: String, columnName: String, refTableName: String): String =
            s"fk_${tableName}_${columnName}_$refTableName"


        def getObjectRefData(
            tableName: String,
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
                case PrimitiveValuePersistenceDataFinal(columnName, columnType, isNullable) => List()
                case ref: ReferenceValuePersistenceDataFinal =>
                    ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList
                case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                    getObjectRefData(tableName, fields, parent)
            }.toList

        def createForeignKey(tableName: String, columnName: String, refTableName: String, refColumnName: String): Unit =
            SQL(s"""
                ALTER TABLE ${esc(tableName)} ADD CONSTRAINT ${esc(foreignKeyName(tableName, columnName, refTableName))}
                    FOREIGN KEY (${esc(columnName)}) REFERENCES ${esc(refTableName)}(${esc(refColumnName)})
            """).execute.apply()

        def createSingleValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
            isArray: Boolean,
        ): Unit =
            val pkSql = if isArray then "" else s",\n CONSTRAINT ${esc(tableName + primaryKeySuffix)} PRIMARY KEY (${esc(idColumn.columnName)})"
            val idType = getIdFieldType(idColumn.columnType)
            val valueType = getFieldType(valueColumnType)
            SQL(s"""
                CREATE TABLE ${esc(tableName)} (
                    ${esc(idColumn.columnName)} $idType NOT NULL,
                    ${esc(valueColumnName)} $valueType NOT NULL,
                    $archivedEntityColumnName ${getIdFieldType(BooleanFieldType)} NOT NULL DEFAULT FALSE
                    $pkSql
                )
            """).execute.apply()
            savedTablesIdsMap(tableName)
            val tableId = savedTablesIdsMap(tableName)
            dbUtils.addTableColumn(tableId, idColumn.columnName, idType, "id")
            dbUtils.addTableColumn(tableId, valueColumnName, valueType, "value")


        def renameColumn(tableName: String, prevColumnName: String, newColumnName: String): Unit =
            SQL(s"""
                ALTER TABLE ${esc(tableName)} RENAME COLUMN ${esc(prevColumnName)} TO ${esc(newColumnName)}
            """).execute.apply()
            dbUtils.addTableRenamingData(tableName, prevColumnName, newColumnName, version.id)

        def dropPrimaryKey(pk: PrimaryKeyData): Unit =
            dropConstraint(pk.tableName, pk.keyName, true)

        def dropForeignKey(fk: ForeignKeyData): Unit =
            dropConstraint(fk.tableName, fk.keyName, false)

        def dropConstraint(tableName: String, keyName: String, cascade: Boolean): Unit =
            val cascadeSql = if cascade then " CASCADE" else ""
            SQL(s"""
                ALTER TABLE ${esc(tableName)} DROP CONSTRAINT ${esc(keyName)} $cascadeSql
            """).execute.apply()

        def createPrimaryKey(tableName: String, columnName: String): Unit =
            SQL(s"""
                ALTER TABLE ${esc(tableName)} ADD CONSTRAINT ${esc(tableName + primaryKeySuffix)} PRIMARY KEY (${esc(columnName)})
            """).execute.apply()

        def addPrimaryKeyColumn(tableName: String, columnName: String, columnType: String): Unit =
            addColumn(tableName, columnName, columnType, false)
            createPrimaryKey(tableName, columnName)

        def addColumn(tableName: String, columnName: String, columnType: String, isNullable: Boolean, default: String = ""): Unit =
            val defaultSql = if default.isEmpty then "" else s" DEFAULT $default"
            SQL(s"""
                ALTER TABLE ${esc(tableName)} ADD COLUMN ${esc(columnName)} $columnType ${if isNullable then " " else "NOT NULL"}$defaultSql
            """).execute.apply()

        def createObjectValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            fields: Map[String, ValuePersistenceDataFinal],
        ): Unit =
            val fieldsSqlData = getFieldsColumsData(fields, None)
            val fieldsSql = fieldsSqlData.map { case (columnName, columnType, fieldName, isNullable) =>
                s"${esc(columnName)} $columnType ${if isNullable then "" else "NOT NULL"}"
            }.mkString(", ")

            SQL(s"""
                CREATE TABLE ${esc(tableName)} (
                    ${esc(idColumn.columnName)} ${getIdFieldType(idColumn.columnType)} NOT NULL,
                    $fieldsSql,
                    $archivedEntityColumnName ${getIdFieldType(BooleanFieldType)} NOT NULL DEFAULT FALSE,
                    CONSTRAINT ${esc(tableName + primaryKeySuffix)} PRIMARY KEY (${esc(idColumn.columnName)})
                )
            """).execute.apply()
            val tableId = savedTablesIdsMap(tableName)
            dbUtils.addTableColumn(tableId, idColumn.columnName, getIdFieldType(idColumn.columnType), "id")
            fieldsSqlData.foreach { case (columnName, columnType, fieldName, _) =>
                dbUtils.addTableColumn(tableId, columnName, columnType, fieldName)
            }
            
        def getRenamedArchivedColumnName(columnName: String, existingColumns: Map[String, ColumnData]) =
            val result = columnName + archivedColumnNameSuffix
            var i = 1
            while existingColumns.contains(result + i) do i += 1
            result + i
            
        def checkAndFixExistingTableIdColumn(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            existingColumns: Map[String, ColumnData],
            isArray: Boolean,
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
                                    if (!isArray)
                                        addPrimaryKeyColumn(tableName, idColumn.columnName, getIdFieldType(idColumn.columnType))
                                        dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName, version.id)
                            else
                                dropPrimaryKey(pkData)
                                if !isArray then
                                    createPrimaryKey(tableName, idColumn.columnName)
                                    dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName, version.id)
                        else if !isArray then
                            createPrimaryKey(tableName, idColumn.columnName)
                            dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName, version.id)
                    else if pkDataOption.isEmpty then
                        if !isArray then
                            addPrimaryKeyColumn(tableName, idColumn.columnName, getIdFieldType(idColumn.columnType))
                            dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName, version.id)
                    else
                        val pkData = pkDataOption.get
                        val pkColumn = pkData.columnNames.head
                        if isSameIdType(idColumn.columnType, existingColumns(pkColumn).columnType) then
                            renameColumn(tableName, pkColumn, idColumn.columnName)
                        else
                            dropPrimaryKey(pkData)
                            if !isArray then
                                addPrimaryKeyColumn(tableName, idColumn.columnName, getIdFieldType(idColumn.columnType))
                                dbUtils.addPrimaryKeyAlteringData(tableName, pkColumn, idColumn.columnName, version.id)
                    dbUtils.addTableColumn(savedTablesIdsMap(tableName), idColumn.columnName, getIdFieldType(idColumn.columnType), "id")
                catch
                    case e: Exception => throw DbTableMigrationException(tableName, e)
            else
                throw DbTableMigrationException(tableName)
                
        def checkAndFixExistingTableValueColumn(
                                                   tableName: String,
                                                   valueColumnName: String,
                                                   valueColumnType: PersistenceFieldType,
                                                   isNullable: Boolean,
                                                   existingColumns: Map[String, ColumnData],
                                                   fieldName: Option[String],
        ): Unit =
            if existingColumns.contains(valueColumnName) then
                if isSameType(valueColumnType, existingColumns(valueColumnName).columnType) then
                    //ignoring else - Value column of same type already exists - no actions
                    renameColumn(tableName, valueColumnName, getRenamedArchivedColumnName(valueColumnName,
                        existingColumns))
                    addColumn(tableName, valueColumnName, getFieldType(valueColumnType), isNullable)
                    dbUtils.addTableRenamingData(tableName, null, valueColumnName, version.id)
            else
                addColumn(tableName, valueColumnName, getFieldType(valueColumnType), isNullable)
                dbUtils.addTableRenamingData(tableName, null, valueColumnName, version.id)
            fieldName.foreach(fieldName =>
                dbUtils.addTableColumn(savedTablesIdsMap(tableName), valueColumnName, getIdFieldType(valueColumnType), fieldName)
            )

        def checkAndFixExistingSingleValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
            isArray: Boolean,
        ): Unit =
            val existingColumns: Map[String, ColumnData] =  metadataUtils.getTableColumnsDataMap(tableName)
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns, isArray)
            checkAndFixExistingTableValueColumn(tableName, valueColumnName, valueColumnType, false, 
                existingColumns, Some("value"))
            checkAndFixExistingTableValueColumn(tableName, archivedEntityColumnName, BooleanFieldType, false, 
                existingColumns, None)


        def checkAndFixExistingSimpleObjectValueTable(
            tableName: String,
            fields: Map[String, ValuePersistenceDataFinal],
            parentIndirect: Option[ReferenceValuePersistenceDataFinal],
            existingColumns: Map[String, ColumnData],
            fieldsPrefixOpt: Option[String],
        ): Unit =
            val fieldsPrefix = fieldsPrefixOpt.getOrElse("")
            parentIndirect.foreach(parentTableRef =>
                checkAndFixExistingTableValueColumn(tableName, parentTableRef.columnName, 
                    parentTableRef.refTableData.idColumnType, false, existingColumns, Some(fieldsPrefix + "parent"))
            )
            fields.foreach((fieldName, fieldData) =>
                fieldData match
                    case PrimitiveValuePersistenceDataFinal(columnName, columnType, isNullable) =>
                        checkAndFixExistingTableValueColumn(tableName, columnName, columnType, isNullable, 
                            existingColumns, Some(fieldsPrefix + fieldName))
                    case ref: ReferenceValuePersistenceDataFinal =>
                        checkAndFixExistingTableValueColumn(tableName, ref.columnName, ref.refTableData.idColumnType,
                            ref.isNullable, existingColumns, Some(fieldsPrefix + fieldName))
                    case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                        checkAndFixExistingSimpleObjectValueTable(tableName, fields, parent, existingColumns,
                            Some(fieldsPrefix + fieldName + "."))
            )
            if fields.isEmpty then
                checkAndFixExistingTableValueColumn(tableName, archivedEntityColumnName, BooleanFieldType, false, 
                    existingColumns, None)

        def checkAndFixExistingObjectValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            fields: Map[String, ValuePersistenceDataFinal],
            existingColumnsOption: Option[Map[String, ColumnData]] = None, 
        ): Unit =
            val existingColumns: Map[String, ColumnData] = existingColumnsOption.getOrElse( 
                metadataUtils.getTableColumnsDataMap(tableName) )
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns, false)
            checkAndFixExistingSimpleObjectValueTable(tableName, fields, None, existingColumns, None)

        def getFieldsColumsData(
            fields: Map[String, ValuePersistenceDataFinal], 
            prefixField: Option[String], 
        ): List[(String, String, String, Boolean)] =
            val fieldsPrefix = prefixField.map(_ + ".").getOrElse("")
            fields.view
                .toList
                .flatMap { (fieldName, fieldData) => fieldData match
                    case PrimitiveValuePersistenceDataFinal(columnName, columnType, isNullable) =>
                        List((columnName, getFieldType(columnType), fieldsPrefix + fieldName, isNullable))
                    case ref: ReferenceValuePersistenceDataFinal =>
                        List((ref.columnName, getFieldType(ref.refTableData.idColumnType), 
                            fieldsPrefix + fieldName, ref.isNullable))
                    case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                        val parentSqlData = parent.map { parentTableRef =>
                            (parentTableRef.columnName, getFieldType(parentTableRef.refTableData.idColumnType), 
                                fieldsPrefix + fieldName, false)
                        }.toList
                        getFieldsColumsData(fields, Some(fieldsPrefix + fieldName)) ++ parentSqlData
                }


        val existingTableNames = metadataUtils.getTableNames(typesSchemaName)

        val refData =
            typesPersistenceData.flatMap {
                case PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                    if !existingTableNames.contains(tableName)
                        then createSingleValueTable(tableName, idColumn, valueColumn.columnName, valueColumn.columnType, false)
                        else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumn.columnName,
                            valueColumn.columnType, false)
                    Nil
                case ArrayTypePersistenceDataFinal(items, _) =>
                    items.view.flatMap {
                        case ItemTypePersistenceDataFinal(tableName, idColumn, valueColumn) =>
                            val (valueColumnName, valueColumnType, refData) = valueColumn match
                                case PrimitiveValuePersistenceDataFinal(valueColumnName, valueColumnType, _) =>
                                    (valueColumnName, valueColumnType, Nil)
                                case ref: ReferenceValuePersistenceDataFinal =>
                                    (ref.columnName, ref.refTableData.idColumnType,
                                        ref.refTableData.data.map(RefData(tableName, ref.columnName, _)).toList)
                            if !existingTableNames.contains(tableName)
                                then createSingleValueTable(tableName, idColumn, valueColumnName, valueColumnType, true)
                                else checkAndFixExistingSingleValueTable(tableName, idColumn, valueColumnName,
                                    valueColumnType, true)
                            refData
                    }
                case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, parent) =>
                    if !existingTableNames.contains(tableName)
                        then createObjectValueTable(tableName, idColumn, fields)
                        else checkAndFixExistingObjectValueTable(tableName, idColumn, fields)
                    getObjectRefData(tableName, fields, parent.map(refTable => ReferenceValuePersistenceDataFinal(
                        idColumn.columnName, refTable.refTableWrapperCopy, false)))
                    
            }

        val refDataByTable = refData.groupBy(_.tableName)
        metadataUtils.getForeignKeys(typesSchemaName, null).foreach (  fk =>
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
        metadataUtils.getForeignKeys(typesSchemaName, null).foreach(fk =>
            foreignKeysCacheByTable.getOrElseUpdate(fk.tableName, mutable.Map()).put(
                fk.links.map(_.columnName).mkString(","), fk)
        )
        refData.foreach { ref =>
            if foreignKeysCacheByTable.get(ref.tableName).flatMap(_.get(ref.columnName)).isEmpty then
                createForeignKey(ref.tableName, ref.columnName, ref.refTableData.tableName,
                    ref.refTableData.idColumn.columnName)
        }


