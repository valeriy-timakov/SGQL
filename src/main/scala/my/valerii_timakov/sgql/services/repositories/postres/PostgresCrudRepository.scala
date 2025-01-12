package my.valerii_timakov.sgql.services.repositories.postres

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.type_definitions.{EntityIdTypeDefinition, FieldTypeDefinition, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, AbstractObjectEntityType, ArrayEntityType, CustomPrimitiveEntityType, EntitySuperType, EntityType, ObjectEntitySuperType, ObjectEntityType, ReferenceType, RootPrimitiveType}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, ByteId, CustomPrimitiveValue, Entity, EntityId, EntityValue, FilledEntityId, FixedStringId, IntId, ItemValue, LongId, ObjectValue, ReferenceValue, RootPrimitiveValue, ShortIntId, SimpleObjectValue, StringId, UUIDId, ValueTypes}
import my.valerii_timakov.sgql.entity.read_modiriers.{AbstractObjectGetFieldsDescriptor, AllGetFieldsDescriptor, AllInReferenceGetFieldsDescriptor, GetFieldsDescriptor, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, ObjectGetFieldsDescriptor, SearchCondition, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, DbTableMigrationException, NotInitializedException}
import my.valerii_timakov.sgql.services.{ItemTypePersistenceDataFinal, ReferenceValuePersistenceDataFinal, ValuePersistenceDataFinal, *}

import scala.util.{Failure, Success, Try}
import scalikejdbc.*

import java.sql.ResultSet
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable

implicit val uuidTypeBinder: TypeBinder[UUID] = TypeBinder[UUID](
    (rs: ResultSet, colIdx: Int) => UUID.fromString(rs.getString(colIdx))
)(
    (rs: ResultSet, colName: String) => UUID.fromString(rs.getString(colName))
)


private case class RefererTableData(
                                       tableName: String,
                                       columnName: String,
                                       prevReferef: Option[RefererTableData]
                                   )

private case class RefererTablePartialData(
                                              tableName: String,
                                              prevReferef: Option[RefererTableData]
                                          ):
    def toRefererTableData(columnName: String): RefererTableData =
        RefererTableData(tableName, columnName, prevReferef)


class PostgresCrudRepository(
                                connectionConf: Config,
                                persistenceConf: Config,
                                typesMapper: TypesToPersistenceMapper, 
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


    override def create(entityType: EntityType[_, _, _], data: ValueTypes): Try[EntityId[_, _]] =
        DB.autoCommit { implicit session =>
            create(entityType, data)(session)
        }

    override def update(entity: Entity[_, _, _]): Try[Option[Unit]] =
        DB.autoCommit { implicit session =>
            update(entity)(session)
        }

    override def delete(entityType: EntityType[_, _, _], id: EntityId[_, _]): Try[Option[Unit]] =
        DB.autoCommit { implicit session =>
            delete(entityType, id)(session)
        }

    override def get(entityType: EntityType[_, _, _], id: EntityId[_, _], getFields: ObjectGetFieldsDescriptor): Try[Option[Entity[_, _, _]]] =
        DB.readOnly { implicit session =>
            get(entityType, id, getFields)(session)
        }

    override def find(entityType: EntityType[_, _, _], query: SearchCondition, getFields: ObjectGetFieldsDescriptor): Try[Vector[Entity[_, _, _]]] =
        DB.readOnly { implicit session =>
            find(entityType, query, getFields)(session)
        }

    def create(entityType: EntityType[_, _, _], data: ValueTypes)(implicit session: DBSession): Try[EntityId[_, _]] =
        
        def insertPrimitiveValue(
            tableName: String, 
            idColumn:  PrimitiveValuePersistenceDataFinal, 
            valueColumnName: String,
            value: Any
        ): EntityId[_, _] =
            SQL(s"""
                INSERT INTO $typesSchemaName.$tableName (${esc(valueColumnName)}) VALUES (?) 
                RETURNING ${esc(idColumn.columnName)}
            """)
                .bind(value)
                .map(getIdValueMapper(idColumn))
                .single
                .apply()
                .getOrElse(throw new ConsistencyException("Id is not returned!"))
            
        def insertObjectValue(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            filedValuesMap: Map[String, EntityValue], 
            fieldsPersistenceData: Map[String, ValuePersistenceDataFinal],
            parent: Option[ObjectEntitySuperType[_, _]],
            typeName: String,
        ): EntityId[_, _] =
            getColumnsValues(filedValuesMap.toList, fieldsPersistenceData, parent, tableName, idColumn, typeName, 0)
                .sortBy(_._4)(Ordering[Byte].reverse)
                .foldRight(None)((tableData: (String, PrimitiveValuePersistenceDataFinal, List[(String, Any)], Byte), idOpt: Option[EntityId[_, _]]) =>
                    val id = (idOpt, tableData) match
                        case (None, (tableName, idColumn, columnsNameValues, _)) =>
                            val (columnNames, columnValues) = columnsNameValues.unzip
                            insertObjectPartValueAndGenerateId(tableName, columnNames, columnValues, idColumn)
                        case(Some(id), (tableName, idColumn, columnsNameValues, _)) =>
                            val (columnNames, columnValues) = columnsNameValues.unzip
                            insertObjectPartValueWithId(tableName, columnNames :+ idColumn.columnName,
                                columnValues :+ id.value, idColumn)
                            id
                    Some(id)
                )
                .getOrElse(throw new ConsistencyException("Id is not returned!"))

        def insertArrayValues(typeName: String, values: Seq[ItemValue], persData: ArrayTypePersistenceDataFinal): EntityId[_, _] =
            val id = getNextId(typeName, persData.idType)
            splitValuesByTypes(values, persData).foreach { case (persData, items) =>
                    val valuesLine = items.zipWithIndex.map { case (v, i) => s"VALUES ( :id, :v$i )" }.mkString(", ")
                    val params = items.zipWithIndex.map { case (v, i) => s"v$i" -> v }
                    val res = SQL(
                        s"""INSERT INTO $typesSchemaName.${persData.tableName}
                                ( ${esc(persData.idColumn.columnName)}, ${esc(persData.valueColumn.columnName)} )
                                $valuesLine"""
                    )
                        .bindByName(params :+ "id" -> id.value: _*)
                        .update.apply()
                    mapColColuntResult(res, s"Multiple entities updated for id: $id!", items.size)
                }
            id
            
        def insertObjectPartValueAndGenerateId(
            tableName: String, 
            columnNames: List[String], 
            columnValues: List[Any], 
            idColumn: PrimitiveValuePersistenceDataFinal
        ): EntityId[_, _] =
            SQL(s"""
                INSERT INTO $typesSchemaName.$tableName
                (${columnNames.map(colName => s"${esc(colName)}" ).mkString(", ")})
                VALUES (${columnNames.map(_ => '?' ).mkString(", ")})
                RETURNING ${esc(idColumn.columnName)}
            """)
                .bind(columnValues: _*)
                .map(getIdValueMapper(idColumn))
                .single
                .apply()
                .getOrElse(throw new ConsistencyException("Id is not returned!"))
            
        def insertObjectPartValueWithId(
            tableName: String,
            columnNamesExt: List[String],
            columnValuesExt: List[Any],
            idColumn: PrimitiveValuePersistenceDataFinal
        ): Unit =
            val res = SQL(s"""
                INSERT INTO $typesSchemaName.$tableName
                (${columnNamesExt.map(colName => s"${esc(colName)}" ).mkString(", ")})
                VALUES (${columnNamesExt.map(_ => '?' ).mkString(", ")})
            """)
                .bind(columnValuesExt: _*)
                .update
                .apply()
            mapColColuntResult(res, s"Multiple entities inserted for $columnValuesExt!", 1)

        def getNextId(typeName: String, idType: PersistenceFieldType): EntityId[_, _] =
            val generateNextNumericValueFunc = s"nextval('${getSequenceName(typeName)}')"
            val generateNextValueFunc = idType match
                case LongFieldType => () => generateNextNumericValueFunc
                case IntFieldType => () => generateNextNumericValueFunc
                case ShortIntFieldType => () => generateNextNumericValueFunc
                case ByteFieldType => () => generateNextNumericValueFunc
                case UUIDFieldType => () => generateRandomUUIDFunc
                case StringFieldType(_) => s"$stringIdAutoGenerationFunction"
                case FixedStringFieldType(len) => s"LPAD($stringIdAutoGenerationFunction, $len, '0')"
                case _ => throw new ConsistencyException(s"Id type $idType is not supported!")
            val sequenceName = getSequenceName(typeName)
            SQL(s"SELECT nextval('$typesSchemaName.$sequenceName')")
                .map(getIdValueMapper(idType, 1))
                .single
                .apply()
                .getOrElse(throw new ConsistencyException(s"Id od sequence for type $typeName is not returned!"))

        Try {
            (data, entityType, entityType.persistenceData ) match
                case (
                    value: RootPrimitiveValue[_],
                    CustomPrimitiveEntityType(_, _),
                    PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn)
                ) =>
                    insertPrimitiveValue(tableName, idColumn, valueColumn.columnName, value.value)
                case (
                    filedValuesMap:  Map[String, EntityValue],
                    ObjectEntityType(_, valueType),
                    ObjectTypePersistenceDataFinal(tableName, idColumn, fieldsPersistenceData, parentPersistenceData)
                ) =>
                    insertObjectValue(tableName, idColumn, filedValuesMap, fieldsPersistenceData, valueType.parent, entityType.name)
                case (
                    values: Seq[ItemValue],
                    ArrayEntityType(typeName, _),
                    persData: ArrayTypePersistenceDataFinal
                ) =>
                    insertArrayValues(typeName, values, persData)
                case _ => throw new ConsistencyException(s"Entity value $data is of not known type $entityType, or " +
                    s"persistence data  ${entityType.persistenceData} not compatible!")
        }

    def update(entity: Entity[_, _, _])(implicit session: DBSession): Try[Option[Unit]] =

        def updatePrimitiveValue(
                                    tableName: String,
                                    idColumn: PrimitiveValuePersistenceDataFinal,
                                    valueColumn: PrimitiveValuePersistenceDataFinal,
                                    id: EntityId[_, _],
                                    value: RootPrimitiveValue[_]
                                ): Option[Unit]=
            val res =
                SQL(s"""
                UPDATE $typesSchemaName.$tableName
                SET ${esc(valueColumn.columnName)} = ?
                WHERE ${esc(idColumn.columnName)} = ?
            """)
                .bind(value.value, id.value)
                .update.apply()
            mapColColuntResult(res, s"Multiple entities updated for id: ${entity.id}!", 1)

        def updateObjectValue(
                                tableName: String,
                                idColumn: PrimitiveValuePersistenceDataFinal,
                                entityType: ObjectEntityType[_, _],
                                fieldsPersistenceData: Map[String, ValuePersistenceDataFinal],
                                filedValuesMap: Map[String, EntityValue]
                            ): Option[Unit]=
            getColumnsValues(filedValuesMap.toList, fieldsPersistenceData, entityType.valueType.parent,
                tableName, idColumn, entityType.name, 0)
                .map { case (tableName, idColumn, columnsValues, _) =>
                    val res = SQL(s"""
                                        UPDATE $typesSchemaName.$tableName
                                        SET ${columnsValues.map { case (columnName, _) => s"${esc(columnName)} = ?" }.mkString(", ")}
                                        WHERE ${esc(idColumn.columnName)} = ?
                                    """)
                        .bind(columnsValues.map(_._2) :+ entity.id.value: _*)
                        .update.apply()
                    mapColColuntResult(res,  s"Multiple entities updated for id: ${entity.id}!", 1)
                }
                .fold(Some(()))( (acc, res) => if acc.isDefined then res else None )

        def updateArrayValues(
                                 persData: ArrayTypePersistenceDataFinal,
                                 id: EntityId[_, _],
                                 values: Seq[ItemValue]
                             ): Option[Unit]=
            val tmpData = splitValuesByTypes(values, persData)
            tmpData.map { case (persData, items) =>
                    SQL(s"DELETE FROM $typesSchemaName.${persData.tableName} WHERE ${esc(persData.idColumn.columnName)} = ?")
                        .bind(id.value)
                        .update.apply()
                    val valuesLine = items.zipWithIndex.map{ case (v, i) => s"VALUES ( :id, :v$i )"}.mkString(", ")
                    val params = items.zipWithIndex.map { case (v, i) => s"v$i" -> v }
                    val res = SQL(
                        s"""INSERT INTO $typesSchemaName.${persData.tableName}
                                    ( ${esc(persData.idColumn.columnName)}, ${esc(persData.valueColumn.columnName)} )
                                    $valuesLine"""
                    )
                        .bindByName(params :+ "id" -> id.value: _*)
                        .update.apply()
                    mapColColuntResult(res,  s"Multiple entities updated for id: ${entity.id}!", items.size)
                }
                .fold(Some(()))( (acc, res) => if acc.isDefined then res else None )

        Try {
            val persistenceData = entity.typeDefinition.persistenceData
            (entity, persistenceData) match
                case (
                    CustomPrimitiveValue(id, value, _),
                    PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn)
                ) =>
                    updatePrimitiveValue(tableName, idColumn, valueColumn, id, value)
                case (
                    ObjectValue(_, filedValuesMap, entityType),
                    ObjectTypePersistenceDataFinal(tableName, idColumn, fieldsPersistenceData, _)
                ) =>
                    updateObjectValue(tableName, idColumn, entityType, fieldsPersistenceData, filedValuesMap)
                case (
                    ArrayValue(id, values, _),
                    persData: ArrayTypePersistenceDataFinal
                ) =>
                    updateArrayValues(persData, id, values)
                case _ => throw new ConsistencyException(s"Entity value is of not known type, or persistence data not " +
                    s"compatible! Entity: $entity. Persistence data: $persistenceData")
        }

    def delete(entityType: EntityType[_, _, _], id: EntityId[_, _])(implicit session: DBSession): Try[Option[Unit]] =
        def deleteArrayValues(tablesData: Set[ItemTypePersistenceDataFinal]): Option[Unit] =
            val res = tablesData.map(tableData =>
                SQL(s"""DELETE FROM $typesSchemaName.${tableData.tableName} WHERE ${esc(tableData.idColumn.columnName)} = ?""")
                    .bind(true, id)
                    .update.apply()
            ).sum
            if (res == 0)
                None
            else
                Some(())
        def deleteSingleValue(tableName: String, idColumnName: String): Option[Unit] =
            val res = SQL(s"""DELETE FROM $typesSchemaName.$tableName WHERE ${esc(idColumnName)} = ?""")
                .bind(true, id)
                .update.apply()
            mapColColuntResult(res, s"Multiple entities deleted for id: $id!", 1)
        Try {
            (entityType, entityType.persistenceData) match
                case (_, PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn)) =>
                    deleteSingleValue(tableName, idColumn.columnName)
                case (objectType: ObjectEntityType[_, _], persistanceData: ObjectTypePersistenceDataFinal) =>
                    getAllObjectTables(objectType, persistanceData)
                        .map((tableName, idColumnName) => deleteSingleValue(tableName, idColumnName))
                        .fold(Some(()))( (acc, res) => if acc.isDefined then res else None )
                case (arrayType: ArrayEntityType[_, _], ArrayTypePersistenceDataFinal(items, _, _)) =>
                    deleteArrayValues(items)
        }

    def get(entityType: EntityType[_, _, _], id: EntityId[_, _], getFields: ObjectGetFieldsDescriptor)(implicit session: DBSession): Try[Option[Entity[_, _, _]]] =
//        def getItemExtractor(
//                                pos: Int,
//                                itemType: RootPrimitiveTypeDefinition[_, _],
//                            ): WrappedResultSet => RootPrimitiveValue[_, _] =
//            (rs: WrappedResultSet) => itemType.extract(rs, pos)
        def getAllFieldsGetDescriptor(objecDef: FieldsContainer): List[NestedGetFieldsDescriptor] =
           objecDef.allFields.map { case (fieldName, fieldType) =>
                fieldType.valueType match
                    case definition: SimpleObjectTypeDefinition[_] =>
                        SubObjectGetFieldsDescriptor(fieldName, Right(getAllFieldsGetDescriptor(definition)))
                    case definition: RootPrimitiveTypeDefinition[_] =>
                        SingleGetFieldsDescriptor(fieldName)
                    case definition: TypeReferenceDefinition[_] =>
                        AllInReferenceGetFieldsDescriptor(fieldName)
                    case definition: TypeBackReferenceDefinition[_] =>
                        AllInReferenceGetFieldsDescriptor(fieldName)
                    case _ =>
                        throw new ConsistencyException(s"Field $fieldName type $fieldType is not supported!")
            }.toList
        Try {
            val res = entityType match
                case primType: CustomPrimitiveEntityType[_, _, _] =>
                    val persData = primType.persistenceData
                    val valueOpt =
                        SQL(s"""
                           SELECT ${esc(persData.valueColumn.columnName)}
                           FROM $typesSchemaName.${persData.tableName}
                           WHERE ${esc(persData.idColumn.columnName)} = ?
                        """)
                            .bind(id.value)
                            .map(rs => primType.valueType.rootType.extract(rs, 1))
                            .single
                            .apply()
                            .flatten
                    //valueOpt.map(value => primType.createEntityRaw(id, value))
                case objectType: ObjectEntityType[_, _] =>
                    val fieldsInDescriptor = getFields match
                        case ObjectGetFieldsDescriptor(Left(AllGetFieldsDescriptor)) => getAllFieldsGetDescriptor(objectType.valueType)
                        case ObjectGetFieldsDescriptor(Right(fields)) => fields
                    var rowIdx = 0
                    val fieldsIdxsMap = mutable.HashMap[String, Int]()
//                    val tablesData: List[TableGetDescriptor] = getAllObjectTablesGetDescriptors(objectType, fieldsInDescriptor, None, None).map(tableGetDescriptor =>
//                        rowIdx += 1
//                        val rowAlias = tableAliasInQueryPrefix + rowIdx
//                        val getValuesLine = row.fields.map {
//                                case (dsc: SingleGetFieldsDescriptor, PrimitiveValuePersistenceDataFinal(columnName, _, isNullable)) =>
//                                    s"$rowAlias.$columnName"
//                                case (dsc: ListGetFieldsDescriptor, fieldPersistenceData: ReferenceValuePersistenceDataFinal) =>
//                                    s"$rowAlias.${fieldPersistenceData.columnName}"
//                                case (dsc: SubObjectGetFieldsDescriptor, fieldPersistenceData: ReferenceValuePersistenceDataFinal) =>
//                                    s"$rowAlias.${fieldPersistenceData.columnName}"
//                                case (AllGetFieldsDescriptor, fieldPersistenceData: ReferenceValuePersistenceDataFinal) =>
//                                    s"$rowAlias.${fieldPersistenceData.columnName}"
//                                case (dsc: ListGetFieldsDescriptor, SimpleObjectValuePersistenceDataFinal(parent, fields)) =>
//                                case (dsc: SubObjectGetFieldsDescriptor, SimpleObjectValuePersistenceDataFinal(parent, fields)) =>
//                                case (AllGetFieldsDescriptor, SimpleObjectValuePersistenceDataFinal(parent, fields)) =>
//                                case (dsc, persistanceData) => throw new ConsistencyException(s"Unknown combination of field descriptor $dsc " +
//                                    s"and field persistence data $persistanceData is not supported!")
//                        }
//                        (s"${row.tableName} as $rowAlias", s"$rowAlias.${row.idColumn}", row._3.map(fieldData => s"$rowAlias.${fieldData.columnName}"))
//                    )
//                    val fieldValues =
//                        SQL(s"""
//                                           SELECT ${esc(valueColumn.columnName)}
//                                           FROM $typesSchemaName.$tableName
//                                           WHERE ${esc(idColumn.columnName)} = ?
//                                        """)
//                            .bind(id.value)
//                            .map(rs => typeDef.rootType.extract(rs, 1))
//                            .single
//                            .apply()
//                            .flatten
                    //filedValuesMap:  Map[String, EntityValue],
                case ArrayEntityType(typeName, _) =>
                    //values: Seq[ItemValue],
                    //           persData: ArrayTypePersistenceDataFinal
                case _ => throw new ConsistencyException(s"Entity type $entityType is not known!")
            None
        }

    def find(entityType: EntityType[_, _, _], query: SearchCondition, getFields: ObjectGetFieldsDescriptor)(implicit session: DBSession): Try[Vector[Entity[_, _, _]]] = ???
    
    def setTypesDefinitionsProvider(typesDefinitionsProvider: TypesDefinitionProvider): Unit =
        this.typesDefinitionsProviderContainer = Some(typesDefinitionsProvider)
    
    def init(typesDefinitionsProvider: TypesDefinitionProviderInitializer): Version =

        initConnectionPoolAndTypesSchema()

        dbUtils.init()

        currentTypesToTablesMap = typesDefinitionsProvider.typesToTablesMap

        val version = DB.localTx(implicit session =>
            val lastVersion = dbUtils.getLatestVersion
            lastVersion.foreach(version =>
                previousTypesToTablesMap = dbUtils.getTypesToTablesMap(version.id)
            )
            val version = dbUtils.addVersion("Version." + lastVersion.map(_.id + 1).getOrElse(1L))
            savedTablesIdsMap = saveTypesToTablesMap(currentTypesToTablesMap, version)
            renameChangedTables(version)
            createOrMigrateTables(typesDefinitionsProvider.getAllPersistenceData, version)
            version
        )

        version

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
        ByteFieldType -> Set("SMALLINT", "INT2"),
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

    private val generateRandomUUIDFunc = "uuid_generate_v4()"

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

    private val integerTypes = Set(LongFieldType, IntFieldType, ShortIntFieldType, ByteFieldType)

    private val typesSchemaName = connectionConf.getString("schema")
    private val primaryKeySuffix = persistenceConf.getString("primary-key-suffix")
    private val foreignKeySuffix = persistenceConf.getString("foreign-key-suffix")
    private val archivedColumnNameSuffix = persistenceConf.getString("archived-column-name-suffix")
    private val sequenceSuffix = persistenceConf.getString("sequence-suffix")
    private val stringIdAutoGenerationFunction = persistenceConf.getString("string-id-auto-generation")
    private val tableAliasInQueryPrefix = persistenceConf.getString("table-alias-in-query-prefix")
//    private val nameSubnamesDelimiter = persistenceConf.getString("column-name-subnames-delimiter")
    private val parentObjectReferenceSubfieldName = persistenceConf.getString("parent-object-reference-subfield-name")
    private val subobjectFieldsDelimiter = "."

    private var typesPersistenceData: Map[AbstractEntityType[_, _, _], TypePersistenceData] = Map()

    private case class GetDescriptorChainCell(currDsc: NestedGetFieldsDescriptor, parentDsc: Option[GetDescriptorChainCell] = None):
        lazy val fieldName: String = parentDsc.map(_.fieldName + subobjectFieldsDelimiter).getOrElse("") + currDsc.fieldName
        lazy val asParentPrefix: String = fieldName + subobjectFieldsDelimiter

    private def getAllObjectTables(
                                      objectType: AbstractObjectEntityType[_, _],
                                      persistanceData: ObjectTypePersistenceDataFinal
                                  ): List[(String, String)] =
                val parentPersistenceData = objectType.valueType.parent.map((parentType: ObjectEntitySuperType[_, _]) =>
                    (parentType, getEntityPersistendeData(parentType).asInstanceOf[ObjectTypePersistenceDataFinal]))
                (persistanceData.tableName, persistanceData.idColumn.columnName) :: (
                    parentPersistenceData match
                        case None =>
                            Nil
                        case Some((parentType, parentPersistenceData)) =>
                            getAllObjectTables(parentType, parentPersistenceData)
                    )

    private case class GetFieldDescriptor(getDescriptorChainCell: GetDescriptorChainCell, persistenceData: Either[ValuePersistenceDataFinal, TypeBackReferenceDefinition[_]])

    private object GetFieldDescriptor:
        def apply(getDescriptorChainCell: GetDescriptorChainCell, persistenceData: ValuePersistenceDataFinal): GetFieldDescriptor =
            GetFieldDescriptor(getDescriptorChainCell, Left(persistenceData))
        def apply(getDescriptorChainCell: GetDescriptorChainCell, persistenceData: TypeBackReferenceDefinition[_]): GetFieldDescriptor =
            GetFieldDescriptor(getDescriptorChainCell, Right(persistenceData))

    private case class TableGetDescriptor(
                                             tableName: String,
                                             idColumn: String,
                                             referer: Option[RefererTableData],
                                             fields: List[GetFieldDescriptor]
                                         )

    private def parseFieldsDescriptiors(
                                           fieldsDescriptors: List[NestedGetFieldsDescriptor],
                                           persistenceData: AbstractObjectPersistenceData,
                                           objectTypeDef: FieldsContainer,
                                           refererPart: RefererTablePartialData,
                                           referrersPrefix: Option[GetDescriptorChainCell],
                                           currTypeName: String
                                       ): (List[GetFieldDescriptor], List[NestedGetFieldsDescriptor], List[TableGetDescriptor] ) =

        fieldsDescriptors.foldLeft((Nil, Nil, Nil))((
                                                        acc: (List[GetFieldDescriptor], List[NestedGetFieldsDescriptor], List[TableGetDescriptor]),
                                                        gfd: NestedGetFieldsDescriptor
                                                    ) =>
            val currFieldChainCell = GetDescriptorChainCell(gfd, referrersPrefix)
            persistenceData.fields.get(gfd.fieldName) match
                case Some(fieldPersistenceData) => fieldPersistenceData match
                    case simpleObjectPersistenceData: SimpleObjectValuePersistenceDataFinal =>
                        val soType = objectTypeDef.fields.getOrElse(gfd.fieldName,
                            throw new ConsistencyException(s"Field ${gfd.fieldName} is not found in object type $currTypeName!"))
                        (gfd, soType.valueType) match
                            case (SubObjectGetFieldsDescriptor(_, Right(fieldsDescriptors)), soTypeDef: SimpleObjectTypeDefinition[_]) =>
                                val soSubfieldsData = getSimpleObjectTablesGetDescriptors(currFieldChainCell,
                                    simpleObjectPersistenceData, soTypeDef, fieldsDescriptors, refererPart, currTypeName)
                                (soSubfieldsData._1 ++ acc._1, acc._2, soSubfieldsData._2 ++ acc._3)
                            case _ =>
                                throw new ConsistencyException(s"Get field descriptor $gfd is not supported for $fieldPersistenceData and $soType!")
                    case primitiveValueFieldPersistenceData: PrimitiveValuePersistenceDataFinal =>
                        (GetFieldDescriptor(currFieldChainCell, primitiveValueFieldPersistenceData) :: acc._1, acc._2, acc._3)
                    case referenceValueFieldPersistenceData: ReferenceValuePersistenceDataFinal =>
                        objectTypeDef.fields.get(gfd.fieldName) match
                            case Some(FieldTypeDefinition(backRef: TypeBackReferenceDefinition[_], _)) =>
                                val refType: AbstractObjectEntityType[_, _] = backRef.referencedType
                                (gfd, refType) match
                                    case (SubObjectGetFieldsDescriptor(_, Right(fieldsDescriptors)), objectParentType: ObjectEntitySuperType[_, _]) =>
                                        val nextReferer = Some(refererPart.toRefererTableData(referenceValueFieldPersistenceData.columnName))
                                        (acc._1, acc._2, getAllObjectTablesGetDescriptors(objectParentType, fieldsDescriptors, nextReferer, Some(currFieldChainCell)) ++ acc._3)
                                    case _ =>
                                        (GetFieldDescriptor(currFieldChainCell, referenceValueFieldPersistenceData) :: acc._1, acc._2, acc._3)
                            case fieldType =>
                                throw new ConsistencyException(s"Back reference field ${gfd.fieldName} is not found in " +
                                    s"object type $currTypeName or has wrong type! Found: $fieldType")
                    case _ =>
                        throw new ConsistencyException(s"Field persistence data $fieldPersistenceData is not supported!")
                case None =>
                    objectTypeDef.fields.get(gfd.fieldName) match
                        case Some(FieldTypeDefinition(backRef: TypeBackReferenceDefinition[_], _)) =>
                            (GetFieldDescriptor(currFieldChainCell, backRef) :: acc._1, acc._2, acc._3)
                        case _ =>
                            (acc._1, gfd :: acc._2, acc._3)
        )

    private def getAllObjectTablesGetDescriptors(
                                                    objectType: AbstractObjectEntityType[_, _],
                                                    fieldsDescriptors: List[NestedGetFieldsDescriptor],
                                                    referer: Option[RefererTableData],
                                                    referrersPrefix: Option[GetDescriptorChainCell]
                                  ): List[TableGetDescriptor] =
//        val simpleObjectsParentsCache = mutable.HashMap[String, List[GetDescriptorChainCell]]()
//        def getSimpleObjectFieldParentPersistenceData(
//                                                         getFieldDescriptorChain: GetDescriptorChainCell,
//                                                         referenceData: ReferenceValuePersistenceDataFinal,
//                                                     ): (ObjectTypeDefinition[_,  _], ValuePersistenceDataFinal) =
//            val fields = getFieldDescriptorChain.parentDsc match
//                case None =>
//                    val parentRefType = objectType.valueType.fields
//                        .get(getFieldDescriptorChain.currDsc.fieldName)
//                        .map(_.valueType)
//                        .getOrElse(throw new ConsistencyException(s"Field ${getFieldDescriptorChain.currDsc.fieldName} " +
//                            s"is not found in object type ${objectType.name}!"))
//                    parentRefType match
//                        case TypeReferenceDefinition(ObjectEntitySuperType(name, parentTypeDef)) =>
//                            persistenceData.fields.get(name) match
//                                case Some(parentPersistenceData) =>
//                                    (parentTypeDef, parentPersistenceData)
//                                case None =>
//                                    throw new ConsistencyException(s"Parent type $name is not found in persistence data!")
//                        case _ =>
//                            throw new ConsistencyException(s"Field type definition $parentRefType is not reference to ObjectEntitySuperType!")
//                case Some(parent) =>
//                    val parentFields = getSimpleObjectFieldParentPersistenceData(parent)
//                    //  .map(persistenceData.fields.get(getFieldDescriptorChain.currDsc.fieldName))//ValuePersistenceDataFinal
//                    parentFields match
//                        case TypeReferenceDefinition(parentType) =>
//                            parentType match
//                                case objectParentType: ObjectEntitySuperType[_, _] => 
//                                    objectParentType
//                                case _ =>
//                                    throw new ConsistencyException(s"Type $parentType is not of ObjectEntitySuperType!")
//                        case _ =>
//                            throw new ConsistencyException(s"Field type definition $parentFields is not of type TypeReferenceDefinition!")
//            fields

        val persistenceData: ObjectTypePersistenceDataFinal = objectType.persistenceData
        val objectTypeDef: ObjectTypeDefinition[_, _] = objectType.valueType
        val refererPart: RefererTablePartialData = RefererTablePartialData(persistenceData.tableName, referer)
        val (
            currTableGFDs: List[GetFieldDescriptor],
            parentsGFDs: List[NestedGetFieldsDescriptor],
            nestedTablesDescriptors: List[TableGetDescriptor]
        ) = parseFieldsDescriptiors(fieldsDescriptors, persistenceData, objectTypeDef, refererPart, referrersPrefix, objectType.name)

        val parentTablesData = if (parentsGFDs.nonEmpty)
            val parentPersistenceData = objectTypeDef.parent.map((parentType: ObjectEntitySuperType[_, _]) =>
                (parentType, getEntityPersistendeData(parentType).asInstanceOf[ObjectTypePersistenceDataFinal]))
                .getOrElse(new ConsistencyException(s"Fields ${parentsGFDs.map(_.fieldName).mkString(", ")} are not found in " +
                    s"top supertype ${objectType.name}!"))
        else
            Nil

        TableGetDescriptor(persistenceData.tableName, persistenceData.idColumn.columnName, referer, currTableGFDs) :: (
            nestedTablesDescriptors ++ (
                objectTypeDef.parent match
                    case None =>
                        if (parentsGFDs.nonEmpty)
                            throw new ConsistencyException(s"Fields ${parentsGFDs.map(_.fieldName).mkString(", ")} are not found in " +
                                s"top supertype ${objectType.name}!")
                        Nil
                    case Some(parentType) =>
                        val nextReferer: Option[RefererTableData] =
                            Some(RefererTableData(persistenceData.tableName, persistenceData.idColumn.columnName, referer))
                        getAllObjectTablesGetDescriptors(parentType, parentsGFDs, nextReferer, referrersPrefix)
            ))

    private def getSimpleObjectTablesGetDescriptors(
                                      parentsPrefix: GetDescriptorChainCell,
                                      simpleObjectPersistenceData: SimpleObjectValuePersistenceDataFinal,
                                      simpleObjectTypeDef: SimpleObjectTypeDefinition[_],
                                      fieldsDescriptors: List[NestedGetFieldsDescriptor],
                                      referer: RefererTablePartialData,
                                      currTypeName: String
                              ): (List[GetFieldDescriptor], List[TableGetDescriptor]) =


        val (
            sameTableData: List[GetFieldDescriptor],
            parentGFDs: List[NestedGetFieldsDescriptor],
            nestedTDs: List[TableGetDescriptor]
        ) =
            parseFieldsDescriptiors(fieldsDescriptors, simpleObjectPersistenceData, simpleObjectTypeDef, referer, Some(parentsPrefix), currTypeName)

        
        val soParentTableDescripros: List[TableGetDescriptor] =
            if (parentGFDs.nonEmpty) 
                lazy val presentParentGFDsLine = "Parent fields are present in descriptor " + parentGFDs.mkString(", ")
                val parentPersistenceReferenceData = simpleObjectPersistenceData.parent.getOrElse(
                    throw new ConsistencyException(s"$presentParentGFDsLine, but persistence parent reference not found in $simpleObjectPersistenceData!"))
                val refTableName = parentPersistenceReferenceData.refTableData.data.getOrElse(
                    throw new ConsistencyException(s"Parent reference table data not found in simple Object persistence data $simpleObjectPersistenceData!")
                ).tableName
                var currParentType: ObjectEntitySuperType[_, _] = simpleObjectTypeDef.parent.getOrElse(
                    throw new ConsistencyException(s"$presentParentGFDsLine, but there is no parent in $simpleObjectTypeDef!"))
                var currParentTableName = currParentType.persistenceData.tableName
                while (currParentTableName != refTableName) 
                    currParentType = currParentType.valueType.parent.getOrElse(
                        throw new ConsistencyException(s"$presentParentGFDsLine, but there is no parents with referenced table name in $simpleObjectTypeDef!"))
                    currParentTableName = currParentType.persistenceData.tableName
                getAllObjectTablesGetDescriptors(currParentType, parentGFDs, Some(referer.toRefererTableData(parentPersistenceReferenceData.columnName)), Some(parentsPrefix))
            else
                Nil

        (sameTableData, soParentTableDescripros ++ nestedTDs)

    private def getIdValueMapper(idType: PersistenceFieldType, pos: Int): WrappedResultSet => FilledEntityId[_, _] =
        (rs: WrappedResultSet) => idType match
            case LongFieldType => LongId(rs.long(pos))
            case IntFieldType => IntId(rs.int(pos))
            case ShortIntFieldType => ShortIntId(rs.short(pos))
            case ByteFieldType => ByteId(rs.byte(pos))
            case UUIDFieldType => UUIDId(rs.get[UUID](pos))
            case StringFieldType => StringId(rs.string(pos))
            case idType: FixedStringFieldType =>
                FixedStringId(rs.string(pos), FixedStringIdTypeDefinition(idType.length))
            case _ => throw new ConsistencyException(s"Id type $idType is not supported!")

    private def getIdValueMapper(idColumn: PrimitiveValuePersistenceDataFinal): WrappedResultSet => FilledEntityId[_, _] =
        (rs: WrappedResultSet) => idColumn.columnType match
            case LongFieldType => LongId(rs.long(idColumn.columnName))
            case IntFieldType => IntId(rs.int(idColumn.columnName))
            case ShortIntFieldType => ShortIntId(rs.short(idColumn.columnName))
            case ByteFieldType => ByteId(rs.byte(idColumn.columnName))
            case UUIDFieldType => UUIDId(rs.get[UUID](idColumn.columnName))
            case StringFieldType => StringId(rs.string(idColumn.columnName))
            case idType: FixedStringFieldType =>
                FixedStringId(rs.string(idColumn.columnName), FixedStringIdTypeDefinition(idType.length))
            case _ => throw new ConsistencyException(s"Id type ${idColumn.columnType} is not supported!")


    private def getColumnsValuesAndRestFields(
                                                 filedValues: List[(String, EntityValue)],
                                                 fieldsPersistenceData: Map[String, ValuePersistenceDataFinal]
                                             ): (List[(String, Any)], List[(String, EntityValue)]) =
        val res = filedValues.map { (fieldName, fieldValue) =>
            fieldsPersistenceData.get(fieldName) match
                case Some(fieldPersistenceData) =>
                    (fieldValue, fieldPersistenceData) match
                        case (prim: RootPrimitiveValue[_], PrimitiveValuePersistenceDataFinal(columnName, _, isNullable)) =>
                            (List((columnName, prim.value)), Nil)
                        case (value: ReferenceValue[_], fieldPersData: ReferenceValuePersistenceDataFinal) =>
                            (List((fieldPersData.columnName, value.refId)), Nil)
                        case (SimpleObjectValue(id, subFields, _), SimpleObjectValuePersistenceDataFinal(parentPersOpt, fieldsPers)) =>
                            val parentData =
                                id.map { id =>
                                    val parentPers = parentPersOpt.getOrElse(throw new ConsistencyException("Parent is not defined!"))
                                    (parentPers.columnName, id)
                                }
                            val fieldsData: (List[(String, Any)], List[(String, EntityValue)]) = getColumnsValuesAndRestFields(subFields.toList, fieldsPers)
                            (fieldsData._1 ++ parentData.toList, fieldsData._2)
                        case _ => throw new ConsistencyException(s"Field value is of not known type, or found pesistent " +
                            s"data not compatible! Value: $fieldValue. Persistence data: $fieldPersistenceData")
                case None =>
                    (Nil, List((fieldName, fieldValue)))
        }
        (
            res.flatMap(_._1),
            res.flatMap(_._2)
        )

    private def getColumnsValues(
                                    filedValues: List[(String, EntityValue)],
                                    fieldsPersistenceData: Map[String, ValuePersistenceDataFinal],
                                    parent: Option[ObjectEntitySuperType[_, _]],
                                    tableName: String,
                                    idColumnName: PrimitiveValuePersistenceDataFinal,
                                    typeName: String,
                                    level: Byte,
                                ): List[(String, PrimitiveValuePersistenceDataFinal, List[(String, Any)], Byte)] =
        val (columnsValues, restFields) = getColumnsValuesAndRestFields(filedValues, fieldsPersistenceData)
        if (restFields.isEmpty)
            List((tableName, idColumnName, columnsValues, level))
        else
            parent match
                case None =>
                    throw new ConsistencyException(s"Fields ${restFields.map(_._1).mkString(", ")} are not found in " +
                        s"type $typeName and there is no parent of those type!")
                case Some(parent) =>
                    getEntityPersistendeData(parent) match
                        case ObjectTypePersistenceDataFinal(tableName, idColumn, fields, _) =>
                            List((tableName, idColumnName, columnsValues, level)) ++
                                getColumnsValues(restFields, fields, parent.valueType.parent, tableName,
                                    idColumn, typeName, (level + 1).toByte)
                        case _ => throw new ConsistencyException("Parent is not Object!")


    private def splitValuesByTypes(
                                      values: Seq[ItemValue],
                                      persData: ArrayTypePersistenceDataFinal
                                  ): Map[ItemTypePersistenceDataFinal, Seq[Any]] =
        values.map {
            case pv: RootPrimitiveValue[_] => (
                persData.itemsMap.getOrElse(typesMapper.getValueFieldType(pv.typeDefinition.valueType),
                    throw new ConsistencyException(s"Item value type is not found! ${pv.typeDefinition.valueType}")),
                pv.value
            )
            case rv: ReferenceValue[_] => (
                persData.itemsMap.getOrElse(typesMapper.getIdFieldType(rv.typeDefinition.valueType.idType),
                    throw new ConsistencyException(s"Item id type is not found! ${rv.typeDefinition.valueType.idType}")),
                rv.refId.value
            )
        }.groupMap(_._1)(_._2)

    private def getSequenceName(typeName: String): String = esc(typeName + sequenceSuffix)

    private def mapColColuntResult(count: Int, nonUniqueErrorMessage: String, expectedCount: Int): Option[Unit] =
        if count == 0 then None
        else if count == expectedCount then Some(())
        else throw new ConsistencyException(nonUniqueErrorMessage)

    private def getEntityPersistendeData(entityType: AbstractEntityType[_, _, _]) = {
        typesDefinitionsProvider.getPersistenceData(entityType.name).getOrElse(
            throw new ConsistencyException(s"Type persistence data not found for ${entityType.name}!"))
    }


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
                ALTER TABLE $typesSchemaName.${esc(currTableName)} RENAME TO $typesSchemaName.${esc(newTableName)}
            """).execute.apply()

        val existingTableNames = metadataUtils.getTableNames(typesSchemaName)

        currentTypesToTablesMap.foreach { case (typeName, currTableName) =>
            previousTypesToTablesMap.get(typeName).foreach(prevTableName =>
                if (prevTableName != currTableName && existingTableNames.contains(prevTableName))
                    renameTable(prevTableName, currTableName)
                    dbUtils.addTableRenamingData(prevTableName, currTableName, version.id)
            )
        }

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
            .exists(_.contains(dbTypeName.toUpperCase))

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
                ALTER TABLE $typesSchemaName.${esc(tableName)} ADD CONSTRAINT ${esc(foreignKeyName(tableName, columnName, refTableName))}
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
            val idType = if isArray then getFieldType(idColumn.columnType) else getIdFieldType(idColumn.columnType)
            val valueType = getFieldType(valueColumnType)
            val idAutogenerator = getIdAutoGenerator(idColumn.columnType)
            val idAutogeneratorSql = if idAutogenerator.isEmpty then "" else s" DEFAULT $idAutogenerator"

            SQL(s"""
                CREATE TABLE $typesSchemaName.${esc(tableName)} (
                    ${esc(idColumn.columnName)} $idType NOT NULL $idAutogeneratorSql,
                    ${esc(valueColumnName)} $valueType NOT NULL
                    $pkSql
                )
            """).execute.apply()
            savedTablesIdsMap(tableName)
            val tableId = savedTablesIdsMap(tableName)
            dbUtils.addTableColumn(tableId, idColumn.columnName, idType, "id")
            dbUtils.addTableColumn(tableId, valueColumnName, valueType, "value")

        def getIdAutoGenerator(columnType: PersistenceFieldType): String =
            columnType match
                case UUIDFieldType => generateRandomUUIDFunc
                case StringFieldType(_) => s"$stringIdAutoGenerationFunction"
                case FixedStringFieldType(len) => s"LPAD($stringIdAutoGenerationFunction, $len, '0')"
                case _ => ""

        def renameColumn(tableName: String, prevColumnName: String, newColumnName: String): Unit =
            SQL(s"""
                ALTER TABLE $typesSchemaName.${esc(tableName)} RENAME COLUMN ${esc(prevColumnName)} TO ${esc(newColumnName)}
            """).execute.apply()
            dbUtils.addTableRenamingData(tableName, prevColumnName, newColumnName, version.id)

        def dropPrimaryKey(pk: PrimaryKeyData): Unit =
            dropConstraint(pk.tableName, pk.keyName, true)

        def dropForeignKey(fk: ForeignKeyData): Unit =
            dropConstraint(fk.tableName, fk.keyName, false)

        def dropConstraint(tableName: String, keyName: String, cascade: Boolean): Unit =
            val cascadeSql = if cascade then " CASCADE" else ""
            SQL(s"""
                ALTER TABLE $typesSchemaName.${esc(tableName)} DROP CONSTRAINT ${esc(keyName)} $cascadeSql
            """).execute.apply()

        def createPrimaryKeyExecute(tableName: String, columnName: String): Unit =
            SQL(s"""
                ALTER TABLE $typesSchemaName.${esc(tableName)} ADD CONSTRAINT ${esc(tableName + primaryKeySuffix)} PRIMARY KEY (${esc(columnName)})
            """).execute.apply()

        def addColumn(tableName: String, columnName: String, columnType: String, isNullable: Boolean, default: String = ""): Unit =
            val defaultSql = if default.isEmpty then "" else s" DEFAULT $default"
            SQL(s"""
                ALTER TABLE $typesSchemaName.${esc(tableName)} ADD COLUMN ${esc(columnName)} $columnType ${if isNullable then "" else "NOT NULL"} $defaultSql
            """).execute.apply()

        def addDefaultValue(tableName: String, columnName: String, default: String): Unit =
            SQL(s"""
                ALTER TABLE $typesSchemaName.${esc(tableName)} ALTER COLUMN ${esc(columnName)} SET DEFAULT $default
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
            val idColumnType = getIdFieldType(idColumn.columnType)
            val idAutogenerator = getIdAutoGenerator(idColumn.columnType)
            val idAutogeneratorSql = if idAutogenerator.isEmpty then "" else s" DEFAULT $idAutogenerator"

            SQL(s"""
                CREATE TABLE $typesSchemaName.${esc(tableName)} (
                    ${esc(idColumn.columnName)} $idColumnType NOT NULL idAutogeneratorSql,
                    $fieldsSql
                    CONSTRAINT ${esc(tableName + primaryKeySuffix)} PRIMARY KEY (${esc(idColumn.columnName)})
                )
            """).execute.apply()
            val tableId = savedTablesIdsMap(tableName)
            dbUtils.addTableColumn(tableId, idColumn.columnName, idColumnType, "id")
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
            idAutogenerator: String,
        ): Unit =

            def addPrimaryKeyColumn(
                                   tableName: String,
                                   idColumn: PrimitiveValuePersistenceDataFinal,
                                   isArray: Boolean,
                                   prevColumnName: String,
                                   default: String = ""
                               ): Unit =
                val columnType: String = getIdFieldType(idColumn.columnType)
                addColumn(tableName, idColumn.columnName, columnType, false, default)
                if (!isArray)
                    createPrimaryKeyExecute(tableName, idColumn.columnName)
                    dbUtils.addPrimaryKeyAlteringData(tableName, prevColumnName, idColumn.columnName, version.id)

            def replacePKOnDifferentType(
                                            pkColumn: String,
                                            pkData: PrimaryKeyData,
                                            existingIdColumnData: Option[ColumnData],
                                            preprocess: Boolean => Unit
                                        ): Unit =
                val isSameTypeCheckFn = if isArray then isSameType _ else isSameIdType _
                val isDifferentType = !isSameTypeCheckFn(idColumn.columnType, existingColumns(pkColumn).columnType)
                if (isDifferentType) dropPrimaryKey(pkData)
                preprocess(isDifferentType)
                if (isDifferentType)
                    addPrimaryKeyColumn(tableName, idColumn, isArray, pkColumn, idAutogenerator)
                else if (idAutogenerator.nonEmpty && !existingIdColumnData.exists(_.defaultValue.exists(_.contains(idAutogenerator))))
                    addDefaultValue(tableName, idColumn.columnName, idAutogenerator)


            def replacePrimaryKey(pk: PrimaryKeyData, oldIdColumnName: String, existingIdColumnData: Option[ColumnData]): Unit =
                dropPrimaryKey(pk)
                if (!isArray)
                    createPrimaryKeyExecute(tableName, idColumn.columnName)
                    dbUtils.addPrimaryKeyAlteringData(tableName, oldIdColumnName, idColumn.columnName, version.id)
                    if (idAutogenerator.nonEmpty && !existingIdColumnData.exists(_.defaultValue.exists(_.contains(idAutogenerator))))
                        addDefaultValue(tableName, idColumn.columnName, idAutogenerator)

            def createPrimaryKey(existingIdColumnData: Option[ColumnData]): Unit =
                createPrimaryKeyExecute(tableName, idColumn.columnName)
                dbUtils.addPrimaryKeyAlteringData(tableName, null, idColumn.columnName, version.id)
                if (idAutogenerator.nonEmpty && !existingIdColumnData.exists(_.defaultValue.exists(_.contains(idAutogenerator))))
                    addDefaultValue(tableName, idColumn.columnName, idAutogenerator)

            val pkDataOption = metadataUtils.getTablePrimaryKeys(tableName)
            if (pkDataOption.forall(_.columnNames.size <= 1))
                try
                    val existingIdColumnData = existingColumns.get(idColumn.columnName)
                    if (pkDataOption.isDefined)
                        val pkData = pkDataOption.get
                        val pkColumn = pkData.columnNames.head
                        if (existingIdColumnData.isDefined)
                            if (pkColumn == idColumn.columnName)
                                replacePKOnDifferentType(pkColumn, pkData, existingIdColumnData,
                                    if (_) renameColumn(tableName, pkColumn, getRenamedArchivedColumnName(pkColumn, existingColumns))
                                )
                            else
                                replacePrimaryKey(pkData, pkColumn, existingIdColumnData)
                        else
                            replacePKOnDifferentType(pkColumn, pkData, existingIdColumnData, isDifferentType =>
                                if (!isDifferentType) renameColumn(tableName, pkColumn, idColumn.columnName)
                            )
                    else
                        if (existingIdColumnData.isDefined)
                            if (!isArray)
                                createPrimaryKey(existingIdColumnData)
                        else
                            addPrimaryKeyColumn(tableName, idColumn, isArray, null, idAutogenerator)

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
                                                   fieldName: String,
        ): Unit =
            val valueColumnPGType = getFieldType(valueColumnType)
            if (existingColumns.contains(valueColumnName))
                if (isSameType(valueColumnType, existingColumns(valueColumnName).columnType))
                    //ignoring else - Value column of same type already exists - no actions
                    renameColumn(tableName, valueColumnName, getRenamedArchivedColumnName(valueColumnName,
                        existingColumns))
                    addColumn(tableName, valueColumnName, valueColumnPGType, isNullable)
                    dbUtils.addTableRenamingData(tableName, null, valueColumnName, version.id)
            else
                addColumn(tableName, valueColumnName, valueColumnPGType, isNullable)
                dbUtils.addTableRenamingData(tableName, null, valueColumnName, version.id)
            dbUtils.addTableColumn(savedTablesIdsMap(tableName), valueColumnName, valueColumnPGType, fieldName)

        def checkAndFixExistingSingleValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            valueColumnName: String,
            valueColumnType: PersistenceFieldType,
            isArray: Boolean,
        ): Unit =
            val idAutogenerator = getIdAutoGenerator(idColumn.columnType)
            val existingColumns: Map[String, ColumnData] =  metadataUtils.getTableColumnsDataMap(tableName)
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns, isArray, idAutogenerator)
            checkAndFixExistingTableValueColumn(tableName, valueColumnName, valueColumnType, false, 
                existingColumns, "value")


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
                    parentTableRef.refTableData.idColumnType, false, existingColumns, fieldsPrefix + parentObjectReferenceSubfieldName)
            )
            fields.foreach((fieldName, fieldData) =>
                fieldData match
                    case PrimitiveValuePersistenceDataFinal(columnName, columnType, isNullable) =>
                        checkAndFixExistingTableValueColumn(tableName, columnName, columnType, isNullable, 
                            existingColumns, fieldsPrefix + fieldName)
                    case ref: ReferenceValuePersistenceDataFinal =>
                        checkAndFixExistingTableValueColumn(tableName, ref.columnName, ref.refTableData.idColumnType,
                            ref.isNullable, existingColumns, fieldsPrefix + fieldName)
                    case SimpleObjectValuePersistenceDataFinal(parent, fields) =>
                        checkAndFixExistingSimpleObjectValueTable(tableName, fields, parent, existingColumns,
                            Some(fieldsPrefix + fieldName + "."))
            )

        def checkAndFixExistingObjectValueTable(
            tableName: String,
            idColumn: PrimitiveValuePersistenceDataFinal,
            fields: Map[String, ValuePersistenceDataFinal],
            existingColumnsOption: Option[Map[String, ColumnData]] = None, 
        ): Unit =
            val idAutogenerator = getIdAutoGenerator(idColumn.columnType)
            val existingColumns: Map[String, ColumnData] = existingColumnsOption.getOrElse(
                metadataUtils.getTableColumnsDataMap(tableName) )
            checkAndFixExistingTableIdColumn(tableName, idColumn, existingColumns, false, idAutogenerator)
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
                case ArrayTypePersistenceDataFinal(items, idType, typeName) =>
                    if (integerTypes.contains(idType))
                        SQL(s"""
                            CREATE SEQUENCE IF NOT EXISTS $typesSchemaName.${getSequenceName(typeName)}
                                START WITH 1  INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
                        """.stripMargin).execute.apply()
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


