package my.valerii_timakov.sgql.services.repositories.postres

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.type_definitions.{ArrayTypeDefinition, CustomPrimitiveTypeDefinition, EntityIdTypeDefinition, FieldTypeDefinition, FieldValueTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, ItemValueTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, ByteId, CustomPrimitiveValue, Entity, EntityId, EntityValue, FixedStringId, IntId, ItemValue, LongId, ObjectValue, ReferenceValue, RootPrimitiveValue, ShortIntId, SimpleObjectValue, StringId, UUIDId, ValueTypes}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, AbstractObjectEntityType, AbstractPrimitiveEntityType, ArrayEntityType, CustomPrimitiveEntityType, EntityType, ObjectEntitySuperType, ObjectEntityType, PrimitiveEntitySuperType}
import my.valerii_timakov.sgql.entity.read_modiriers.{AbstractObjectGetFieldsDescriptor, AllGetFieldsDescriptor, AllInReferenceGetFieldsDescriptor, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, ObjectGetFieldsDescriptor, SearchCondition, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, DbTableMigrationException, NotInitializedException}
import my.valerii_timakov.sgql.services.*
import scalikejdbc.*

import java.sql.ResultSet
import java.util.UUID
import scala.collection.mutable

implicit val uuidTypeBinder: TypeBinder[UUID] = TypeBinder[UUID](
    (rs: ResultSet, colIdx: Int) => UUID.fromString(rs.getString(colIdx))
)(
    (rs: ResultSet, colName: String) => UUID.fromString(rs.getString(colName))
)

private trait ReferredTable:
    def tableName: String
    def referer: Option[RefererTableData]

private case class RefererTableData(
                                       tableName: String,
                                       columnName: String,
                                       referer: Option[RefererTableData]
                                   ) extends ReferredTable

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


    override def create(entityType: EntityType[_, _, _], data: ValueTypes): EntityId[_, _] =
        DB.localTx { implicit session =>
            create(entityType, data)(session)
        }

    override def update(entity: Entity[_, _, _]): Option[Unit] =
        DB.autoCommit { implicit session =>
            update(entity)(session)
        }

    override def delete(entityType: EntityType[_, _, _], id: EntityId[_, _]): Option[Unit] =
        DB.autoCommit { implicit session =>
            delete(entityType, id)(session)
        }

    override def get[ID <: EntityId[_, ID]](
                                               entityType: EntityType[ID, _, _], 
                                               id: EntityId[_, ID], 
                                               getFields: ObjectGetFieldsDescriptor
                                           ): Option[Entity[ID, _, _]] =
        DB.readOnly { implicit session =>
            get(entityType, id, getFields)(session)
        }

    override def find(entityType: EntityType[_, _, _], query: SearchCondition, getFields: ObjectGetFieldsDescriptor): Vector[Entity[_, _, _]] =
        DB.readOnly { implicit session =>
            find(entityType, query, getFields)(session)
        }

    def create(entityType: EntityType[_, _, _], data: ValueTypes)(implicit session: DBSession): EntityId[_, _] =
        
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
                .sortBy(_._4)(Ordering[Byte])
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

    def update(entity: Entity[_, _, _])(implicit session: DBSession): Option[Unit] =

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
            getColumnsValues(filedValuesMap.toList, fieldsPersistenceData, entityType.typeDefinition.parent,
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

    def delete(entityType: EntityType[_, _, _], id: EntityId[_, _])(implicit session: DBSession): Option[Unit] =
        def deleteArrayValues(tablesData: Set[ItemTypePersistenceDataFinal]): Option[Unit] =
            val res = tablesData.map(tableData =>
                SQL(s"""DELETE FROM $typesSchemaName.${tableData.tableName} WHERE ${esc(tableData.idColumn.columnName)} = ?""")
                    .bind(true, id.value)
                    .update.apply()
            ).sum
            if (res == 0)
                None
            else
                Some(())
        def deleteSingleValue(tableName: String, idColumnName: String): Option[Unit] =
            val res = SQL(s"""DELETE FROM $typesSchemaName.$tableName WHERE ${esc(idColumnName)} = ?""")
                .bind(id.value)
                .update.apply()
            mapColColuntResult(res, s"Multiple entities deleted for id: $id!", 1)

        (entityType, entityType.persistenceData) match
            case (_, PrimitiveTypePersistenceDataFinal(tableName, idColumn, valueColumn)) =>
                deleteSingleValue(tableName, idColumn.columnName)
            case (objectType: ObjectEntityType[_, _], persistanceData: ObjectTypePersistenceDataFinal) =>
                getAllObjectTables(objectType, persistanceData)
                    .map((tableName, idColumnName) => deleteSingleValue(tableName, idColumnName))
                    .fold(Some(()))( (acc, res) => if acc.isDefined then res else None )
            case (arrayType: ArrayEntityType[_, _], ArrayTypePersistenceDataFinal(items, _, _)) =>
                deleteArrayValues(items)

    def get[ID1 <: EntityId[_, ID1]](
                                        entityType: EntityType[ID1, _, _],
                                        id: EntityId[_, ID1],
                                        getFields: ObjectGetFieldsDescriptor
                                  )(implicit session: DBSession): Option[Entity[ID1, _, _]] =
        def getTableAliace(map:  Map[(Option[RefererTableData], String), String], tgd: ReferredTable) =
            map.getOrElse((tgd.referer, tgd.tableName), throw new ConsistencyException(s"Table aliace not found for ${(tgd.referer, tgd.tableName)}"))
            
        def getTablesLineReversedInner(tablesLines: List[String], prevLength: Int, currDelimiter: String, nextDelimiter: String): StringBuilder =
            if (tablesLines.nonEmpty)
                val currValue = tablesLines.head
                val result = getTablesLineReversedInner(tablesLines.tail, 
                    prevLength + currValue.length + currDelimiter.length, nextDelimiter, nextDelimiter)
                result.append(currValue)
                result.append(currDelimiter)
                result
            else 
                new StringBuilder(prevLength)
                
        def getListLineReversed(tablesLines: List[String], delimiter: String): String =
            getTablesLineReversedInner(tablesLines, 0, "", delimiter).toString


        def extractRefObject2(
            refFieldTypeDef: TypeReferenceDefinition[_],
            subFieldsDscs: List[NestedGetFieldsDescriptor],
            ownerDsc: GetDescriptorChainCell,
            refFieldIdx: Int,
            refFieldName: String,
            rs: WrappedResultSet,
            fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])],
        ): ReferenceValue[_] =
            extractRefObject(refFieldTypeDef, subFieldsDscs, ownerDsc, refFieldIdx, refFieldName, rs, fieldsMap)

        def extractRefObject[ID2 <: EntityId[_, ID2]](
                                                               refFieldTypeDef: TypeReferenceDefinition[ID2],
                                                               subFieldsDscs: List[NestedGetFieldsDescriptor],
                                                               ownerDsc: GetDescriptorChainCell,
                                                               refFieldIdx: Int,
                                                               refFieldName: String,
                                                               rs: WrappedResultSet,
                                                               fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])],
                       ): ReferenceValue[ID2] =
            val refEntity = refFieldTypeDef.referencedType match
                case refType: ObjectEntityType[ID2, _] =>
                    extractObject(refType, subFieldsDscs, Some(ownerDsc), rs, fieldsMap).getOrElse(
                        throw new ConsistencyException(s"Referenced object for field $refFieldName is absent in DB!")
                    )
                case refType: ObjectEntitySuperType[ID2, _] =>
                    extractObjectSuperTypeEntity(refType, subFieldsDscs, ownerDsc, refFieldName, rs, fieldsMap)
            val reference: ReferenceValue[ID2] = refFieldTypeDef.extract(rs, refFieldIdx).getOrElse(
                throw new ConsistencyException(s"Reference for field $refFieldName is absent in DB!")
            )
            reference.setRefValue(refEntity)
            reference

        def extractObjectSuperTypeEntity[ID2 <: EntityId[_, ID2]](
                                                                           refType: ObjectEntitySuperType[ID2, _],
                                                                           subFieldsDscs: List[NestedGetFieldsDescriptor],
                                                                           ownerDsc: GetDescriptorChainCell,
                                                                           refFieldName: String,
                                                                           rs: WrappedResultSet,
                                                                           fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])],
                                                                           fieldValuesMap: Map[String, EntityValue] = Map()
        ) =
            val entities = typesDefinitionsProvider.getAllLeafObjectsSubtypesTyped(refType).map(leafSubType =>
                    extractObject(leafSubType, subFieldsDscs, Some(ownerDsc), rs, fieldsMap)
                )
                .collect({ case Some(entity) => entity })
            if (entities.size == 1)
                entities.iterator.next()
            else if (entities.isEmpty)
                throw new ConsistencyException(s"Referenced object for field $refFieldName is absent in DB!")
            else
                throw new ConsistencyException(s"Multiple entities found for field $refFieldName! Found: $entities")


        def getLastExistingChild[ID2 <: EntityId[_, ID2]](
                                                                   objectType: AbstractObjectEntityType[ID2, _],
                                                                   ownerDsc: GetDescriptorChainCell,
                                                                   rs: WrappedResultSet,
                                                                   fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]
        ): Option[(ID2, AbstractObjectEntityType[ID2, _])] =
            extractObjectId(objectType, Some(ownerDsc), rs, fieldsMap).map(id =>
                objectType match
                    case leafType: ObjectEntityType[ID2, _] =>
                        (id, leafType.asInstanceOf[ObjectEntityType[ID2, _]])
                    case superType: ObjectEntitySuperType[ID2, _] =>
                        val existingChildren = superType.directChildren
                            .map(getLastExistingChild(_, ownerDsc, rs, fieldsMap))
                            .collect({ case Some(childType) => childType })
                        if (existingChildren.size == 1)
                            existingChildren.iterator.next()
                        else if (existingChildren.isEmpty)
                            (id, superType.asInstanceOf[ObjectEntitySuperType[ID2, _]])
                        else
                            throw new ConsistencyException(s"Multiple entities found for type $objectType! Found: $existingChildren")
            )


        def extractSimpleObject2(
                                                            objectType: SimpleObjectTypeDefinition[_],
                                                            getFieldsDscs: List[NestedGetFieldsDescriptor],
                                                            ownerDsc: GetDescriptorChainCell,
                                                            rs: WrappedResultSet,
                                                            fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]
                                                        ): Option[SimpleObjectValue[_]] =
            extractSimpleObject(objectType, getFieldsDscs, ownerDsc, rs, fieldsMap)

        def extractSimpleObject[ID2 <: EntityId[_, ID2]](
                                                                  objectType: SimpleObjectTypeDefinition[ID2],
                                                                  getFieldsDscs: List[NestedGetFieldsDescriptor],
                                                                  ownerDsc: GetDescriptorChainCell,
                                                                  rs: WrappedResultSet,
                                                                  fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]
        ): Option[SimpleObjectValue[ID2]] =
            val fields: List[(String, EntityValue)] =
                extractFieldsValues(getFieldsDscs, Some(ownerDsc), rs, fieldsMap)
                .collect({case (fieldName, Some(fieldValue)) => (fieldName, fieldValue) })

            objectType.parent match
                case Some(refType) =>
                    getLastExistingChild(refType, ownerDsc, rs, fieldsMap)
                        .map(idAndParent => objectType.createValue(Some(idAndParent), fields.toMap))                    
                case None =>
                    if (fields.isEmpty)
                        None
                    else
                        Some(objectType.createValue(None, fields.toMap))

        def extractObject[ID2 <: EntityId[_, ID2]](
                                                            objectType: ObjectEntityType[ID2, _],
                                                            mainObjectGetFieldsDscs: List[NestedGetFieldsDescriptor],
                                                            ownerDsc: Option[GetDescriptorChainCell],
                                                            rs: WrappedResultSet,
                                                            fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]
                   ): Option[ObjectValue[ID2, _]] =
            extractObjectId(objectType, ownerDsc, rs, fieldsMap).map(id =>
                val fields = extractFieldsValues(mainObjectGetFieldsDscs, ownerDsc, rs, fieldsMap).toMap
                objectType.createEntityAndCheckFields(id, fields)
            )
            
        def extractObjectId[ID2 <: EntityId[_, ID2]](
                                                              objectType: AbstractObjectEntityType[ID2, _],
                                                              ownerDsc: Option[GetDescriptorChainCell],
                                                              rs: WrappedResultSet,
                                                              fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]
        ): Option[ID2] =
            val idDscChainCell = GetDescriptorChainCell(SingleGetFieldsDescriptor(entityIdFieldNameForDsc), ownerDsc)
            fieldsMap.get((idDscChainCell, Some(objectType.name))) match
                case Some((idx, idType: EntityIdTypeDefinition[ID2])) =>
                    idType.extract(rs, idx)
                case _ =>
                    throw new ConsistencyException(s"ID data $idDscChainCell not found in fields map!")
            
        def extractFieldsValues(
            getFieldsDscs: List[NestedGetFieldsDescriptor],
            ownerDsc: Option[GetDescriptorChainCell],
            rs: WrappedResultSet,
            fieldsMap: Map[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]
        ): List[(String, Option[EntityValue])] =
            getFieldsDscs.map(fieldGDsc =>
                val currFieldDsc = GetDescriptorChainCell(fieldGDsc, ownerDsc)
                val (fieldIdx, fieldType) = fieldsMap.getOrElse((currFieldDsc, None),
                    throw new ConsistencyException(s"Field $fieldGDsc not found in fields map!"))
                (fieldGDsc, fieldType) match
                    case (SingleGetFieldsDescriptor(fieldName), fieldTypeDef: RootPrimitiveTypeDefinition[_]) =>
                        fieldName -> fieldTypeDef.extract(rs, fieldIdx)
                    case (SingleGetFieldsDescriptor(fieldName), fieldTypeDef: TypeReferenceDefinition[_]) =>
                        fieldName -> fieldTypeDef.extract(rs, fieldIdx)
                    case (SubObjectGetFieldsDescriptor(fieldName, Right(subFieldsDscs)), fieldTypeDef: TypeReferenceDefinition[_]) =>
                        fieldName -> Some(extractRefObject2(fieldTypeDef, subFieldsDscs, currFieldDsc, fieldIdx, fieldName, rs, fieldsMap))
                    case (SubObjectGetFieldsDescriptor(fieldName, Right(subFieldsDscs)), soTypeDef: SimpleObjectTypeDefinition[_]) =>
                        fieldName -> extractSimpleObject2(soTypeDef, subFieldsDscs, currFieldDsc, rs, fieldsMap)
            )

        def checkAndExpandNotExpandedDescriptors(
                                                    getFields: AbstractObjectGetFieldsDescriptor, objectTypeDef: FieldsContainer
                                                ): List[NestedGetFieldsDescriptor] =
            getFields.fields match
                case Left(AllGetFieldsDescriptor) =>
                    expandAllFieldsGetDescriptor(objectTypeDef)
                case Right(fields) => fields.map(fieldDsc =>
                    val nestedField: NestedGetFieldsDescriptor =  fieldDsc match
                        case subObjectDsc: SubObjectGetFieldsDescriptor =>
                            val fieldObjecDef: FieldsContainer = objectTypeDef.getFieldType(subObjectDsc.fieldName, false).valueType match
                                case soDef: SimpleObjectTypeDefinition[_] =>
                                    soDef
                                case ref: TypeReferenceDefinition[_] =>
                                    ref.referencedType match
                                        case refObjType: AbstractObjectEntityType[_, _] =>
                                            refObjType.typeDefinition
                                        case _ =>
                                            throw new ConsistencyException(s"Referenced type ${ref.name} is not object " +
                                                s"when described as SubObjectGetFieldsDescriptor for field ${subObjectDsc.fieldName}!")
                                case other =>
                                    throw new ConsistencyException(s"Type of ${subObjectDsc.fieldName} field $other is " +
                                        s"not object when described as SubObjectGetFieldsDescriptor!")
                            val subFieldsDescriptors: List[NestedGetFieldsDescriptor] = subObjectDsc.fields match
                                case Left(AllGetFieldsDescriptor) =>
                                    expandAllFieldsGetDescriptor(fieldObjecDef)
                                case Right(fields) =>
                                    checkAndExpandNotExpandedDescriptors(subObjectDsc, fieldObjecDef)
                            SubObjectGetFieldsDescriptor(subObjectDsc.fieldName, Right(subFieldsDescriptors))
                        case _ =>
                            fieldDsc
                    nestedField
                )

            
        def expandAllFieldsGetDescriptor(objecDef: FieldsContainer): List[NestedGetFieldsDescriptor] =
           objecDef.allFields.map { case (fieldName, fieldType) =>
                fieldType.valueType match
                    case definition: SimpleObjectTypeDefinition[_] =>
                        SubObjectGetFieldsDescriptor(fieldName, Right(expandAllFieldsGetDescriptor(definition)))
                    case definition: RootPrimitiveTypeDefinition[_] =>
                        SingleGetFieldsDescriptor(fieldName)
                    case definition: TypeReferenceDefinition[_] =>
                        definition.referencedType.typeDefinition match
                            case _: ArrayTypeDefinition[_, _] =>
                                ListGetFieldsDescriptor(fieldName, None, None)
                            case _: CustomPrimitiveTypeDefinition[_, _, _] =>
                                SingleGetFieldsDescriptor(fieldName)
                            case objectDef: ObjectTypeDefinition[_, _] =>                                
                                SubObjectGetFieldsDescriptor(fieldName, Right(expandAllFieldsGetDescriptor(objectDef)))
                    case definition: TypeBackReferenceDefinition[_] =>
                        AllInReferenceGetFieldsDescriptor(fieldName)
                    case _ =>
                        throw new ConsistencyException(s"Field $fieldName type $fieldType is not supported!")
            }.toList

        val res: Option[Entity[ID1, _, _]] = entityType match
            case primType: CustomPrimitiveEntityType[ID1, _, _] =>
                val persData = primType.persistenceData
                val valueOpt =
                    SQL(s"""
                       SELECT ${esc(persData.valueColumn.columnName)}
                       FROM $typesSchemaName.${persData.tableName}
                       WHERE ${esc(persData.idColumn.columnName)} = ?
                    """)
                        .bind(id.value)
                        .map(rs => primType.typeDefinition.rootType.extract(rs, 1))
                        .single
                        .apply()
                        .flatten
                valueOpt.map(value => primType.createEntityRaw(id, value))
            case objectType: ObjectEntityType[ID1, _] =>
                val expandedDescriptors = checkAndExpandNotExpandedDescriptors(getFields, objectType.typeDefinition)
                val tableGetDescriptors = getAllObjectTablesGetDescriptors(objectType, expandedDescriptors, None, None)
                val tablesAliasesMap = tableGetDescriptors.zipWithIndex.map((tgd, rowIdx) => {
                    val tableAlias = tableAliasInQueryPrefix + tgd.tableName + rowIdx
                    (tgd.referer, tgd.tableName) -> tableAlias
                }).toMap
                //Option[String] contains subtype name when field is ID of one of some subtypes referenced by to parent type
                //None - for any value type
                val fieldsMap = mutable.HashMap[(GetDescriptorChainCell, Option[String]), (Int, FieldValueTypeDefinition[_] | EntityIdTypeDefinition[_])]()
                var currFieldIdx = 0
                val (tableLines, columnsLines, refData) = tableGetDescriptors.foldLeft((Nil, Nil, Nil)) (
                    (
                        acc: (List[String], List[String], List[GetFieldData]),
                        tgd: TableGetDescriptor
                    ) =>
                        val (tablesList, columnsList, backRefFieldsList) = acc
                        val tableAlias = getTableAliace(tablesAliasesMap, tgd)
                        currFieldIdx += 1
                        val idDscChainCell = GetDescriptorChainCell(SingleGetFieldsDescriptor(entityIdFieldNameForDsc), tgd.parentDsc)
                        fieldsMap += (idDscChainCell, Some(tgd.realObjectTypeName)) -> (currFieldIdx, tgd.idType)
                        val tableLine = s"$typesSchemaName.${esc(tgd.tableName)} as $tableAlias" + tgd.referer.map(ref =>
                                val refAlias = getTableAliace(tablesAliasesMap, ref)
                                s" on $refAlias.${ref.columnName} = $tableAlias.${tgd.idColumn}"
                            ).getOrElse("")
                        val (updatedColumnsList, updatedBackRefFieldsList) = tgd.fields.foldLeft((
                            s"$tableAlias.${tgd.idColumn}" :: columnsList,
                            backRefFieldsList
                        ))(
                            (
                                acc: (List[String], List[GetFieldData]),
                                gfd: GetFieldData
                            ) =>
                                val (columnsList, backRefFieldsList) = acc
                                gfd.fieldTypeDefinition match
                                    case fieldTypeDef: FieldTypeDefinition[_] =>
                                        (fieldTypeDef.valueType, fieldTypeDef.persistenceData) match
                                            case (fieldTypeDef: ItemValueTypeDefinition[_], Some(persistenceData: OneValuePersistenceDataFinal)) =>
                                                currFieldIdx += 1
                                                fieldsMap += (gfd.getDescriptorChainCell, None) -> (currFieldIdx, fieldTypeDef)
                                                (s"$tableAlias.${esc(persistenceData.columnName)}" :: columnsList, backRefFieldsList)
                                            case (_: TypeBackReferenceDefinition[_], None) =>
                                                (columnsList, gfd :: backRefFieldsList)
                                            case (_, persistanceData) => throw new ConsistencyException(s"Unknown combination of field " +
                                                s"descriptor ${gfd.getDescriptorChainCell} and field persistence data $persistanceData is not supported!")

                                    case primitiveEntityType: AbstractPrimitiveEntityType[_, _, _] =>
                                        currFieldIdx += 1
                                        fieldsMap += (gfd.getDescriptorChainCell, None) -> (currFieldIdx, primitiveEntityType.typeDefinition.rootType.asInstanceOf[FieldValueTypeDefinition[_]])
                                        (s"$tableAlias.${esc(primitiveEntityType.persistenceData.valueColumn.columnName)}" :: columnsList, backRefFieldsList)
                        )
                        (
                            tableLine :: tablesList,
                            updatedColumnsList,
                            updatedBackRefFieldsList
                        )
                )
                val firstTableAlias = getTableAliace(tablesAliasesMap, tableGetDescriptors.head)
                SQL(s"""
                   SELECT ${getListLineReversed(columnsLines, ", ")}
                   FROM ${getListLineReversed(tableLines, " LEFT JOIN ")}
                   WHERE $firstTableAlias.${esc(objectType.persistenceData.idColumn.columnName)} = ?
                """)
                    .bind(id.value)
                    .map(rs => extractObject(objectType, expandedDescriptors, None, rs, fieldsMap.toMap))
                    .single
                    .apply()
                    .flatten
            case ArrayEntityType(typeName, _) =>
                //values: Seq[ItemValue],
                //           persData: ArrayTypePersistenceDataFinal
                None
            case _ => throw new ConsistencyException(s"Entity type $entityType is not known!")
        res

    def find(entityType: EntityType[_, _, _], query: SearchCondition, getFields: ObjectGetFieldsDescriptor)(implicit session: DBSession): Vector[Entity[_, _, _]] = ???
    
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

    private val generateRandomUUIDFunc = "public.uuid_generate_v4()"

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
        "UPDATE", "VACUUM", "VALUES", "VIEW", "SYSTEM_USER")

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
    private val primitiveTypeValueFieldNameForDsc = "value"
    private val entityIdFieldNameForDsc = "id"

    private var typesPersistenceData: Map[AbstractEntityType[_, _, _], TypePersistenceData] = Map()

    private case class GetDescriptorChainCell(currDsc: NestedGetFieldsDescriptor, parentDsc: Option[GetDescriptorChainCell] = None):
        lazy val fieldName: String = parentDsc.map(_.fieldName + subobjectFieldsDelimiter).getOrElse("") + currDsc.fieldName
        lazy val asParentPrefix: String = fieldName + subobjectFieldsDelimiter

    private case class GetFieldData(getDescriptorChainCell: GetDescriptorChainCell, fieldTypeDefinition: FieldTypeDefinition[_] | AbstractPrimitiveEntityType[_, _, _])

    private def getAllObjectTables(
                                      objectType: AbstractObjectEntityType[_, _],
                                      persistanceData: ObjectTypePersistenceDataFinal
                                  ): List[(String, String)] =
                val parentPersistenceData = objectType.typeDefinition.parent.map((parentType: ObjectEntitySuperType[_, _]) =>
                    (parentType, getEntityPersistendeData(parentType).asInstanceOf[ObjectTypePersistenceDataFinal]))
                (persistanceData.tableName, persistanceData.idColumn.columnName) :: (
                    parentPersistenceData match
                        case None =>
                            Nil
                        case Some((parentType, parentPersistenceData)) =>
                            getAllObjectTables(parentType, parentPersistenceData)
                    )

    private case class TableGetDescriptor(
        tableName: String,
        idColumn: String,
        idType: EntityIdTypeDefinition[_],
        parentDsc: Option[GetDescriptorChainCell],
        referer: Option[RefererTableData],
        fields: List[GetFieldData],
        realObjectTypeName: String, 
    ) extends ReferredTable


    private def getAllObjectTablesGetDescriptors(
                                                    objectType: AbstractObjectEntityType[_, _],
                                                    fieldsDescriptors: List[NestedGetFieldsDescriptor],
                                                    referer: Option[RefererTableData],
                                                    referrersPrefix: Option[GetDescriptorChainCell],
                                                    ignoreParentTypes: Option[mutable.Set[ObjectEntitySuperType[_, _]]] = None
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
        val objectTypeDef: ObjectTypeDefinition[_, _] = objectType.typeDefinition
        val refererPart: RefererTablePartialData = RefererTablePartialData(persistenceData.tableName, referer)
        val (
            currTableGFDs: List[GetFieldData],
            parentsGFDs: List[NestedGetFieldsDescriptor],
            nestedTablesDescriptors: List[TableGetDescriptor]
        ) = parseFieldsDescriptiors(fieldsDescriptors, objectTypeDef, refererPart, referrersPrefix, objectType.name)

        TableGetDescriptor(persistenceData.tableName, persistenceData.idColumn.columnName, objectType.typeDefinition.idType,
            referrersPrefix, referer, currTableGFDs, objectType.name) :: (
            nestedTablesDescriptors ++ (
                objectTypeDef.parent match
                    case None =>
                        if (parentsGFDs.nonEmpty)
                            throw new ConsistencyException(s"Fields ${parentsGFDs.map(_.fieldName).mkString(", ")} are not found in " +
                                s"top supertype ${objectType.name}!")
                        Nil
                    case Some(parentType) =>
                        if (ignoreParentTypes.exists(_.contains(parentType)))
                            Nil
                        else
                            ignoreParentTypes.foreach(_.add(parentType))
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
                              ): (List[GetFieldData], List[TableGetDescriptor]) =
        val (
            sameTableData: List[GetFieldData],
            parentGFDs: List[NestedGetFieldsDescriptor],
            nestedTDs: List[TableGetDescriptor]
        ) =
            parseFieldsDescriptiors(fieldsDescriptors, simpleObjectTypeDef, referer, Some(parentsPrefix), currTypeName)

        
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
                    currParentType = currParentType.typeDefinition.parent.getOrElse(
                        throw new ConsistencyException(s"$presentParentGFDsLine, but there is no parents with referenced table name in $simpleObjectTypeDef!"))
                    currParentTableName = currParentType.persistenceData.tableName
                getAllObjectTablesGetDescriptors(currParentType, parentGFDs, Some(referer.toRefererTableData(parentPersistenceReferenceData.columnName)), Some(parentsPrefix))
            else
                Nil

        (sameTableData, soParentTableDescripros ++ nestedTDs)

    private def parseFieldsDescriptiors(
                                           fieldsDescriptors: List[NestedGetFieldsDescriptor],
                                           objectTypeDef: FieldsContainer,
                                           refererPart: RefererTablePartialData,
                                           referrersPrefix: Option[GetDescriptorChainCell],
                                           currTypeName: String
                                       ): (List[GetFieldData], List[NestedGetFieldsDescriptor], List[TableGetDescriptor]) =

        fieldsDescriptors.foldLeft((Nil, Nil, Nil))((
                                                        acc: (List[GetFieldData], List[NestedGetFieldsDescriptor], List[TableGetDescriptor]),
                                                        gfd: NestedGetFieldsDescriptor
                                                    ) =>
            val (
                sameTableData: List[GetFieldData],
                parentGFDs: List[NestedGetFieldsDescriptor],
                nestedTDs: List[TableGetDescriptor]
                ) = acc
            val currFieldChainCell = GetDescriptorChainCell(gfd, referrersPrefix)
            objectTypeDef.fields.get(gfd.fieldName) match
                case Some(fieldTypeDef) => (fieldTypeDef.persistenceData, fieldTypeDef.valueType) match
                    case (None, typeDef: TypeBackReferenceDefinition[_]) =>
                        (GetFieldData(currFieldChainCell, fieldTypeDef) :: sameTableData, parentGFDs, nestedTDs)
                    case (Some(primitiveValueFieldPersistenceData: PrimitiveValuePersistenceDataFinal), typeDef: RootPrimitiveTypeDefinition[_]) =>
                        (GetFieldData(currFieldChainCell, fieldTypeDef) :: sameTableData, parentGFDs, nestedTDs)
                    case (Some(simpleObjectPersistenceData: SimpleObjectValuePersistenceDataFinal), typeDef: SimpleObjectTypeDefinition[_]) =>
                        gfd match
                            case SubObjectGetFieldsDescriptor(_, Right(fieldsDescriptors)) =>
                                val soSubfieldsData = getSimpleObjectTablesGetDescriptors(currFieldChainCell,
                                    simpleObjectPersistenceData, typeDef, fieldsDescriptors, refererPart, currTypeName)
                                val soParentSubtypesTgds = typeDef.parent.map((parentType: ObjectEntitySuperType[_, _]) =>
                                    typesDefinitionsProvider.getAllLeafObjectsSubtypes(parentType).flatMap(subtype =>
                                        getAllObjectTablesGetDescriptors(subtype, fieldsDescriptors, refererPart.prevReferef, 
                                            Some(currFieldChainCell), Some(mutable.Set[ObjectEntitySuperType[_, _]]())))
                                ).getOrElse(Set())
                                (soSubfieldsData._1 ++ sameTableData, parentGFDs, soSubfieldsData._2 ++ soParentSubtypesTgds ++ nestedTDs)
                            case _ =>
                                throw new ConsistencyException(s"Get field descriptor $gfd is not supported for $fieldTypeDef and $typeDef!")
                    case (Some(referenceValueFieldPersistenceData: ReferenceValuePersistenceDataFinal), typeDef: TypeReferenceDefinition[_]) =>
                        val nextReferer = Some(refererPart.toRefererTableData(referenceValueFieldPersistenceData.columnName))
                        val referencedType: AbstractEntityType[_, _, _] = typeDef.referencedType
                        val refererGDF = GetFieldData(currFieldChainCell, fieldTypeDef)
                        (gfd, referencedType) match
                            case (SubObjectGetFieldsDescriptor(_, Right(fieldsDescriptors)), objectType: ObjectEntityType[_, _]) =>
                                val tablesGetDefinitions = getAllObjectTablesGetDescriptors(objectType, fieldsDescriptors, nextReferer, Some(currFieldChainCell))
                                (refererGDF :: sameTableData, parentGFDs, tablesGetDefinitions ++ nestedTDs)
                            case (SubObjectGetFieldsDescriptor(_, Right(fieldsDescriptors)), objectType: ObjectEntitySuperType[_, _]) =>
                                val processedParentsSet = Some(mutable.Set[ObjectEntitySuperType[_, _]]())
                                val subTypesTablesGetDefinitions = typesDefinitionsProvider.getAllLeafObjectsSubtypes(objectType).flatMap(subtype =>
                                        getAllObjectTablesGetDescriptors(subtype, fieldsDescriptors, nextReferer, Some(currFieldChainCell), processedParentsSet))
                                (refererGDF :: sameTableData, parentGFDs, subTypesTablesGetDefinitions.toList ++ nestedTDs)
                            case (SingleGetFieldsDescriptor(_), primitiveType: AbstractPrimitiveEntityType[_, _, _]) =>
                                def getPrimitiveTGDsWithChildren(primitiveType: AbstractPrimitiveEntityType[_, _, _]): List[TableGetDescriptor] =
                                    val childrenTGDs = primitiveType match
                                        case leafPrimitiveType: CustomPrimitiveEntityType[_, _, _] => Nil
                                        case primitiveSuperType: PrimitiveEntitySuperType[_, _, _] =>
                                            primitiveSuperType.directChildren.flatMap(getPrimitiveTGDsWithChildren)
                                    val persistenceData = primitiveType.persistenceData
                                    TableGetDescriptor(persistenceData.tableName, persistenceData.idColumn.columnName,
                                        primitiveType.typeDefinition.idType, Some(currFieldChainCell), nextReferer,
                                        List(GetFieldData(GetDescriptorChainCell(SingleGetFieldsDescriptor(primitiveTypeValueFieldNameForDsc),
                                            Some(currFieldChainCell)), primitiveType)), primitiveType.name) :: childrenTGDs
                                (refererGDF :: sameTableData, parentGFDs, getPrimitiveTGDsWithChildren(primitiveType) ++ nestedTDs)
                            case _ =>
                                (GetFieldData(currFieldChainCell, fieldTypeDef) :: sameTableData, parentGFDs, nestedTDs)
                    case _ =>
                        throw new ConsistencyException(s"Field persistence data $fieldTypeDef is not supported!")
                case None =>
                    if (objectTypeDef.allFields.contains(gfd.fieldName))
                        (sameTableData, gfd :: parentGFDs, nestedTDs)
                    else
                        throw new ConsistencyException(s"Field ${gfd.fieldName} is not found in object type $currTypeName!")
        )

    private def getIdValueMapper(idType: PersistenceFieldType, pos: Int): WrappedResultSet => EntityId[_, _] =
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

    private def getIdValueMapper(idColumn: PrimitiveValuePersistenceDataFinal): WrappedResultSet => EntityId[_, _] =
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
                            (List((fieldPersData.columnName, value.refId.value)), Nil)
                        case (SimpleObjectValue(idAndParentType, subFields, _), SimpleObjectValuePersistenceDataFinal(parentPersOpt, fieldsPers)) =>
                            val parentData =
                                idAndParentType.map { idAndParentType =>
                                    val parentPers = parentPersOpt.getOrElse(throw new ConsistencyException("Parent is not defined!"))
                                    (parentPers.columnName, idAndParentType._1)
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
        val (columnsValues, parentFields) = getColumnsValuesAndRestFields(filedValues, fieldsPersistenceData)
        if (parentFields.isEmpty)
            List((tableName, idColumnName, columnsValues, level))
        else
            parent match
                case None =>
                    throw new ConsistencyException(s"Fields ${parentFields.map(_._1).mkString(", ")} are not found in " +
                        s"type $typeName and there is no parent of those type!")
                case Some(parent) =>
                    getEntityPersistendeData(parent) match
                        case ObjectTypePersistenceDataFinal(parentTableName, idColumn, fields, _) =>
                            List((tableName, idColumnName, columnsValues, level)) ++
                                getColumnsValues(parentFields, fields, parent.typeDefinition.parent, parentTableName,
                                    idColumn, typeName, (level + 1).toByte)
                        case _ => throw new ConsistencyException("Parent is not Object!")


    private def splitValuesByTypes(
                                      values: Seq[ItemValue],
                                      persData: ArrayTypePersistenceDataFinal
                                  ): Map[ItemTypePersistenceDataFinal, Seq[Any]] =
        values.map {
            case pv: RootPrimitiveValue[_] => (
                persData.itemsMap.getOrElse(typesMapper.getValueFieldType(pv.valueType.typeDefinition),
                    throw new ConsistencyException(s"Item value type is not found! ${pv.valueType.typeDefinition}")),
                pv.value
            )
            case rv: ReferenceValue[_] => (
                persData.itemsMap.getOrElse(typesMapper.getIdFieldType(rv.valueType.typeDefinition.idType),
                    throw new ConsistencyException(s"Item id type is not found! ${rv.valueType.typeDefinition.idType}")),
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
            SQL(
                s"""CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA $typesSchemaName"""
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
                s"${esc(columnName)} $columnType ${if isNullable then "" else "NOT NULL"}, "
            }.mkString("")
            val idColumnType = getIdFieldType(idColumn.columnType)
            val idAutogenerator = getIdAutoGenerator(idColumn.columnType)
            val idAutogeneratorSql = if idAutogenerator.isEmpty then "" else s" DEFAULT $idAutogenerator"

            SQL(s"""
                CREATE TABLE $typesSchemaName.${esc(tableName)} (
                    ${esc(idColumn.columnName)} $idColumnType NOT NULL $idAutogeneratorSql,
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


