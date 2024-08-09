package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.{AbstractEntityType, AbstractNamedEntityType, ArrayEntitySuperType, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DoubleTypeDefinition, EntityFieldTypeDefinition, EntityIdTypeDefinition, EntityType, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectEntitySuperType, ObjectTypeDefinition, PrimitiveEntitySuperType, PrimitiveFieldTypeDefinition, RootPrimitiveTypeDefinition, SimpleEntityType, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeReferenceDefinition, TypesDefinitionsParseError, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, NoTypeFound, TypesLoadExceptionException}

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader


sealed trait ValuePersistenceDataFinal:
    def columnNames: Seq[String]

class SimpleValuePersistenceDataFinal(
    val columnName: String,
    val columnType: FieldType,
) extends ValuePersistenceDataFinal:
    override def columnNames: Seq[String] = Seq(columnName)

class ReferenceValuePersistenceDataFinal(
    val columnName: String,
    referenceTable: TableReference,
) extends ValuePersistenceDataFinal:
    override def columnNames: Seq[String] = Seq(columnName)

class SimpleObjectValuePersistenceDataFinal(
    val parent: Option[ReferenceValuePersistenceDataFinal],
    val fields: Map[String, ValuePersistenceDataFinal],
) extends ValuePersistenceDataFinal:
    override def columnNames: Seq[String] = fields.values.flatMap(_.columnNames).toSeq

trait TableReference:
    def referenceTable: String

class TableReferenceFactory:
    class TableReferenceProxy(name: Option[String]) extends TableReference:
        var _name: Option[String] = name
        def get: String = _name.getOrElse(throw new RuntimeException("Reference table name is not set!"))
    private val instancesMap: mutable.Map[String, TableReferenceProxy] = mutable.Map()
    def createForTable(name: String): TableReferenceProxy = TableReferenceProxy(Some(name))
    def createForType(typeName: String): TableReferenceProxy =
        instancesMap.getOrElseUpdate(typeName, TableReferenceProxy(None))
    def initForType(typeName: String, tableName: String): Unit =
        instancesMap.get(typeName).exists( instance =>
            instance._name = Some(tableName)
            true
        )

trait TypePersistenceDataFinal

class PrimitiveTypePersistenceDataFinal(
    val tableName: String,
    val idColumn: SimpleValuePersistenceDataFinal,
    val valueColumn: SimpleValuePersistenceDataFinal,
) extends TypePersistenceDataFinal

class ItemTypePersistenceDataFinal(
    val tableName: String,
    val idColumn: SimpleValuePersistenceDataFinal,
    val valueColumn: ValuePersistenceDataFinal,
) extends TypePersistenceDataFinal

class ArrayTypePersistenceDataFinal(
    val data: Seq[ItemTypePersistenceDataFinal],
) extends TypePersistenceDataFinal

class ObjectTypePersistenceDataFinal(
    val tableName: String,
    val idColumn: SimpleValuePersistenceDataFinal,
    val fields: Map[String, ValuePersistenceDataFinal],
    val parent: Option[ReferenceValuePersistenceDataFinal],
) extends TypePersistenceDataFinal




trait PersistenceConfigLoader:
    def load(dataResourcePath: String, 
             typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, AbstractTypePersistenceData]


val primitiveTypeDefinitionToFieldType: Map[RootPrimitiveTypeDefinition, FieldType] = Map(
    LongTypeDefinition -> LongFieldType,
    IntTypeDefinition -> IntFieldType,
    StringTypeDefinition -> StringFieldType(0),
    DoubleTypeDefinition -> DoubleFieldType,
    FloatTypeDefinition -> FloatFieldType,
    BooleanTypeDefinition -> BooleanFieldType,
    DateTypeDefinition -> DateFieldType,
    DateTimeTypeDefinition -> DateTimeFieldType,
    TimeTypeDefinition -> TimeFieldType,
    UUIDTypeDefinition -> UUIDFieldType,
    BinaryTypeDefinition -> BLOBFieldType)

val idTypeDefinitionToFieldType: Map[EntityIdTypeDefinition, FieldType] = Map(
    LongIdTypeDefinition -> LongFieldType,
    IntIdTypeDefinition -> IntFieldType,
    StringIdTypeDefinition -> StringFieldType(0),
    UUIDIdTypeDefinition -> UUIDFieldType)

val consistencePersistenceTypesMap: Map[RootPrimitiveTypeDefinition, Set[FieldType]] = Map(
    LongTypeDefinition -> Set(LongFieldType),
    IntTypeDefinition -> Set(IntFieldType, LongFieldType),
    StringTypeDefinition -> Set(StringFieldType(0)),
    DoubleTypeDefinition -> Set(DoubleFieldType),
    FloatTypeDefinition -> Set(FloatFieldType, DoubleFieldType),
    BooleanTypeDefinition -> Set(BooleanFieldType),
    DateTypeDefinition -> Set(DateFieldType, DateTimeFieldType),
    DateTimeTypeDefinition -> Set(DateTimeFieldType),
    TimeTypeDefinition -> Set(TimeFieldType),
    UUIDTypeDefinition -> Set(UUIDFieldType),
    BinaryTypeDefinition -> Set(BLOBFieldType)
)


object PersistenceConfigLoader extends PersistenceConfigLoader:

    var objectsSuperTypesPersistenceData: Map[String, ObjectTypePersistenceDataFinal] = Map.empty
    val tableReferenceFactory = new TableReferenceFactory()
    var typesDataPersistenceMap: Map[String, AbstractTypePersistenceData] = Map.empty
        
    override def load(dataResourcePath: String, 
                      typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, TypePersistenceDataFinal] =
        val tdSource = Source.fromResource (dataResourcePath)
        PersistenceConfigParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: RootPackagePersistenceData) =>
                typesDataPersistenceMap = packageData.toMap
                typesDefinitionsMap
                    .map ( (typeName, typeDef) =>
                            typeName -> mergeTypePersistenceData(typeName, typeDef, typesDataPersistenceMap.get(typeName))
                    )
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)

    private def mergeTypePersistenceData(typeName: String,
                                         typeDef: AbstractNamedEntityType,
                                         parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =
        typeDef match
            case EntityType(name, valueType) => mergeEntityPersistenceData(typeName, name, valueType, parsed)
            case ObjectEntitySuperType(_, valueType) => mergeObjectSuperTypePersistenceData(typeName, valueType, parsed)
            case ArrayEntitySuperType(name, elementTypes) => mergeArraySuperTypePersistenceData(typeName, elementTypes, parsed)
            case PrimitiveEntitySuperType(name, parentTypeName) => mergePrimitiveSuperTypePersistenceData(typeName, parentTypeName, parsed)

    private def mergeEntityPersistenceData(typeName: String,
                                           name: String,
                                           valueType: EntityFieldTypeDefinition,
                                           parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =
        valueType match
            case td: PrimitiveFieldTypeDefinition =>
                val parsedData = parsed.map(p => p.asInstanceOf[PrimitiveTypePersistenceData])
                    .getOrElse(PrimitiveTypePersistenceData(typeName, None, None, None))

                val valueColumnData = parsedData.valueColumn.getOrElse(SimpleValuePersistenceData(None, None))
                PrimitiveTypePersistenceDataFinal(
                    parsedData.tableName.getOrElse( tableNameFromTypeName(typeName) ),
                    mergeIdTypeDefinition(valueType.idType, parsedData.idColumn),
                    SimpleValuePersistenceDataFinal(
                        valueColumnData.columnName.getOrElse("value"),
                        valueColumnData.columnType.getOrElse(
                            primitiveTypeDefinitionToFieldType.getOrElse(td.rootType, throw new NoTypeFound(name)) )
                    ))
            case ObjectTypeDefinition(fields, parent) =>
                val parsedData = parsed.map(p => p.asInstanceOf[ObjectTypePersistenceData])
                    .getOrElse(ObjectTypePersistenceData(typeName, None, None, Map.empty, Right(ReferenceValuePersistenceData(None, None))))

                val parent = None

                val valueColumns = fields.map {
                    case (fieldName, fieldType) =>
                        SimpleValuePersistenceData(
                            Some(columnNameFromFieldName(fieldName)),
                            Some(primitiveTypeDefinitionToFieldType.getOrElse(fieldType, throw new NoTypeFound(fieldType.name)))
                        )
                }

                ObjectTypePersistenceDataFinal(
                    tableNameFromTypeName(typeName),
                    mergeIdTypeDefinition(valueType.idType, parsedData.idColumn),
                    valueColumns,
                    parent
                )
            case _ =>
                throw new TypesLoadExceptionException(s"Type $typeName not supported")

    private class ColumnsNamesChecker:
        val columnsUsages: mutable.Map[String, String] = mutable.Map()
        def addAndCheckUsage(columnName: String, columnUsage: String): Unit =
            if (columnsUsages.contains(columnName)) {
                throw new ConsistencyException(s"Column name $columnName is already used for ${columnsUsages(columnName)}, but also find usage for $columnUsage!")
            }
            columnsUsages(columnName) = fieldName


    private def mergeObjectSuperTypePersistenceData(typeName: String,
                                                    valueType: ObjectTypeDefinition,
                                                    parsed: Option[AbstractTypePersistenceData]): ObjectTypePersistenceDataFinal =

        val parsedData = parsed
            .map(p =>
                if (!p.isInstanceOf[ObjectTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type $typeName is not ObjectTypePersistenceData type!")
                p.asInstanceOf[ObjectTypePersistenceData]
            )
            .getOrElse(ObjectTypePersistenceData(typeName, None, None, Map.empty, Right(ReferenceValuePersistenceData(None, None))))

        val tableName = parsedData.tableName.getOrElse(tableNameFromTypeName(typeName))
        val idColumnSrc = parsedData.idColumn
            .getOrElse(SimpleValuePersistenceData(None, None))
        val idColumn = SimpleValuePersistenceDataFinal(
            idColumnSrc.columnName.getOrElse("id"),
            idColumnSrc.columnType.getOrElse(idTypeDefinitionToFieldType.getOrElse(valueType.idType, throw new NoTypeFound(valueType.idType.name)))
        )
        val parentPersistenceData = parentPersitanceDataToFinal(typeName, valueType, parsedData.parentRelation)
        val fieldsFinal = mergeFieldsPersistenceData(valueType.fields, parsedData.fields, typeName)

        val superFieldsFinal = valueType.parent match
            case Some(parentDef) =>
                parsedData.parentRelation match
                    case Left(ExpandParentMarkerTotal) => copyParentFieldsPersitenceData("", parentDef, true)
                    case Left(_) => copyParentFieldsPersitenceData("", parentDef, false)
                    case Right(_) => Map.empty
            case None =>
                Map.empty

        val columnsNamesChecker = ColumnsNamesChecker()
        columnsNamesChecker.addAndCheckUsage(idColumn.columnName, "ID column")
        parentPersistenceData.foreach(p => columnsNamesChecker.addAndCheckUsage(p.columnName, "Parent column"))
        fieldsFinal.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Field " + fe._1) ) )
        superFieldsFinal.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Super field " + fe._1) ) )

        ObjectTypePersistenceDataFinal(tableName, idColumn, fieldsFinal ++ superFieldsFinal, parentPersistenceData)

    private def parentPersitanceDataToFinal(typeName: String,
                                            valueType: ObjectTypeDefinition,
                                            parentRelation: Either[ExpandParentMarker, ReferenceValuePersistenceData]): Option[ReferenceValuePersistenceDataFinal] =
        valueType.parent match
            case Some(parentDef) =>
                parentRelation match
                    case Right(ReferenceValuePersistenceData(columnName, referenceTable)) =>
                        Some(ReferenceValuePersistenceDataFinal(
                            columnName.getOrElse(columnNameFromFieldName("parent__" + parentDef.name)),
                            referenceTable.map(tableReferenceFactory.createForTable).getOrElse(tableReferenceFactory.createForType(parentDef.name))
                        ))
                    case Left(ExpandParentMarkerSingle) =>
                        parentDef.valueType.parent.map(superParentDef =>
                            val fieldsParsedData = typesDataPersistenceMap.get(superParentDef.name)
                                .flatMap(p => {
                                    if (!p.isInstanceOf[ObjectTypePersistenceData]) {
                                        throw new ConsistencyException(s"Persistence data for type ${superParentDef.name} is not ObjectTypePersistenceData type!")
                                    }
                                    p.asInstanceOf[ObjectTypePersistenceData].parentRelation.toOption
                                })
                                .getOrElse(ReferenceValuePersistenceData(None, None))
                            ReferenceValuePersistenceDataFinal(
                                fieldsParsedData.columnName.getOrElse(columnNameFromFieldName("super_parent__" + superParentDef.name)),
                                fieldsParsedData.referenceTable.map(tableReferenceFactory.createForTable).getOrElse(tableReferenceFactory.createForType(superParentDef.name))
                            ))
                    case _ =>
                        None
            case None =>
                parentRelation match
                    case Right(ReferenceValuePersistenceData(columnName, referenceTable)) =>
                        if (referenceTable.isDefined) {
                            throw new ConsistencyException(s"Type $typeName has no super type, but there is reference table defined!")
                        }
                        if (columnName.isDefined) {
                            throw new ConsistencyException(s"Type $typeName has no super type, but there is reference column defined!")
                        }
                        None
                    case _ =>
                        None



    private def getOrCreatePrimitivePersistenceData(columnType: Option[FieldType],
                                                    valueType: SimpleTypeDefinitions,
                                                    fieldName: String,
                                                    typeName: String ): FieldType =
        columnType.getOrElse(valueType match
            case fieldType: RootPrimitiveTypeDefinition =>
                val rootFieldType = fieldType.rootType
                primitiveTypeDefinitionToFieldType.getOrElse(rootFieldType,
                    throw new NoTypeFound(rootFieldType.name))
            case _ =>
                throw new ConsistencyException(s"Field $fieldName of type $typeName with simple type " +
                    s"$fieldType parsed as primitive type whereas field type is not RootPrimitiveTypeDefinition!")
        )

        
    private def checkRootPrimitiveAndPersistenceTypeConsistency(fieldType: RootPrimitiveTypeDefinition,
                                                                fieldPersistType: SimpleValuePersistenceData,
                                                                fieldName: String,
                                                                typeName: String): Unit =
        val consistentPersistTypes = consistencePersistenceTypesMap.getOrElse(fieldType,
            throw new NoTypeFound(fieldType.name))
        if (!fieldPersistType.columnType.exists(consistentPersistTypes.contains)) {
            throw new ConsistencyException(s"Field $fieldName of type $typeName defined " +
                s"with inconsistent persistence type ${fieldPersistType.columnType} where field type is ${fieldType.name}!")
        }
                
    private def mergeIdTypeDefinition(idType: EntityIdTypeDefinition, 
                                      parsed: Option[SimpleValuePersistenceData]): SimpleValuePersistenceDataFinal =
        val data = parsed
            .map(p => (p.columnName, p.columnType))
            .getOrElse((None, None))
        SimpleValuePersistenceDataFinal(
            data._1.getOrElse("id"),
            data._2.getOrElse( idTypeDefinitionToFieldType.getOrElse(idType, throw new NoTypeFound(idType.name)) )
        )

    private def copyParentFieldsPersitenceData(prefix: String,
                                               typeDef: ObjectEntitySuperType,
                                               copySuperParents: Boolean): Map[String, ValuePersistenceDataFinal] =
        val prefix_ = prefix + s"parent__${typeDef.name}__"

        val fieldsParsedData = typesDataPersistenceMap.get(typeDef.name)
            .map(p => {
                if (!p.isInstanceOf[ObjectTypePersistenceData]) {
                    throw new ConsistencyException(s"Persistence data for type ${typeDef.name} is not ObjectTypePersistenceData type!")
                }
                p.asInstanceOf[ObjectTypePersistenceData].fields
            })
            .getOrElse(Map.empty)

        val parentFieldsData = mergeFieldsPersistenceData(typeDef.valueType.fields, fieldsParsedData, typeDef.name, prefix_)

        val superParentFieldsData = typeDef.valueType.parent.map(superParentType =>
            if (copySuperParents) {
                copyParentFieldsPersitenceData(prefix_, superParentType, copySuperParents)
            } else {
                Map.empty
            }
        ).getOrElse(Map.empty)

        val columnsNamesChecker = ColumnsNamesChecker()
        parentFieldsData.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Field " + fe._1)))
        superParentFieldsData.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Super field " + fe._1)))

        parentFieldsData ++ superParentFieldsData

    private def mergeFieldsPersistenceData(fields: Map[String, SimpleEntityType],
                                           fieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                           typeName: String,
                                           prefix: String = "" ): Map[String, ValuePersistenceDataFinal] =
        fields.map( (fieldName, fieldType) =>
            val parsedFieldData = getOrCreateDefaultsAndCheck(fieldName, fieldType, fieldsPersistenceDataMap, typeName)
            val columnData: ValuePersistenceDataFinal = toValuePersistenceDataFinal(parsedFieldData, fieldName,
                fieldType, typeName, prefix)
            fieldName -> columnData
        )

    private def getOrCreateDefaultsAndCheck(fieldName: String,
                                            fieldType: SimpleEntityType,
                                            fieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                            typeName: String ): ValuePersistenceData =
        fieldsPersistenceDataMap
            .get(fieldName)
            .map(persistData => {
                fieldType.valueType match
                    case fieldType: RootPrimitiveTypeDefinition =>
                        persistData match
                            case fieldPersistType: SimpleValuePersistenceData =>
                                checkRootPrimitiveAndPersistenceTypeConsistency(fieldType, fieldPersistType,
                                    fieldName, typeName)
                            case _ =>
                                throw new ConsistencyException(s"Field $fieldName of type $typeName with " +
                                    s"simple type $fieldType defined in persistence as not simpe type!")
                    case ObjectTypeDefinition(_, _) =>
                        persistData match
                            case fieldPersistType: SimpleValuePersistenceData =>
                                throw new ConsistencyException(s"Field $fieldName of type $typeName with " +
                                    s"object type $fieldPersistType defined in persistence as primitive type!")
                            case _ => // do nothing
                    case TypeReferenceDefinition(referencedType, _) =>
                        referencedType match
                            case EntityType(_, valueType: PrimitiveFieldTypeDefinition) =>
                                persistData match
                                    case fieldPersistType: SimpleValuePersistenceData =>
                                        checkRootPrimitiveAndPersistenceTypeConsistency(valueType.rootType,
                                            fieldPersistType, fieldName, typeName)
                                    case _ => // do nothing
                            case PrimitiveEntitySuperType(_, valueType) =>
                                persistData match
                                    case fieldPersistType: SimpleValuePersistenceData =>
                                        checkRootPrimitiveAndPersistenceTypeConsistency(valueType.rootType,
                                            fieldPersistType, fieldName, typeName)
                                    case _ => // do nothing
                            case _ =>
                                persistData match
                                    case fieldPersistType: ReferenceValuePersistenceData =>// do nothing
                                    case _ =>
                                        throw new ConsistencyException(s"Field $fieldName of type $typeName with " +
                                            s"object type reference defined in persistence as not reference type!")
                    case _ =>
                        throw new ConsistencyException(s"Field $fieldName of type $typeName has not expected " +
                            s"field type $fieldType!")

                persistData
            })
            .getOrElse(fieldType.valueType match
                case fieldType: RootPrimitiveTypeDefinition =>
                    SimpleValuePersistenceData(None, None)
                case ObjectTypeDefinition(_, _) =>
                    SimpleObjectValuePersistenceData(Right(ReferenceValuePersistenceData(None, None)), Map.empty)
                case TypeReferenceDefinition(_, _) =>
                    ReferenceValuePersistenceData(None, None)
            )

    private def toValuePersistenceDataFinal(parsedFieldData: ValuePersistenceData,
                                            fieldName: String,
                                            fieldType: SimpleEntityType,
                                            typeName: String,
                                            prefix: String = ""): ValuePersistenceDataFinal =
        val columnName = prefix + parsedFieldData.columnName.getOrElse(columnNameFromFieldName(fieldName))
        parsedFieldData match
            case SimpleValuePersistenceData(_, columnType) =>
                val persisType = columnType.getOrElse(fieldType.valueType match
                    case fieldType: RootPrimitiveTypeDefinition =>
                        val rootFieldType = fieldType.rootType
                        primitiveTypeDefinitionToFieldType.getOrElse(rootFieldType,
                            throw new NoTypeFound(rootFieldType.name))
                    case _ =>
                        throw new ConsistencyException(s"Field $fieldName of type $typeName with simple type " +
                            s"$fieldType parsed as primitive type whereas field type is not RootPrimitiveTypeDefinition!")
                )
                SimpleValuePersistenceDataFinal(columnName, persisType)

            case ReferenceValuePersistenceData(_, referenceTable) =>
                fieldType.valueType match
                    case TypeReferenceDefinition(referencedType, _) =>
                        ReferenceValuePersistenceDataFinal(
                            columnName,
                            referenceTable.map(tableReferenceFactory.createForTable).getOrElse(tableReferenceFactory.createForType(referencedType.name))
                        )
                    case _ =>
                        throw new ConsistencyException(s"Field $fieldName of type $typeName with reference type " +
                            s"parsed as reference type whereas field type is not TypeReferenceDefinition!")

            case SimpleObjectValuePersistenceData(parent, subFieldsDataMap) =>
                fieldType.valueType match
                    case subObjectValueType: ObjectTypeDefinition =>
                        if (subFieldsDataMap.keys.exists(!subObjectValueType.fields.contains(_))) {
                            throw new ConsistencyException(s"Field $fieldName of type $typeName contains " +
                                s"persistent data for fields not defined in type definition!")
                        }
                        val fieldsDataFinal = mergeFieldsPersistenceData(subObjectValueType.fields, subFieldsDataMap, 
                            typeName, s"$prefix${fieldName}__")
                        val parentPersistenceData = parentPersitanceDataToFinal(s"$typeName's $fieldName field simple type'", 
                            subObjectValueType, parent)

                        if (parent.isLeft) {
                            //                    fieldsDataFinal ++= List(...)
                        }
                        
                        SimpleObjectValuePersistenceDataFinal(parentPersistenceData, fieldsDataFinal)

                    case _ => 
                        throw new ConsistencyException(s"Previous check shuld prevent this, but something went wrong!")


    private def mergeArraySuperTypePersistenceData(typeName: String,
                                                   elementTypes: ArrayTypeDefinition,
                                                   parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =
        val idColumn = createIdTypeDefinition(idType)
        val valueColumns = elementTypes.map {
            case RootPrimitiveTypeDefinition(typeName) =>
                SimpleValuePersistenceData(
                    Some("value"),
                    Some(primitiveTypeDefinitionToFieldType.getOrElse(typeName, throw new NoTypeFound(typeName)))
                )
            case _ =>
                throw new TypesLoadExceptionException(s"Type $typeName not supported")
        }
        Some(SimpleTypePersistenceData(typeName, Some(tableNameFromTypeName(typeName)), Some(idColumn), Some(valueColumns)))

    private def mergePrimitiveSuperTypePersistenceData(typeName: String,
                                                       parentTypeName: PrimitiveFieldTypeDefinition,
                                                       parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =
        None

                    
    private def tableNameFromTypeName(typeName: String): String = typeName.toLowerCase().replace(".", "_")
    private def columnNameFromFieldName(fieldName: String): String = camelCaseToSnakeCase(fieldName)
    private def camelCaseToSnakeCase(name: String): String = name.replaceAll("[A-Z]", "_$0").toLowerCase()
                
