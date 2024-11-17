package my.valerii_timakov.sgql.services

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.TypesDefinitionsParseError
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractNamedEntityType, ArrayEntitySuperType, ArrayItemTypeDefinition, ArrayItemValueTypeDefinitions, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, ByteIdTypeDefinition, ByteTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DecimalTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntityType, FieldTypeDefinition, FieldsContainer, FixedStringIdTypeDefinition, FixedStringTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectEntitySuperType, ObjectTypeDefinition, PrimitiveEntitySuperType, PrimitiveTypeDefinition, RootPrimitiveTypeDefinition, ShortIdTypeDefinition, ShortIntTypeDefinition, SimpleObjectTypeDefinition, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, NoTypeFound, NotDefinedOperationException, TypesLoadExceptionException}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader


sealed trait ValuePersistenceDataFinal:
    def columnNames: Seq[String]

case class PrimitiveValuePersistenceDataFinal(
    columnName: String,
    columnType: PersistenceFieldType,
    isNullable: Boolean,
) extends ValuePersistenceDataFinal:
    override def columnNames: Seq[String] = Seq(columnName)

class ReferenceValuePersistenceDataFinal(
    val columnName: String,
    referenceTable: TableReference,
    val isNullable: Boolean,
) extends ValuePersistenceDataFinal:
    override def columnNames: Seq[String] = Seq(columnName)
    def refTableData: TableReferenceDataWrapper = referenceTable.get
    override def toString: String = s"ReferenceValuePersistenceDataFinal(columnName=$columnName, referenceTable=${referenceTable.get})"
    
class ParentTableReferenceFinal(referenceTable: TableReference):
    def refTableData: TableReferenceDataWrapper = referenceTable.get
    def refTableWrapperCopy: TableReference = referenceTable.copy
    override def toString: String = s"ParentTableReferenceFinal(referenceTable=${referenceTable.get})"

case class SimpleObjectValuePersistenceDataFinal(
    parent: Option[ReferenceValuePersistenceDataFinal],
    fields: Map[String, ValuePersistenceDataFinal],
) extends ValuePersistenceDataFinal:
    def this(fieldsAndParentPersistenceData: FieldsAndParentPersistenceData) =
        this(fieldsAndParentPersistenceData.parent, fieldsAndParentPersistenceData.fields)
    override def columnNames: Seq[String] = fields.values.flatMap(_.columnNames).toSeq
    
object SimpleObjectValuePersistenceDataFinal:
    def apply(fieldsAndParentPersistenceData: FieldsAndParentPersistenceData) =
        new SimpleObjectValuePersistenceDataFinal(fieldsAndParentPersistenceData.parent, fieldsAndParentPersistenceData.fields)

case class TableReferenceData(
    tableName: String,
    idColumn: PrimitiveValuePersistenceDataFinal,
)

case class TableReferenceDataWrapper(
    data: Option[TableReferenceData],
    idColumnType: PersistenceFieldType
)
object TableReferenceDataWrapper:
    def apply(tableName: String, idColumn: PrimitiveValuePersistenceDataFinal, idColumnType: PersistenceFieldType) =
        new TableReferenceDataWrapper(Some(TableReferenceData(tableName, idColumn)), idColumnType)
    def apply(idColumnType: PersistenceFieldType) =
        new TableReferenceDataWrapper(None, idColumnType)

trait TableReference:
    def get: TableReferenceDataWrapper
    def copy: TableReference = TableReferenceInstance(get)
    
class TableReferenceInstance(data: TableReferenceDataWrapper) extends TableReference:
    override def get: TableReferenceDataWrapper = data

class TableReferenceFactory:
    class TableReferenceProxy(data: Option[TableReferenceDataWrapper]) extends TableReference:
        var _data: Option[TableReferenceDataWrapper] = data
        def get: TableReferenceDataWrapper = _data.getOrElse(throw new RuntimeException("Reference table name is not set!"))
    private val instancesMap: mutable.Map[String, TableReferenceProxy] = mutable.Map()
    private val tableNamesMap: mutable.Map[String, mutable.Set[TableReferenceDataWrapper]] = mutable.Map()
    def createForTable(typeName: String, data: TableReferenceDataWrapper): TableReferenceProxy = 
        tableNamesMap.getOrElseUpdate(typeName, mutable.Set.empty) += data
        TableReferenceProxy(Some(data))
    def createForType(typeName: String): TableReferenceProxy =
        instancesMap.getOrElseUpdate(typeName, TableReferenceProxy(None))
    def initForType(typeName: String, data: TableReferenceDataWrapper): Unit =
        tableNamesMap.get(typeName).foreach(tableNames => 
            val wrongNames = tableNames.filter(_ != data)
            if (wrongNames.nonEmpty) 
                throw new ConsistencyException(s"Table name $data of type $typeName has wrong table names for " +
                    s"reference to it! Names: $wrongNames")
            )
        instancesMap.get(typeName).exists( instance =>
            instance._data = Some(data)
            true
        )

trait TypePersistenceDataFinal:
    def getReferenceData: TableReferenceDataWrapper

case class PrimitiveTypePersistenceDataFinal(
    tableName: String,
    idColumn: PrimitiveValuePersistenceDataFinal,
    valueColumn: PrimitiveValuePersistenceDataFinal,
) extends TypePersistenceDataFinal:
    override def getReferenceData: TableReferenceDataWrapper = 
        TableReferenceDataWrapper(tableName, idColumn, idColumn.columnType)

case class ItemTypePersistenceDataFinal(
    tableName: String,
    idColumn: PrimitiveValuePersistenceDataFinal,
    valueColumn: PrimitiveValuePersistenceDataFinal | ReferenceValuePersistenceDataFinal,
) extends TypePersistenceDataFinal:
    override def getReferenceData: TableReferenceDataWrapper =
        TableReferenceDataWrapper(tableName, idColumn, idColumn.columnType)

case class ArrayTypePersistenceDataFinal(
    data: Set[ItemTypePersistenceDataFinal],
    idType: PersistenceFieldType,
) extends TypePersistenceDataFinal:
    if data.map(_.idColumn.columnType).exists(_ != idType)
        then throw new ConsistencyException(s"Array ID types has are different! Types: $idType")
    def getTableName: Option[String] = None
    override def getReferenceData: TableReferenceDataWrapper =
        TableReferenceDataWrapper(idType)

case class ObjectTypePersistenceDataFinal(
    tableName: String,
    idColumn: PrimitiveValuePersistenceDataFinal,
    fields: Map[String, ValuePersistenceDataFinal],
    parent: Option[ParentTableReferenceFinal],
) extends TypePersistenceDataFinal:
    override def getReferenceData: TableReferenceDataWrapper =
        TableReferenceDataWrapper(tableName, idColumn, idColumn.columnType)


private case class FieldsAndParentPersistenceData(
    fields: Map[String, ValuePersistenceDataFinal],
    parent: Option[ReferenceValuePersistenceDataFinal]
)

trait PersistenceConfigLoader:
    def load(dataResourcePath: String,
             typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, TypePersistenceDataFinal]
    def getTypeToTableMap: Map[String, String]





class PersistenceConfigLoaderImpl(conf: Config) extends PersistenceConfigLoader:

    private val tableReferenceFactory = new TableReferenceFactory()
    private var typesDataPersistenceMap: Map[String, AbstractTypePersistenceData] = Map.empty
    private val columnNameParentPrefix = conf.getString("column-name-parent-prefix")
    private val columnNameSuperParentPrefix = conf.getString("column-name-super-parent-prefix")
    private val nameSubnamesDelimiter = conf.getString("column-name-subnames-delimiter")
    private val defaultValueColumnName = conf.getString("column-name-value-default")
    private val defalutStringMaxLength = conf.getInt("string-max-length-default")
    private val archivedEntityColumnName = conf.getString("archived-entity-column")
    private val idColumnNameDefault = conf.getString("id-column")
    private val valueColumnNameDefault = conf.getString("value-column")

    private val typeItemNameDelimiter = '/'

    private var sqlTableNamesMap: Map[String, String] = Map.empty

    private val primitiveTypeDefinitionToFieldType: Map[RootPrimitiveTypeDefinition, PersistenceFieldType] = Map(
        LongTypeDefinition -> LongFieldType,
        IntTypeDefinition -> IntFieldType,
        ShortIntTypeDefinition -> ShortIntFieldType,
        StringTypeDefinition -> StringFieldType,
        FixedStringTypeDefinition -> FixedStringFieldType,
        DoubleTypeDefinition -> DoubleFieldType,
        FloatTypeDefinition -> FloatFieldType,
        BooleanTypeDefinition -> BooleanFieldType,
        DateTypeDefinition -> DateFieldType,
        DateTimeTypeDefinition -> DateTimeWithTimeZoneFieldType,
        TimeTypeDefinition -> TimeWithTimeZoneFieldType,
        UUIDTypeDefinition -> UUIDFieldType,
        BinaryTypeDefinition -> BLOBFieldType,
        DecimalTypeDefinition -> DecimalFieldType,
    )

    private val idTypeDefinitionToFieldType: Map[EntityIdTypeDefinition, PersistenceFieldType] = Map(
        LongIdTypeDefinition -> LongFieldType,
        IntIdTypeDefinition -> IntFieldType,
        ShortIdTypeDefinition -> ShortIntFieldType,
        ByteIdTypeDefinition -> ByteFieldType,
        StringIdTypeDefinition -> StringFieldType,
        FixedStringIdTypeDefinition -> FixedStringFieldType,
        UUIDIdTypeDefinition -> UUIDFieldType,
    )

    private val consistencePersistenceTypesMap: Map[RootPrimitiveTypeDefinition, Set[PersistenceFieldType]] = Map(
        ByteTypeDefinition -> Set(ByteFieldType, ShortIntFieldType, IntFieldType, LongFieldType),
        ShortIntTypeDefinition -> Set(ShortIntFieldType, IntFieldType, LongFieldType),
        IntTypeDefinition -> Set(IntFieldType, LongFieldType),
        LongTypeDefinition -> Set(LongFieldType),
        StringTypeDefinition -> Set(TextFieldType, StringFieldType),
        FixedStringTypeDefinition -> Set(FixedStringFieldType),
        DoubleTypeDefinition -> Set(DoubleFieldType),
        FloatTypeDefinition -> Set(FloatFieldType, DoubleFieldType),
        DecimalTypeDefinition -> Set(DecimalFieldType),
        BooleanTypeDefinition -> Set(BooleanFieldType),
        DateTypeDefinition -> Set(DateFieldType, DateTimeFieldType),
        DateTimeTypeDefinition -> Set(DateTimeFieldType, DateTimeWithTimeZoneFieldType),
        TimeTypeDefinition -> Set(TimeFieldType, TimeWithTimeZoneFieldType),
        UUIDTypeDefinition -> Set(UUIDFieldType),
        BinaryTypeDefinition -> Set(BLOBFieldType),
    )

    private val arrayItemTypesDefinitionsMap: Map[PersistenceFieldType, RootPrimitiveTypeDefinition] = Map(
        LongFieldType -> LongTypeDefinition,
        IntFieldType -> IntTypeDefinition,
        ShortIntFieldType -> ShortIntTypeDefinition,
        ByteFieldType -> ByteTypeDefinition,
        TextFieldType -> StringTypeDefinition,
        StringFieldType -> StringTypeDefinition,
        FixedStringFieldType -> FixedStringTypeDefinition,
        DoubleFieldType -> DoubleTypeDefinition,
        FloatFieldType -> FloatTypeDefinition,
        BooleanFieldType -> BooleanTypeDefinition,
        DateFieldType -> DateTypeDefinition,
        DateTimeFieldType -> DateTimeTypeDefinition,
        DateTimeWithTimeZoneFieldType -> DateTimeTypeDefinition,
        TimeFieldType -> TimeTypeDefinition,
        TimeWithTimeZoneFieldType -> TimeTypeDefinition,
        UUIDFieldType -> UUIDTypeDefinition,
        BLOBFieldType -> BinaryTypeDefinition,
        DecimalFieldType -> DecimalTypeDefinition,
    )

    private def getIdFieldType(idType: EntityIdTypeDefinition): PersistenceFieldType =
        naturalizeFieldType( idTypeDefinitionToFieldType.getOrElse(idType, throw new NoTypeFound(idType.name)) )

    private def getValueFieldType(valueType: RootPrimitiveTypeDefinition): PersistenceFieldType =
        naturalizeFieldType( primitiveTypeDefinitionToFieldType.getOrElse(valueType, throw new NoTypeFound(valueType.name)))

    private def naturalizeFieldType(fieldTypeDefinition: PersistenceFieldType): PersistenceFieldType =
        fieldTypeDefinition match
            case StringFieldType => StringFieldType(defalutStringMaxLength)
            case FixedStringFieldType(length) => FixedStringFieldType(length)
            case other => other

    private class ColumnsNamesChecker:
        val columnsUsages: mutable.Map[String, String] = mutable.Map()
        def addAndCheckUsage(columnName: String, columnUsage: String): Unit =
            if (columnsUsages.contains(columnName))
                throw new ConsistencyException(s"Column name $columnName is already used for " +
                    s"${columnsUsages(columnName)}, but also find usage for $columnUsage!")
            columnsUsages(columnName) = columnUsage

    private class TableNamesChecker:
        val tableNames: mutable.Set[String] = mutable.Set()
        def addAndCheckUsage(tableName: String): Unit =
            if (tableNames.contains(tableName))
                throw new ConsistencyException(s"Table name $tableName is not unique!")
            tableNames += tableName

    override def getTypeToTableMap: Map[String, String] = sqlTableNamesMap

    override def load(
        dataResourcePath: String,
        typesDefinitionsMap: Map[String, AbstractNamedEntityType]
    ): Map[String, TypePersistenceDataFinal] =
        val tdSource = Source.fromResource (dataResourcePath)
        PersistenceConfigParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: RootPackagePersistenceData) =>
                typesDataPersistenceMap = packageData.toMap
                sqlTableNamesMap = mapTypeNamesToShortestUniqueNames(typesDefinitionsMap, typesDataPersistenceMap)
                val res = typesDefinitionsMap
                    .map ( (typeName, typeDef) =>
                            typeName -> mergeTypePersistenceData(typeDef, typesDataPersistenceMap.get(typeName))
                    )
                res.foreach((typeName, persistenceData) => {
                    tableReferenceFactory.initForType(typeName, persistenceData.getReferenceData)
                })
                res
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)

    private case class TypeNameData(name: String, subNames: List[String])

    private def mapTypeNamesToShortestUniqueNames(
        typesDefinitionsMap: Map[String, AbstractNamedEntityType],
        persistenceDataMap: Map[String, AbstractTypePersistenceData]
    ): Map[String, String] =
        val originalNames = typesDefinitionsMap.keySet
        val resMap = mutable.Map[String, String]()
        val tableNamesChecker = new TableNamesChecker()
        val rest: Iterable[TypeNameData] = typesDefinitionsMap
            .map((name, typeDef) =>
                val subTypesNames =  typeDef.valueType match
                    case arr: ArrayTypeDefinition => arr.allElementTypes.map(_.name).toList
                    case _ => List.empty
                persistenceDataMap.get(name)
                    .map {
                        case singleTaleType: TypePersistenceData => List(None -> singleTaleType.tableName)
                        case arrayType: ArrayTypePersistenceData =>
                            if subTypesNames.isEmpty then throw new ConsistencyException(
                                s"Type $name defined as single in persistence, where it is array type in definition!")
                            subTypesNames.map(itemTypeName =>
                                Some(itemTypeName) ->  arrayType.itemsMap.get(itemTypeName).flatMap(_.tableName)
                            )
                    }.map(itemNamePairs =>
                        val notFoundTypeItemsNames = itemNamePairs.map (itemNamePair =>
                            val itemTypeName = itemNamePair._1
                            itemNamePair._2 match
                                case Some(tableName) =>
                                    tableNamesChecker.addAndCheckUsage(tableName)
                                    val typeName = name + itemTypeName.map(typeItemNameDelimiter + _).getOrElse("")
                                    resMap += typeName -> tableName
                                    None
                                case None =>
                                    Some(itemTypeName)
                        )
                        .filter(_.isDefined)
                        .map(_.get)
                        if notFoundTypeItemsNames.isEmpty
                            then None
                            else Some(TypeNameData(name, notFoundTypeItemsNames
                                .filter(_.isDefined)
                                .map(_.get)))
                    )
                    .getOrElse(Some(TypeNameData(name, subTypesNames)))
            )
            .filter(_.isDefined)
            .map(_.get)

        addShortestUniqueNames(rest, 0, resMap, tableNamesChecker)

        resMap.toMap

    @tailrec
    private def addShortestUniqueNames(
        originalNamesData: Iterable[TypeNameData],
        level: Int,
        resMap: mutable.Map[String, String],
        tableNamesChecker: TableNamesChecker,
    ): Unit =

        def addToResMap(from: mutable.Map[String, TypeNameData]): Unit =
            from.foreach((sqlName, typeNameData) =>
                if (typeNameData.subNames.isEmpty)
                    tableNamesChecker.addAndCheckUsage(sqlName)
                    resMap += typeNameData.name -> sqlName
                else
                    val subResMap = mutable.Map[String, String]()
                    addShortestUniqueSubNames(sqlName, typeNameData.subNames, 0, subResMap, tableNamesChecker)
                    typeNameData.subNames.foreach(subName =>
                        resMap += (typeNameData.name + typeItemNameDelimiter + subName) -> tableName(sqlName, subResMap(subName))
                    )
            )

        val uniqueMap = mutable.Map[String, TypeNameData]()
        val nonUniqueMap = mutable.Map[String, TypeNameData]()
        val nonUniqueShortNamesSet = mutable.Set[String]()
        val mandatoryMap = mutable.Map[String, TypeNameData]()

        originalNamesData.foreach( typeNameData =>
            val (name, subNames) = (typeNameData.name, typeNameData.subNames)
            findNthFromEnd(name, level, TypesDefinitionsParser.NAMESPACES_DILIMITER) match
                case Some(pos) =>
                    val shortName = tableNameFromTypeName( name.substring(pos + 1) )
                    if (uniqueMap.contains(shortName))
                        nonUniqueMap += name -> typeNameData
                        nonUniqueShortNamesSet += shortName
                    else
                        uniqueMap += shortName -> typeNameData
                case None =>
                    mandatoryMap += tableNameFromTypeName(name) -> typeNameData
        )

        addToResMap(mandatoryMap)
        addToResMap(uniqueMap
            .filter((sqlName, _) =>
                val nonUnique = nonUniqueShortNamesSet.contains(sqlName) || mandatoryMap.contains(sqlName)
                if (nonUnique)
                    val tmpNameData = uniqueMap(sqlName)
                    nonUniqueMap += tmpNameData.name -> tmpNameData
                !nonUnique
            )
        )

        if nonUniqueMap.nonEmpty
            then addShortestUniqueNames(nonUniqueMap.values, level + 1, resMap, tableNamesChecker)

    @tailrec
    private def addShortestUniqueSubNames(
        rootName: String,
        originalNames: Iterable[String],
        level: Int,
        resMap: mutable.Map[String, String],
        tableNamesChecker: TableNamesChecker,
    ): Unit =

        val uniqueMap = mutable.Map[String, String]()
        val nonUniqueSet = mutable.Set[String]()
        val nonUniqueShortNamesSet = mutable.Set[String]()
        val mandatoryMap = mutable.Map[String, String]()

        originalNames.foreach(name =>
            findNthFromEnd(name, level, TypesDefinitionsParser.NAMESPACES_DILIMITER) match
                case Some(pos) =>
                    val shortName = tableNameFromTypeName(name.substring(pos + 1))
                    if (uniqueMap.contains(shortName))
                        nonUniqueSet += name
                        nonUniqueShortNamesSet += shortName
                    else
                        uniqueMap += shortName -> name
                case None =>
                    mandatoryMap += tableNameFromTypeName(name) -> name
        )

        mandatoryMap.foreach((sqlName, name) =>
            tableNamesChecker.addAndCheckUsage( tableName(rootName, sqlName) )
            resMap += name -> sqlName
        )
        uniqueMap
            .filter((sqlName, _) =>
                val nonUnique = nonUniqueShortNamesSet.contains(sqlName) || mandatoryMap.contains(sqlName)
                if (nonUnique)
                    nonUniqueSet += uniqueMap(sqlName)
                !nonUnique
            )
            .foreach((sqlName, name) =>
                tableNamesChecker.addAndCheckUsage( tableName(rootName, sqlName) )
                resMap += name -> sqlName
            )

        if nonUniqueSet.nonEmpty
            then addShortestUniqueSubNames(rootName, nonUniqueSet, level + 1, resMap, tableNamesChecker)

    private def tableName(prefix: String, subName: String): String =
        prefix + nameSubnamesDelimiter + subName


    private def findNthFromEnd(str: String, n: Int, char: Char): Option[Int] =
        var matchNo = -1
        var pos = str.length - 1
        while (pos >= 0)
            if (str(pos) == char)
                matchNo += 1
                if matchNo == n then return Some(pos)
            pos -= 1
        None

    private def mergeTypePersistenceData(typeDef: AbstractNamedEntityType,
                                         parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =
        typeDef match
            case EntityType(typeName, td: ObjectTypeDefinition) => mergeObjectTypePersistenceData(typeName, td, parsed)
            case EntityType(typeName, td: ArrayTypeDefinition) => mergeArrayTypePersistenceData(typeName, td, parsed)
            case EntityType(typeName, td: CustomPrimitiveTypeDefinition) => mergePrimitiveTypePersistenceData(typeName, td, parsed)
            case ObjectEntitySuperType(typeName, valueType) => mergeObjectTypePersistenceData(typeName, valueType, parsed)
            case ArrayEntitySuperType(typeName, valueType) => mergeArrayTypePersistenceData(typeName, valueType, parsed)
            case PrimitiveEntitySuperType(typeName, valueType) => mergePrimitiveTypePersistenceData(typeName, valueType, parsed)
            case _ => throw new TypesLoadExceptionException(s"Type ${typeDef.name} not supported")


    private def mergeObjectTypePersistenceData(typeName: String,
                                               typeDefinition: ObjectTypeDefinition,
                                               persistenceDataOpt: Option[AbstractTypePersistenceData]
                                              ): ObjectTypePersistenceDataFinal =
        val persistenceData = persistenceDataOpt
            .map(p =>
                if (!p.isInstanceOf[ObjectTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type $typeName is not ObjectTypePersistenceData type!")
                p.asInstanceOf[ObjectTypePersistenceData]
            )
            .getOrElse(ObjectTypePersistenceData(typeName, None, None, Map.empty, None))

        val tableName = persistenceData.tableName.getOrElse(sqlTableNamesMap(typeName))
        val idColumn = mergeIdTypeDefinition(typeDefinition.idType, persistenceData.idColumn)

        val columnsNamesChecker = ColumnsNamesChecker()
        columnsNamesChecker.addAndCheckUsage(idColumn.columnName, "ID column")

        val parentFieldsOverriden = getOverridenParentsFields(persistenceData.fields, typeDefinition)

        val prefix = ""
        val parentData = parentPersitanceDataToFinal(typeDefinition.parent, persistenceData.parentRelation, prefix)        
        val allFields = mergeFieldsAndParentFields(typeDefinition.fields ++ parentFieldsOverriden, 
            persistenceData.fields, parentData.fields, columnsNamesChecker, prefix, typeName)

        ObjectTypePersistenceDataFinal(tableName, idColumn, allFields, parentData.parent)

    private def getOverridenParentsFields(fieldsPersistenceData: Map[String, ValuePersistenceData],
                                          typeDefinition: FieldsContainer
                                         ): Map[String, FieldTypeDefinition] =
        fieldsPersistenceData
            .filter((fieldName, _) => !typeDefinition.fields.contains(fieldName))
            .map((fieldName, _) =>
                fieldName -> typeDefinition.allFields(fieldName)
            )

    private def mergeFieldsAndParentFields(
        fields: Map[String, FieldTypeDefinition],
        fieldsPersistenceData: Map[String, ValuePersistenceData],
        mergedParentFields: Map[String, ValuePersistenceDataFinal],
        columnsNamesChecker: ColumnsNamesChecker,
        prefix: String,
        typeName: String,
    ):  Map[String, ValuePersistenceDataFinal] =
        val currentFields = mergeFieldsPersistenceData(fields, fieldsPersistenceData, typeName, prefix)
        val allFields = currentFields ++ mergedParentFields.filter((fieldName, _) => !currentFields.contains(fieldName))
        allFields.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Field " + fe._1) ) )
        allFields

    private def mergeArrayTypePersistenceData(typeName: String,
                                              valueType: ArrayTypeDefinition,
                                              persistenceDataOpt: Option[AbstractTypePersistenceData]
                                             ): TypePersistenceDataFinal =
        val parsedData = persistenceDataOpt
            .map {
                case arrayTypePersistenceData: ArrayTypePersistenceData => arrayTypePersistenceData
                case PrimitiveTypePersistenceData(typeName, tableName, idColumn, valueColumn) =>
                    convertPrimitiveToArrayItemData(valueType, typeName, tableName, idColumn, valueColumn)
                case p => throw new ConsistencyException(
                    s"Persistence data for type $typeName is not ArrayTypePersistenceData type $p!")
            }
            .getOrElse(ArrayTypePersistenceData(typeName, Map.empty))

        ArrayTypePersistenceDataFinal(
            valueType.elementTypes.map(et =>
                val itemTypeName = et.valueType match
                    case TypeReferenceDefinition(referencedType) => referencedType.name
                    case td: RootPrimitiveTypeDefinition => td.name
                val parsedElementData = parsedData.itemsMap.getOrElse(itemTypeName, ArrayItemPersistenceData(None, None, None))
                parsedElementData.idColumn.foreach(idColumn =>
                    checkEntityIdAndPersistenceTypeConsistency(valueType.idType, idColumn, () =>
                        s"Array item ID column for type $typeName and item type $itemTypeName"))
                ItemTypePersistenceDataFinal(
                    parsedElementData.tableName.getOrElse(sqlTableNamesMap(typeName + typeItemNameDelimiter + itemTypeName)),
                    mergeIdTypeDefinition(valueType.idType, parsedElementData.idColumn),
                    toArrayItemValuePersistenceDataFinal(
                        checkAndBypassDefaultsArrayItemPersistenceData(et, parsedElementData.valueColumn, typeName), 
                        et.valueType, 
                        typeName)
                )
            ), idTypeDefinitionToFieldType.getOrElse(valueType.idType, throw new NoTypeFound(valueType.idType.name))
        )

    private def convertPrimitiveToArrayItemData(valueType: ArrayTypeDefinition, typeName: String, tableName: Option[String], idColumn: Option[PrimitiveValuePersistenceData], valueColumn: Option[PrimitiveValuePersistenceData]) = {
        val arrayItemType = valueColumn.flatMap(_.columnType)
        arrayItemType match
            case None =>
                if (valueType.elementTypes.size == 1)
                    val itemTypeName: String = valueType.elementTypes.head.name
                    ArrayTypePersistenceData(typeName, Map(itemTypeName ->
                        ArrayItemPersistenceData(tableName, idColumn, valueColumn)))
                else
                    throw new ConsistencyException(
                        s"Primitive persistence data for array type $typeName could not be converted to " +
                            s"array persitence data, because item persistence type is not defined and " +
                            s"array value type has more than one item!")
            case Some(persistenceType) =>
                val consistentType = arrayItemTypesDefinitionsMap(persistenceType)
                val typeDefinition = valueType.elementTypes
                    .find(_.valueType == consistentType)
                    .getOrElse(throw new ConsistencyException(
                        s"Primitive persistence data for array type $typeName could not be converted to " +
                            s"array persitence data, because item persistence type $persistenceType has " +
                            s"no corresponding type definition array value items types!"))
                ArrayTypePersistenceData(typeName, Map(typeDefinition.name ->
                    ArrayItemPersistenceData(tableName, idColumn, valueColumn)))
    }

    private def mergePrimitiveTypePersistenceData(typeName: String,
                                                  valueType: CustomPrimitiveTypeDefinition,
                                                  parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =

        val parsedData = parsed
            .map(p =>
                if (!p.isInstanceOf[PrimitiveTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type $typeName is not PrimitiveTypePersistenceData type!")
                p.asInstanceOf[PrimitiveTypePersistenceData]
            )
            .getOrElse(PrimitiveTypePersistenceData(typeName, None, None, None))

        val valueColumnData = parsedData.valueColumn.getOrElse(PrimitiveValuePersistenceData(None, None))

        PrimitiveTypePersistenceDataFinal(
            parsedData.tableName.getOrElse( sqlTableNamesMap(typeName) ),
            mergeIdTypeDefinition(valueType.idType, parsedData.idColumn),
            PrimitiveValuePersistenceDataFinal(
                colName(valueColumnData.columnName.getOrElse(valueColumnNameDefault)),
                valueColumnData.columnType.getOrElse( getValueFieldType(valueType.rootType) ), false
            ))
        
    private def colName(nameRaw: String): String = 
        val res = nameRaw.toLowerCase
        if (res == archivedEntityColumnName) throw new ConsistencyException(s"Column name $res is reserved!")
        res

    private def getParentPersistenceDataOrDefault(
                                              parentRelationOpt: Option[ExpandParentMarker]
                                          ): Either[ExpandParentMarker, ReferenceValuePersistenceData] =
        parentRelationOpt.map(Left(_)).getOrElse(Right(ReferenceValuePersistenceData(None)))

    private class SimpleObjectParendData[P <: ReferenceValuePersistenceDataFinal | ParentTableReferenceFinal](
        val parent: Option[P],
        val fields: Map[String, ValuePersistenceDataFinal]
    )

    private def parentPersitanceDataToFinal(
        parentType: Option[ObjectEntitySuperType],
        parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
        typeName: String,
        fieldsPrefix: String, 
    ): SimpleObjectParendData[ReferenceValuePersistenceDataFinal] =
        parentType match
            case Some(parentDef) =>
                parentPersistenceData match
                    case Right(ReferenceValuePersistenceData(columnName)) =>
                        SimpleObjectParendData(
                            Some(ReferenceValuePersistenceDataFinal(
                                colName(columnName.getOrElse(columnNameParentPrefix +
                                    columnNameFromFieldName(getTypeSimpleName(parentDef.name)))),
                                tableReferenceFactory.createForType(parentDef.name), false
                            )),
                            Map.empty
                        )
                    case Left(ExpandParentMarkerSingle) =>
                        SimpleObjectParendData(
                            parentDef.valueType.parent.map(superParentDef =>
                                val fieldsParsedData = typesDataPersistenceMap.get(superParentDef.name)
                                    .flatMap(p =>
                                        if (!p.isInstanceOf[ObjectTypePersistenceData])
                                            throw new ConsistencyException(s"Persistence data for type " +
                                                s"${superParentDef.name} is not ObjectTypePersistenceData type!")
                                        val superTypeParentRelation = getParentPersistenceDataOrDefault(
                                            p.asInstanceOf[ObjectTypePersistenceData].parentRelation)
                                        superTypeParentRelation.toOption
                                    )
                                    .getOrElse(ReferenceValuePersistenceData(None))
                                ReferenceValuePersistenceDataFinal(
                                    colName(fieldsParsedData.columnName.getOrElse(columnNameSuperParentPrefix +
                                        columnNameFromFieldName(getTypeSimpleName(superParentDef.name)))),
                                    tableReferenceFactory.createForType(superParentDef.name), false
                                )),
                            copyParentFieldsPersitenceData(fieldsPrefix, parentDef, false)
                        )
                    case Left(ExpandParentMarkerTotal) =>
                        SimpleObjectParendData(None, copyParentFieldsPersitenceData(fieldsPrefix, parentDef, true))
                    case Left(expandMarker) =>
                        throw new NotDefinedOperationException(s"Undefined parent expander marker $expandMarker!")
            case None =>
                parentPersistenceData match
                    case Right(ReferenceValuePersistenceData(columnName)) =>
                        if (columnName.isDefined)
                            throw new ConsistencyException(s"Type $typeName has no super type, but there is reference " +
                                s"column defined!")
                        SimpleObjectParendData(None, Map.empty)
                    case _ =>
                        SimpleObjectParendData(None, Map.empty)

    private def parentPersitanceDataToFinal(
        parentType: Option[ObjectEntitySuperType],
        parentPersistenceData: Option[ExpandParentMarker],
        fieldsPrefix: String, 
    ): SimpleObjectParendData[ParentTableReferenceFinal] =
        parentType match
            case Some(parentDef) =>
                parentPersistenceData match
                    case None =>
                        SimpleObjectParendData(
                            Some(ParentTableReferenceFinal(tableReferenceFactory.createForType(parentDef.name))),
                            Map.empty
                        )
                    case Some(ExpandParentMarkerSingle) =>
                        SimpleObjectParendData(
                            parentDef.valueType.parent.map(superParentDef =>
                                ParentTableReferenceFinal(tableReferenceFactory.createForType(superParentDef.name))),
                                copyParentFieldsPersitenceData(fieldsPrefix, parentDef, false)
                        )
                    case Some(ExpandParentMarkerTotal) =>
                        SimpleObjectParendData(
                            None,
                            copyParentFieldsPersitenceData(fieldsPrefix, parentDef, true)
                        )
                    case Some(expandMarker) =>
                        throw new NotDefinedOperationException(s"Undefined parent expander marker $expandMarker!")
            case None =>
                SimpleObjectParendData(
                    None, Map.empty
                )


    private def checkRootPrimitiveAndPersistenceTypeConsistency(fieldType: RootPrimitiveTypeDefinition,
                                                                fieldPersistType: PrimitiveValuePersistenceData,
                                                                itemDescriptionProvider: () => String): Unit =
        val consistentPersistTypes = consistencePersistenceTypesMap.getOrElse(fieldType,
            throw new NoTypeFound(fieldType.name))
        if (fieldPersistType.columnType.isDefined &&
            !fieldPersistType.columnType.map(_.name).exists(currName => consistentPersistTypes.exists(currName == _.name))
        )
            throw new ConsistencyException(itemDescriptionProvider() + " defined with inconsistent persistence " +
                s"type ${fieldPersistType.columnType} where field type is ${fieldType.name}!")
            
    private def checkEntityIdAndPersistenceTypeConsistency(idType: EntityIdTypeDefinition,
                                                           idPersistType: PrimitiveValuePersistenceData,
                                                           itemDescriptionProvider: () => String): Unit =
        val consistentPersistType = idTypeDefinitionToFieldType.getOrElse(idType, throw new NoTypeFound(idType.name))
        if (idPersistType.columnType.isDefined && !idPersistType.columnType.contains(consistentPersistType))
            throw new ConsistencyException(itemDescriptionProvider() + " defined with inconsistent persistence " +
                s"type ${idPersistType.columnType} where field type is ${idType.name}!")

    private def mergeIdTypeDefinition(idType: EntityIdTypeDefinition,
                                      parsed: Option[PrimitiveValuePersistenceData]): PrimitiveValuePersistenceDataFinal =
        val idColumnSrc = parsed.getOrElse(PrimitiveValuePersistenceData(None, None))
        PrimitiveValuePersistenceDataFinal(
            colName(idColumnSrc.columnName.getOrElse(idColumnNameDefault)),
            idColumnSrc.columnType.getOrElse( getIdFieldType(idType) ), false
        )

    private def getTypeSimpleName(typeName: String): String =
        typeName.substring(typeName.lastIndexOf(TypesDefinitionsParser.NAMESPACES_DILIMITER) + 1)

    private def copyParentFieldsPersitenceData(prefix: String,
                                               typeDef: ObjectEntitySuperType,
                                               copySuperParents: Boolean): Map[String, ValuePersistenceDataFinal] =
        val prefix_ = prefix + columnNameParentPrefix + columnNameFromFieldName(getTypeSimpleName(typeDef.name)) +
            nameSubnamesDelimiter

        val fieldsParsedData = typesDataPersistenceMap.get(typeDef.name)
            .map(p =>
                if (!p.isInstanceOf[ObjectTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type ${typeDef.name} is not ObjectTypePersistenceData type!")
                p.asInstanceOf[ObjectTypePersistenceData].fields
            )
            .getOrElse(Map.empty)

        val parentFieldsData = mergeFieldsPersistenceData(typeDef.valueType.fields, fieldsParsedData, typeDef.name, prefix_)

        val superParentFieldsData = typeDef.valueType.parent.map(superParentType =>
            if (copySuperParents)
                copyParentFieldsPersitenceData(prefix_, superParentType, copySuperParents)
            else
                Map.empty
        ).getOrElse(Map.empty)

        val columnsNamesChecker = ColumnsNamesChecker()
        parentFieldsData.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Field " + fe._1)))
        superParentFieldsData.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Super field " + fe._1)))

        parentFieldsData ++ superParentFieldsData

    private def mergeFieldsPersistenceData(fields: Map[String, FieldTypeDefinition],
                                           fieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                           typeName: String,
                                           prefix: String): Map[String, ValuePersistenceDataFinal] =
        fields
            .map( (fieldName, fieldType) =>
                val columnData = 
                    checkAndBypassDefaultsObjectFieldPersistenceData(fieldName, fieldType, fieldsPersistenceDataMap, typeName)
                        .map(toFieldPersistenceDataFinal(_, fieldName, fieldType, typeName, prefix))
                fieldName -> columnData
            )
            .filter(_._2.isDefined)
            .map(fe => fe._1 -> fe._2.get)

    private def checkAndBypassDefaultsObjectFieldPersistenceData(
                                                                    fieldName: String,
                                                                    fieldType: FieldTypeDefinition,
                                                                    fieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                                                    typeName: String ): Option[ValuePersistenceData] =
        val itemDescriptionProvider = () => s"Field $fieldName of type $typeName"
        fieldsPersistenceDataMap
            .get(fieldName)
            .map(persistData =>
                fieldType.valueType match
                    case fieldType: RootPrimitiveTypeDefinition =>
                        checkRootPrimitivePersistenceData(persistData, fieldType, itemDescriptionProvider)
                    case SimpleObjectTypeDefinition(_, _) =>
                        checkSimpleObjectPersistenceData(persistData, itemDescriptionProvider)
                    case TypeReferenceDefinition(referencedType) =>
                        checkTypeReferencePersistenceData(persistData, referencedType, itemDescriptionProvider)
                    case TypeBackReferenceDefinition(referencedType, _) => //do nothing
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" has not expected field type $fieldType!")

                persistData
            )
            .orElse(fieldType.valueType match
                case fieldType: RootPrimitiveTypeDefinition =>
                    Some(PrimitiveValuePersistenceData(None, None))
                case SimpleObjectTypeDefinition(_, _) =>
                    Some(SimpleObjectValuePersistenceData(Right(ReferenceValuePersistenceData(None)), Map.empty))
                case TypeReferenceDefinition(_) =>
                    Some(ColumnPersistenceData(None))
                case TypeBackReferenceDefinition(_, _) =>
                    None
            )

    private def checkAndBypassDefaultsArrayItemPersistenceData(
                                                                  itemType: ArrayItemTypeDefinition,
                                                                  fieldsPersistenceData: Option[ArrayValuePersistenceData],
                                                                  typeName: String ): ArrayValuePersistenceData =
        val itemDescriptionProvider = () => s"Item $itemType of type $typeName"
        fieldsPersistenceData
            .map(persistData =>
                itemType.valueType match
                    case fieldType: RootPrimitiveTypeDefinition =>
                        checkRootPrimitivePersistenceData(persistData, fieldType, itemDescriptionProvider)
                    case TypeReferenceDefinition(referencedType) =>
                        checkTypeReferencePersistenceData(persistData, referencedType, itemDescriptionProvider)
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" has not expected as array item type $itemType!")

                persistData
            )
            .getOrElse(itemType.valueType match
                case fieldType: RootPrimitiveTypeDefinition =>
                    PrimitiveValuePersistenceData(None, None)
                case TypeReferenceDefinition(_) =>
                    ColumnPersistenceData(None)
            )

    private def checkRootPrimitivePersistenceData(valuePersistenceData: ValuePersistenceData,
                                                  fieldType: RootPrimitiveTypeDefinition,
                                                  itemDescriptionProvider: () => String): Unit =
        valuePersistenceData match
            case primitivePersistenceData: PrimitiveValuePersistenceData =>
                checkRootPrimitiveAndPersistenceTypeConsistency(fieldType, primitivePersistenceData,itemDescriptionProvider)
            case ColumnPersistenceData(_) => // do nothing
            case otherPersistenceData =>
                throw new ConsistencyException(itemDescriptionProvider() +
                    s" with simple type $fieldType defined in persistence as not simpe type $otherPersistenceData!")

    private def checkSimpleObjectPersistenceData(persistData: ValuePersistenceData, itemDescriptionProvider: () => String): Unit =
        persistData match
            case fieldPersistType: PrimitiveValuePersistenceData =>
                throw new ConsistencyException(itemDescriptionProvider() +
                    s" with object type $fieldPersistType defined in persistence as primitive type!")
            case _ => // do nothing

    private def checkTypeReferencePersistenceData(persistData: ValuePersistenceData,
                                                  referencedType: AbstractNamedEntityType,
                                                  itemDescriptionProvider: () => String): Unit =
        referencedType match
            case EntityType(_, valueType: PrimitiveTypeDefinition) =>
                persistData match
                    case fieldPersistType: PrimitiveValuePersistenceData =>
                        checkRootPrimitiveAndPersistenceTypeConsistency(valueType.rootType,
                            fieldPersistType, itemDescriptionProvider)
                    case _ => // do nothing
            case PrimitiveEntitySuperType(_, valueType) =>
                persistData match
                    case fieldPersistType: PrimitiveValuePersistenceData =>
                        checkRootPrimitiveAndPersistenceTypeConsistency(valueType.rootType,
                            fieldPersistType, itemDescriptionProvider)
                    case _ => // do nothing
            case EntityType(_, valueType: ObjectTypeDefinition) =>
                persistData match
                    case fieldPersistType: SimpleObjectValuePersistenceData => // do nothing
                    case fieldPersistType: ColumnPersistenceData => // do nothing
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" with object type reference defined in persistence as not object type!")
            case _ =>
                persistData match
                    case fieldPersistType: ColumnPersistenceData => // do nothing
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" with object type reference defined in persistence as not reference type!")


    private def toFieldPersistenceDataFinal(fieldPersitenceData: ValuePersistenceData,
                                            fieldName: String,
                                            fieldType: FieldTypeDefinition,
                                            typeName: String,
                                            prefix: String = ""): ValuePersistenceDataFinal =
        fieldType.valueType match
            case pritimiveType: RootPrimitiveTypeDefinition =>
                toPrimitivePersistenceDataFinal(pritimiveType, fieldPersitenceData, fieldName, fieldType.isNullable,
                    prefix, () => s"Field $fieldName of type $typeName")
            case subObjectType: SimpleObjectTypeDefinition =>
                fieldPersitenceData match
                    case SimpleObjectValuePersistenceData(parentRelation, fieldsPersistenceMap) =>
                        if (fieldsPersistenceMap.keys.exists(!subObjectType.allFields.contains(_)))
                            throw new ConsistencyException(s"Field $fieldName of type $typeName contains " +
                                s"persistent data for fields not defined in type definition!")
                        val parentFieldsOverriden = getOverridenParentsFields(fieldsPersistenceMap, subObjectType)
                        createSimpleObjectFromParent(subObjectType.fields, fieldsPersistenceMap,
                            subObjectType.parent, fieldName, parentRelation, prefix,
                            s" $fieldName fields of $typeName SimpleObject")
                    case _ =>
                        throw new ConsistencyException(s"Field $fieldName of type $typeName is defined as simple object " +
                            s"type but has not simple object type $fieldPersitenceData found persistence data!")
            case refType: TypeReferenceDefinition =>
                fieldPersitenceData match
                    case primitivePersistenceData: PrimitiveValuePersistenceData =>
                        refType.referencedType.valueType match
                            case customPrimitiveType: CustomPrimitiveTypeDefinition =>
                                toPrimitivePersistenceDataFinal(customPrimitiveType.rootType, 
                                    primitivePersistenceData, fieldName, fieldType.isNullable,
                                    prefix, () => s"Field $fieldName of type $typeName")
                            case otherType => throw new ConsistencyException("Only CustomPrimitiveTypeDefinition could be " +
                                s"saved in primitive columns! Trying to save $otherType as $primitivePersistenceData.")
                    case SimpleObjectValuePersistenceData(parentRelation, fieldsPersistenceMap) =>
                        refType.referencedType.valueType match
                            case objectType: ObjectTypeDefinition =>
                                parentRelation match
                                    case Left(_ :ExpandParentMarker) =>
                                        val parentFieldsOverriden = getOverridenParentsFields(fieldsPersistenceMap, objectType)
                                        createSimpleObjectFromParent(objectType.fields ++ parentFieldsOverriden,
                                            fieldsPersistenceMap, objectType.parent, fieldName, parentRelation, prefix,
                                            s" $fieldName fields of $typeName SimpleObject")
                                    case Right(ReferenceValuePersistenceData(columnName)) =>
                                        toReferencePersistenceDataFinal(refType, ColumnPersistenceData(columnName),
                                            columnNameFromFieldName(fieldName), fieldType.isNullable, prefix,
                                            () => s"Field $fieldName of type $typeName")
                            case otherType =>
                                throw new ConsistencyException("Only ObjectTypePersistenceData could be " +
                                    s"saved in denormalized columns! Trying to save $otherType as $otherType.")
                    case _: ColumnPersistenceData =>
                        toReferencePersistenceDataFinal(refType, fieldPersitenceData,
                            columnNameFromFieldName(fieldName), fieldType.isNullable, prefix,
                            () => s"Field $fieldName of type $typeName")
            case _ =>
                throw new ConsistencyException(s"Type definition is not found! ${fieldType.valueType}")

    private def toArrayItemValuePersistenceDataFinal(persistenceData: ArrayValuePersistenceData,
                                                     itemType: ArrayItemValueTypeDefinitions,
                                                     typeName: String
                                                    ): PrimitiveValuePersistenceDataFinal | ReferenceValuePersistenceDataFinal =
        itemType match
            case primitiveType: RootPrimitiveTypeDefinition =>
                toPrimitivePersistenceDataFinal(primitiveType, persistenceData, defaultValueColumnName, false, "",
                    () => s"Item $itemType of type $typeName")
            case refType: TypeReferenceDefinition =>
                toReferencePersistenceDataFinal(refType, persistenceData, defaultValueColumnName, false, "",
                    () => s"Item $itemType of type $typeName")



    private def createSimpleObjectFromParent(fields: Map[String, FieldTypeDefinition],
                                             subFieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                             parent: Option[ObjectEntitySuperType],
                                             fieldName: String,
                                             parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                             prefix: String,
                                             typeName: String): SimpleObjectValuePersistenceDataFinal =

        val prefix_ = prefix + columnNameFromFieldName(fieldName) + nameSubnamesDelimiter
        val parentPersistenceDataFinal = parentPersitanceDataToFinal(parent, parentPersistenceData, typeName, prefix_)
        
        val columnsNamesChecker = ColumnsNamesChecker()
        parentPersistenceDataFinal.parent.foreach(p => columnsNamesChecker.addAndCheckUsage(p.columnName, "Parent column"))
        
        val allFields = mergeFieldsAndParentFields(fields, subFieldsPersistenceDataMap, 
            parentPersistenceDataFinal.fields, columnsNamesChecker, prefix_, typeName)

        SimpleObjectValuePersistenceDataFinal(parentPersistenceDataFinal.parent, allFields)



    private def toReferencePersistenceDataFinal(fieldType: TypeReferenceDefinition,
                                                persistenceData: ValuePersistenceData,
                                                defaultColumnName: String,
                                                isNullable: Boolean,
                                                prefix: String,
                                                itemDescriptionProvider: () => String) =

        persistenceData match
            case ColumnPersistenceData(columnName) =>
                val columnNameFinal = colName(prefix + columnName.getOrElse(defaultColumnName))
                ReferenceValuePersistenceDataFinal(
                    columnNameFinal,
                    tableReferenceFactory.createForType(fieldType.referencedType.name),
                    isNullable
                )
            case _ =>
                throw new ConsistencyException(itemDescriptionProvider() + s" parsed as not reference type " +
                    s"whereas field type is TypeReferenceDefinition!")

    private def toPrimitivePersistenceDataFinal(fieldType: RootPrimitiveTypeDefinition,
                                                persistenceData: ValuePersistenceData,
                                                defaultColumnName: String,
                                                isNullable: Boolean,
                                                prefix: String,
                                                itemDescriptionProvider: () => String) =

        val persisTypeOpt = persistenceData match
            case PrimitiveValuePersistenceData(_, columnType) =>
                columnType
            case ColumnPersistenceData(_) =>
                None
            case _ =>
                throw new ConsistencyException(itemDescriptionProvider() + s" parsed as not primitive type " +
                    s"whereas field type is RootPrimitiveTypeDefinition!")
        val columnNameFinal = prefix + persistenceData.columnName.getOrElse(defaultColumnName)
        val rootFieldType = fieldType.rootType
        val persisType = persisTypeOpt.getOrElse( getValueFieldType(rootFieldType))

        PrimitiveValuePersistenceDataFinal(colName(columnNameFinal), persisType, isNullable)
                    
    private def tableNameFromTypeName(typeName: String): String = typeName.toLowerCase().replace(TypesDefinitionsParser.NAMESPACES_DILIMITER.toString, "_")
    private def columnNameFromFieldName(fieldName: String): String = camelCaseToSnakeCase(fieldName)
    private def camelCaseToSnakeCase(name: String): String = name.replaceAll("[A-Z]", "_$0").toLowerCase()
                
