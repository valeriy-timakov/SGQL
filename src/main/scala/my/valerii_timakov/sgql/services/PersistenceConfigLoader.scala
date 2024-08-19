package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.{AbstractEntityType, AbstractNamedEntityType, AbstractTypeDefinition, ArrayEntitySuperType, ArrayItemEntityType, ArrayItemTypeDefinitions, ArrayTypeDefinition, BinaryTypeDefinition, BooleanTypeDefinition, CustomPrimitiveTypeDefinition, DateTimeTypeDefinition, DateTypeDefinition, DoubleTypeDefinition, EntityIdTypeDefinition, EntityType, EntityTypeDefinition, FloatTypeDefinition, IntIdTypeDefinition, IntTypeDefinition, LongIdTypeDefinition, LongTypeDefinition, ObjectEntitySuperType, ObjectTypeDefinition, PrimitiveEntitySuperType, PrimitiveFieldTypeDefinition, RootPrimitiveTypeDefinition, SimpleEntityType, SimpleObjectTypeDefinition, SimpleTypeDefinitions, StringIdTypeDefinition, StringTypeDefinition, TimeTypeDefinition, TypeReferenceDefinition, TypesDefinitionsParseError, UUIDIdTypeDefinition, UUIDTypeDefinition}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, NoTypeFound, TypesLoadExceptionException}

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader


sealed trait ValuePersistenceDataFinal:
    def columnNames: Seq[String]

class PrimitiveValuePersistenceDataFinal(
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
    def get: String

class TableReferenceFactory:
    class TableReferenceProxy(name: Option[String]) extends TableReference:
        var _name: Option[String] = name
        def get: String = _name.getOrElse(throw new RuntimeException("Reference table name is not set!"))
    private val instancesMap: mutable.Map[String, TableReferenceProxy] = mutable.Map()
    private val tableNamesMap: mutable.Map[String, mutable.Set[String]] = mutable.Map()
    def createForTable(typeName: String, tableName: String): TableReferenceProxy = 
        tableNamesMap.getOrElseUpdate(typeName, mutable.Set.empty) += tableName
        TableReferenceProxy(Some(tableName))
    def createForType(typeName: String): TableReferenceProxy =
        instancesMap.getOrElseUpdate(typeName, TableReferenceProxy(None))
    def initForType(typeName: String, tableName: String): Unit =
        tableNamesMap.get(typeName).foreach(tableNames => 
            val wrongNames = tableNames.filter(_ != tableName)
            if (wrongNames.nonEmpty) 
                throw new ConsistencyException(s"Table name $tableName of type $typeName has wrong table names for " +
                    s"reference to it! Names: $wrongNames")
            )
        instancesMap.get(typeName).exists( instance =>
            instance._name = Some(tableName)
            true
        )

trait TypePersistenceDataFinal

class PrimitiveTypePersistenceDataFinal(
    val tableName: String,
    val idColumn: PrimitiveValuePersistenceDataFinal,
    val valueColumn: PrimitiveValuePersistenceDataFinal,
) extends TypePersistenceDataFinal

class ItemTypePersistenceDataFinal(
    val tableName: String,
    val idColumn: PrimitiveValuePersistenceDataFinal,
    val valueColumn: PrimitiveValuePersistenceDataFinal | ReferenceValuePersistenceDataFinal,
) extends TypePersistenceDataFinal

class ArrayTypePersistenceDataFinal(
    val data: Set[ItemTypePersistenceDataFinal],
) extends TypePersistenceDataFinal

class ObjectTypePersistenceDataFinal(
    val tableName: String,
    val idColumn: PrimitiveValuePersistenceDataFinal,
    val fields: Map[String, ValuePersistenceDataFinal],
    val parent: Option[ReferenceValuePersistenceDataFinal],
) extends TypePersistenceDataFinal


trait PersistenceConfigLoader:
    def load(dataResourcePath: String,
             typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, TypePersistenceDataFinal]


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
    StringTypeDefinition -> Set(TextFieldType, StringFieldType(0)),
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

    var generatedSimpleObjects: Map[String, ObjectTypePersistenceDataFinal] = Map.empty
    val tableReferenceFactory = new TableReferenceFactory()
    var typesDataPersistenceMap: Map[String, AbstractTypePersistenceData] = Map.empty

    private class ColumnsNamesChecker:
        val columnsUsages: mutable.Map[String, String] = mutable.Map()
        def addAndCheckUsage(columnName: String, columnUsage: String): Unit =
            if (columnsUsages.contains(columnName)) {
                throw new ConsistencyException(s"Column name $columnName is already used for " +
                    s"${columnsUsages(columnName)}, but also find usage for $columnUsage!")
            }
            columnsUsages(columnName) = columnUsage

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
            case EntityType(_, td: ObjectTypeDefinition) => mergeObjectTypePersistenceData(typeName, td, parsed)
            case EntityType(_, td: ArrayTypeDefinition) => mergeArrayTypePersistenceData(typeName, td, parsed)
            case EntityType(_, td: CustomPrimitiveTypeDefinition) => mergePrimitiveTypePersistenceData(typeName, td, parsed)
            case ObjectEntitySuperType(_, valueType) => mergeObjectTypePersistenceData(typeName, valueType, parsed)
            case ArrayEntitySuperType(_, valueType) => mergeArrayTypePersistenceData(typeName, valueType, parsed)
            case PrimitiveEntitySuperType(_, valueType) => mergePrimitiveTypePersistenceData(typeName, valueType, parsed)
            case _ => throw new TypesLoadExceptionException(s"Type $typeName not supported")


    private def mergeObjectTypePersistenceData(typeName: String,
                                                    valueType: ObjectTypeDefinition,
                                                    parsed: Option[AbstractTypePersistenceData]): ObjectTypePersistenceDataFinal =
        val parsedData = parsed
            .map(p =>
                if (!p.isInstanceOf[ObjectTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type $typeName is not ObjectTypePersistenceData type!")
                p.asInstanceOf[ObjectTypePersistenceData]
            )
            .getOrElse(ObjectTypePersistenceData(typeName, None, None, Map.empty, None))

        val tableName = parsedData.tableName.getOrElse(tableNameFromTypeName(typeName))
        val idColumn = mergeIdTypeDefinition(valueType.idType, parsedData.idColumn)
        
        val parentRelation = getParentRelationOrDefault(parsedData.parentRelation)
        val parentPersistenceData = parentPersitanceDataToFinal(valueType.parent, parentRelation, typeName)
        val fieldsFinal = mergeFieldsPersistenceData(valueType.fields, parsedData.fields, typeName)

        val superFieldsFinal = valueType.parent match
            case Some(parentDef) =>
                parentRelation match
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

    private def mergeArrayTypePersistenceData(typeName: String,
                                                   valueType: ArrayTypeDefinition,
                                                   parsed: Option[AbstractTypePersistenceData]): TypePersistenceDataFinal =
        val parsedData = parsed
            .map(p =>
                if (!p.isInstanceOf[ArrayTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type $typeName is not ArrayTypePersistenceData type!")
                p.asInstanceOf[ArrayTypePersistenceData]
            )
            .getOrElse(ArrayTypePersistenceData(typeName, Seq.empty))

        ArrayTypePersistenceDataFinal(
            valueType.elementTypes.map(et =>
                val itemTypeName = et.valueType match
                    case TypeReferenceDefinition(referencedType, _) => referencedType.name
                    case td: RootPrimitiveTypeDefinition => td.name
                val parsedElementData = parsedData.data.find(_.typeName == itemTypeName)
                    .getOrElse(ArrayItemPersistenceData(itemTypeName, None, None, None))
                ItemTypePersistenceDataFinal(
                    parsedElementData.tableName.getOrElse(tableNameFromTypeName(itemTypeName)),
                    mergeIdTypeDefinition(valueType.idType, parsedElementData.idColumn),
                    toArrayItemValuePersistenceDataFinal(
                        checkAndBypassDefaultsArrayItemPersistenceData(et, parsedElementData.valueColumn, typeName), 
                        et.valueType, 
                        typeName)
                )
            )
        )

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
            parsedData.tableName.getOrElse( tableNameFromTypeName(typeName) ),
            mergeIdTypeDefinition(valueType.idType, parsedData.idColumn),
            PrimitiveValuePersistenceDataFinal(
                valueColumnData.columnName.getOrElse("value"),
                valueColumnData.columnType.getOrElse(
                    primitiveTypeDefinitionToFieldType.getOrElse(valueType.rootType, throw new NoTypeFound(valueType.rootType.name)) )
            ))

    private def getParentRelationOrDefault(
                                              parentRelationOpt: Option[Either[ExpandParentMarker, ReferenceValuePersistenceData]]
                                          ): Either[ExpandParentMarker, ReferenceValuePersistenceData] =
        parentRelationOpt.getOrElse(Right(ReferenceValuePersistenceData(None)))

    private def parentPersitanceDataToFinal(parent: Option[ObjectEntitySuperType],
                                            parentRelation: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                            typeName: String)
                                : Option[ReferenceValuePersistenceDataFinal] =
        parent match
            case Some(parentDef) =>
                parentRelation match
                    case Right(ReferenceValuePersistenceData(columnName)) =>
                        Some(ReferenceValuePersistenceDataFinal(
                            columnName.getOrElse(columnNameFromFieldName("parent__" + parentDef.name)),
                            tableReferenceFactory.createForType(parentDef.name)
                        ))
                    case Left(ExpandParentMarkerSingle) =>
                        parentDef.valueType.parent.map(superParentDef =>
                            val fieldsParsedData = typesDataPersistenceMap.get(superParentDef.name)
                                .flatMap(p => {
                                    if (!p.isInstanceOf[ObjectTypePersistenceData]) {
                                        throw new ConsistencyException(s"Persistence data for type " +
                                            s"${superParentDef.name} is not ObjectTypePersistenceData type!")
                                    }
                                    val superTypeParentRelation = getParentRelationOrDefault(
                                        p.asInstanceOf[ObjectTypePersistenceData].parentRelation)
                                    superTypeParentRelation.toOption
                                })
                                .getOrElse(ReferenceValuePersistenceData(None))
                            ReferenceValuePersistenceDataFinal(
                                fieldsParsedData.columnName.getOrElse(columnNameFromFieldName("super_parent__" + 
                                    superParentDef.name)),
                                tableReferenceFactory.createForType(superParentDef.name)
                            ))
                    case Left(ExpandParentMarkerTotal) =>
                        None
                    case _ =>
                        None
            case None =>
                parentRelation match
                    case Right(ReferenceValuePersistenceData(columnName)) =>
                        if (columnName.isDefined) {
                            throw new ConsistencyException(s"Type $typeName has no super type, but there is reference " +
                                s"column defined!")
                        }
                        None
                    case _ =>
                        None


    private def checkRootPrimitiveAndPersistenceTypeConsistency(fieldType: RootPrimitiveTypeDefinition,
                                                                fieldPersistType: PrimitiveValuePersistenceData,
                                                                itemDescriptionProvider: () => String): Unit =
        val consistentPersistTypes = consistencePersistenceTypesMap.getOrElse(fieldType,
            throw new NoTypeFound(fieldType.name))
        if (fieldPersistType.columnType.isDefined &&
            !fieldPersistType.columnType.map(_.name).exists(currName => consistentPersistTypes.exists(currName == _.name))
        ) {
            throw new ConsistencyException(itemDescriptionProvider() + " defined with inconsistent persistence " +
                s"type ${fieldPersistType.columnType} where field type is ${fieldType.name}!")
        }

    private def mergeIdTypeDefinition(idType: EntityIdTypeDefinition,
                                      parsed: Option[PrimitiveValuePersistenceData]): PrimitiveValuePersistenceDataFinal =
        val idColumnSrc = parsed.getOrElse(PrimitiveValuePersistenceData(None, None))
        PrimitiveValuePersistenceDataFinal(
            idColumnSrc.columnName.getOrElse("id"),
            idColumnSrc.columnType.getOrElse(idTypeDefinitionToFieldType.getOrElse(idType, throw new NoTypeFound(idType.name)))
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
            val parsedFieldData = checkAndBypassDefaultsObjectFieldPersistenceData(fieldName, fieldType, fieldsPersistenceDataMap, typeName)
            val columnData: ValuePersistenceDataFinal = toValuePersistenceDataFinal(parsedFieldData, fieldName,
                fieldType, typeName, prefix)
            fieldName -> columnData
        )

    private def checkAndBypassDefaultsObjectFieldPersistenceData(
                            fieldName: String,
                            fieldType: SimpleEntityType,
                            fieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                            typeName: String ): ValuePersistenceData =
        val itemDescriptionProvider = () => s"Field $fieldName of type $typeName"
        fieldsPersistenceDataMap
            .get(fieldName)
            .map(persistData => {
                fieldType.valueType match
                    case fieldType: RootPrimitiveTypeDefinition =>
                        checkRootPrimitivePersistenceData(persistData, fieldType, itemDescriptionProvider)
                    case SimpleObjectTypeDefinition(_, _) =>
                        checkSimpleObjectPersistenceData(persistData, itemDescriptionProvider)
                    case TypeReferenceDefinition(referencedType, _) =>
                        checkTypeReferencePersistenceData(persistData, referencedType, itemDescriptionProvider)
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" has not expected field type $fieldType!")

                persistData
            })
            .getOrElse(fieldType.valueType match
                case fieldType: RootPrimitiveTypeDefinition =>
                    PrimitiveValuePersistenceData(None, None)
                case SimpleObjectTypeDefinition(_, _) =>
                    SimpleObjectValuePersistenceData(Right(ReferenceValuePersistenceData(None)), Map.empty)
                case TypeReferenceDefinition(_, _) =>
                    ReferenceValuePersistenceData(None)
            )

    private def checkAndBypassDefaultsArrayItemPersistenceData(
                              itemType: ArrayItemEntityType,
                              fieldsPersistenceData: Option[PrimitiveValuePersistenceData | ReferenceValuePersistenceData],
                              typeName: String ): PrimitiveValuePersistenceData | ReferenceValuePersistenceData =
        val itemDescriptionProvider = () => s"Item $itemType of type $typeName"
        fieldsPersistenceData
            .map(persistData => {
                itemType.valueType match
                    case fieldType: RootPrimitiveTypeDefinition =>
                        checkRootPrimitivePersistenceData(persistData, fieldType, itemDescriptionProvider)
                    case TypeReferenceDefinition(referencedType, _) =>
                        checkTypeReferencePersistenceData(persistData, referencedType, itemDescriptionProvider)
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" has not expected field type $itemType!")

                persistData
            })
            .getOrElse(itemType.valueType match
                case fieldType: RootPrimitiveTypeDefinition =>
                    PrimitiveValuePersistenceData(None, None)
                case TypeReferenceDefinition(_, _) =>
                    ReferenceValuePersistenceData(None)
            )

    private def checkRootPrimitivePersistenceData(persistData: ValuePersistenceData,
                                                  fieldType: RootPrimitiveTypeDefinition,
                                                  itemDescriptionProvider: () => String): Unit =
        persistData match
            case fieldPersistType: PrimitiveValuePersistenceData =>
                checkRootPrimitiveAndPersistenceTypeConsistency(fieldType, fieldPersistType,itemDescriptionProvider)
            case _ =>
                throw new ConsistencyException(itemDescriptionProvider() +
                    s" with simple type $fieldType defined in persistence as not simpe type!")

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
            case EntityType(_, valueType: PrimitiveFieldTypeDefinition) =>
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
                    case fieldPersistType: ReferenceValuePersistenceData => // do nothing
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" with object type reference defined in persistence as not object type!")
            case _ =>
                persistData match
                    case fieldPersistType: ReferenceValuePersistenceData => // do nothing
                    case _ =>
                        throw new ConsistencyException(itemDescriptionProvider() +
                            s" with object type reference defined in persistence as not reference type!")


    private def toValuePersistenceDataFinal(parsedFieldData: ValuePersistenceData,
                                            fieldName: String,
                                            fieldType: SimpleEntityType,
                                            typeName: String,
                                            prefix: String = ""): ValuePersistenceDataFinal =
        val columnName = prefix + parsedFieldData.columnName.getOrElse(columnNameFromFieldName(fieldName))
        parsedFieldData match
            case PrimitiveValuePersistenceData(_, columnType) =>
                toPrimitivePersistenceDataFinal(fieldType.valueType, columnName, columnType,
                    () => s"Field $fieldName of type $typeName")

            case referecePersistenceData: ReferenceValuePersistenceData =>
                fieldType.valueType match
                    case SimpleObjectTypeDefinition(subFieldsDataMap, parent) =>
                        toObjectPersistenceDataFinal(fieldName, fieldType, typeName, prefix,
                            Right(referecePersistenceData), Map.empty)
                    case _ =>
                        toReferencePersistenceDataFinal(fieldType.valueType, columnName,
                            () => s"Field $fieldName of type $typeName")

            case SimpleObjectValuePersistenceData(parentPersistenceData, subFieldsPersistenceDataMap) =>
                toObjectPersistenceDataFinal(fieldName, fieldType, typeName, prefix,
                    parentPersistenceData, subFieldsPersistenceDataMap)

    private def toArrayItemValuePersistenceDataFinal(parsedFieldData: PrimitiveValuePersistenceData | ReferenceValuePersistenceData,
                                                     itemType: ArrayItemTypeDefinitions,
                                                     typeName: String
                                                    ): PrimitiveValuePersistenceDataFinal | ReferenceValuePersistenceDataFinal =
        val columnName = parsedFieldData.columnName.getOrElse(columnNameFromFieldName("value"))
        parsedFieldData match
            case PrimitiveValuePersistenceData(_, columnType) =>
                toPrimitivePersistenceDataFinal(itemType, columnName, columnType, () => s"Item $itemType of type $typeName")
            case ReferenceValuePersistenceData(_) =>
                toReferencePersistenceDataFinal(itemType, columnName, () => s"Item $itemType of type $typeName")


    private def toObjectPersistenceDataFinal(fieldName: String,
                                             fieldType: SimpleEntityType,
                                             typeName: String,
                                             prefix: String,
                                             parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                             subFieldsPersistenceDataMap: Map[String, ValuePersistenceData]
                                            ): SimpleObjectValuePersistenceDataFinal =
        fieldType.valueType match
            case subObjectValueType: SimpleObjectTypeDefinition =>
                if (subFieldsPersistenceDataMap.keys.exists(!subObjectValueType.allFields.contains(_))) {
                    throw new ConsistencyException(s"Field $fieldName of type $typeName contains " +
                        s"persistent data for fields not defined in type definition!")
                }

                createSimpleObjectFromParent(subObjectValueType.fields, subFieldsPersistenceDataMap,
                    subObjectValueType.parent, fieldName, fieldType, parentPersistenceData, prefix, typeName)

            case TypeReferenceDefinition(EntityType(_, ObjectTypeDefinition(fields, idOrParent)), _) =>
                createSimpleObjectFromParent(fields, subFieldsPersistenceDataMap, idOrParent.toOption, fieldName,
                    fieldType, parentPersistenceData, prefix, typeName)
            case TypeReferenceDefinition(ObjectEntitySuperType(_, ObjectTypeDefinition(fields, idOrParent)), _) =>
                createSimpleObjectFromParent(fields, subFieldsPersistenceDataMap, idOrParent.toOption, fieldName,
                    fieldType, parentPersistenceData, prefix, typeName)
            case _ =>
                throw new ConsistencyException(s"Previous check shuld prevent this, but something went wrong!")

    private def createSimpleObjectFromParent(fields: Map[String, SimpleEntityType],
                                             subFieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                             parent: Option[ObjectEntitySuperType],
                                             fieldName: String,
                                             fieldType: SimpleEntityType,
                                             parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                             prefix: String,
                                             typeName: String): SimpleObjectValuePersistenceDataFinal = {
        val fieldsDataFinal = mergeFieldsPersistenceData(fields, subFieldsPersistenceDataMap,
            typeName, s"$prefix${fieldName}__")
        val parentPersistenceDataFinal = parentPersitanceDataToFinal(parent, parentPersistenceData,
            s"$typeName's $fieldName field simple type'")

        if (parentPersistenceData.isLeft) {
            //                    fieldsDataFinal ++= List(...)
        }

        SimpleObjectValuePersistenceDataFinal(parentPersistenceDataFinal, fieldsDataFinal)
    }


    private def toReferencePersistenceDataFinal(fieldType: SimpleTypeDefinitions,
                                                columnName: String,
                                                itemDescriptionProvider: () => String) = {
        fieldType match
            case TypeReferenceDefinition(referencedType, _) =>
                ReferenceValuePersistenceDataFinal(
                    columnName,
                    tableReferenceFactory.createForType(referencedType.name)
                )
            case _ =>
                throw new ConsistencyException(itemDescriptionProvider() + s" parsed as reference type " +
                    s"whereas field type is not TypeReferenceDefinition!")
    }

    private def toPrimitivePersistenceDataFinal(fieldType: SimpleTypeDefinitions,
                                                columnName: String,
                                                columnType: Option[FieldType],
                                                itemDescriptionProvider: () => String) = {
        val persisType = columnType.getOrElse(fieldType match
            case fieldType: RootPrimitiveTypeDefinition =>
                val rootFieldType = fieldType.rootType
                primitiveTypeDefinitionToFieldType.getOrElse(rootFieldType,
                    throw new NoTypeFound(rootFieldType.name))
            case _ =>
                throw new ConsistencyException(itemDescriptionProvider() + s" with simple type $fieldType parsed as " +
                    s"primitive type whereas field type is not RootPrimitiveTypeDefinition!")
        )
        PrimitiveValuePersistenceDataFinal(columnName, persisType)
    }
                    
    private def tableNameFromTypeName(typeName: String): String = typeName.toLowerCase().replace(".", "_")
    private def columnNameFromFieldName(fieldName: String): String = camelCaseToSnakeCase(fieldName)
    private def camelCaseToSnakeCase(name: String): String = name.replaceAll("[A-Z]", "_$0").toLowerCase()
                
