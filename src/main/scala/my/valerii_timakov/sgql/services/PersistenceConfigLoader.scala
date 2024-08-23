package my.valerii_timakov.sgql.services

import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, NoTypeFound, TypesLoadExceptionException}

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.input.StreamReader


sealed trait ValuePersistenceDataFinal:
    def columnNames: Seq[String]

class PrimitiveValuePersistenceDataFinal(
    val columnName: String,
    val columnType: PersistenceFieldType,
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
    def this(fieldsAndParentPersistenceData: FieldsAndParentPersistenceData) =
        this(fieldsAndParentPersistenceData.parent, fieldsAndParentPersistenceData.fields)
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
) extends TypePersistenceDataFinal:
    def this (tableName: String, idColumn: PrimitiveValuePersistenceDataFinal, fieldsAndParend: FieldsAndParentPersistenceData) =
        this(tableName, idColumn, fieldsAndParend.fields, fieldsAndParend.parent)

private case class FieldsAndParentPersistenceData(
    fields: Map[String, ValuePersistenceDataFinal],
    parent: Option[ReferenceValuePersistenceDataFinal]
)

trait PersistenceConfigLoader:
    def load(dataResourcePath: String,
             typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, TypePersistenceDataFinal]


val primitiveTypeDefinitionToFieldType: Map[RootPrimitiveTypeDefinition, PersistenceFieldType] = Map(
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

val idTypeDefinitionToFieldType: Map[EntityIdTypeDefinition, PersistenceFieldType] = Map(
    LongIdTypeDefinition -> LongFieldType,
    IntIdTypeDefinition -> IntFieldType,
    StringIdTypeDefinition -> StringFieldType(0),
    UUIDIdTypeDefinition -> UUIDFieldType)

val consistencePersistenceTypesMap: Map[RootPrimitiveTypeDefinition, Set[PersistenceFieldType]] = Map(
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
            if (columnsUsages.contains(columnName))
                throw new ConsistencyException(s"Column name $columnName is already used for " +
                    s"${columnsUsages(columnName)}, but also find usage for $columnUsage!")
            columnsUsages(columnName) = columnUsage

    override def load(dataResourcePath: String,
                      typesDefinitionsMap: Map[String, AbstractNamedEntityType]): Map[String, TypePersistenceDataFinal] =
        val tdSource = Source.fromResource (dataResourcePath)
        PersistenceConfigParser.parse(StreamReader( tdSource.reader() )) match
            case Right(packageData: RootPackagePersistenceData) =>
                typesDataPersistenceMap = packageData.toMap
                typesDefinitionsMap
                    .map ( (typeName, typeDef) =>
                            typeName -> mergeTypePersistenceData(typeDef, typesDataPersistenceMap.get(typeName))
                    )
            case Left(err: TypesDefinitionsParseError) =>
                throw new TypesLoadExceptionException(err)

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
                                               parsed: Option[AbstractTypePersistenceData]
                                              ): ObjectTypePersistenceDataFinal =
        val parsedData = parsed
            .map(p =>
                if (!p.isInstanceOf[ObjectTypePersistenceData])
                    throw new ConsistencyException(s"Persistence data for type $typeName is not ObjectTypePersistenceData type!")
                p.asInstanceOf[ObjectTypePersistenceData]
            )
            .getOrElse(ObjectTypePersistenceData(typeName, None, None, Map.empty, None))

        val tableName = parsedData.tableName.getOrElse(tableNameFromTypeName(typeName))
        val idColumn = mergeIdTypeDefinition(typeDefinition.idType, parsedData.idColumn)

        val parentRelation = getParentPersistenceDataOrDefault(parsedData.parentRelation)
        val columnsNamesChecker = ColumnsNamesChecker()
        columnsNamesChecker.addAndCheckUsage(idColumn.columnName, "ID column")

        val fieldsAndParentPersistenceData = mergeFieldsAndParentPersistenceData(
            typeDefinition.parent, typeDefinition.fields, parentRelation, parsedData.fields, columnsNamesChecker, typeName, "")

        ObjectTypePersistenceDataFinal(tableName, idColumn, fieldsAndParentPersistenceData)

    private def mergeFieldsAndParentPersistenceData(parentType: Option[ObjectEntitySuperType],
                                                    fields: Map[String, FieldType],
                                                    parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                                    fieldsPersistenceData: Map[String, ValuePersistenceData],
                                                    columnsNamesChecker: ColumnsNamesChecker,
                                                    typeName: String,
                                                    prefix: String
                                                   ): FieldsAndParentPersistenceData =

        val parentPersistenceDataFinal = parentPersitanceDataToFinal(parentType, parentPersistenceData, typeName)
        val fieldsFinal = mergeFieldsPersistenceData(fields, fieldsPersistenceData, typeName, prefix)
        val superFieldsFinal = mergeParentsFields(parentType, parentPersistenceData, prefix)
            .filter((fieldName, _) => !fieldsFinal.contains(fieldName))

        parentPersistenceDataFinal.foreach(p => columnsNamesChecker.addAndCheckUsage(p.columnName, "Parent column"))
        val allFieldsDataFinal = fieldsFinal ++ superFieldsFinal
        allFieldsDataFinal.foreach(fe => fe._2.columnNames.foreach(columnsNamesChecker.addAndCheckUsage(_, "Field " + fe._1) ) )

        FieldsAndParentPersistenceData(allFieldsDataFinal, parentPersistenceDataFinal)

    private def mergeParentsFields(parent: Option[ObjectEntitySuperType],
                                   parentRelation: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                   prefix: String
                                  ): Map[String, ValuePersistenceDataFinal] =
        parent match
            case Some(parentDef) =>
                parentRelation match
                    case Left(ExpandParentMarkerTotal) => copyParentFieldsPersitenceData(prefix, parentDef, true)
                    case Left(_) => copyParentFieldsPersitenceData(prefix, parentDef, false)
                    case Right(_) => Map.empty
            case None =>
                Map.empty

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
                    case TypeReferenceDefinition(referencedType) => referencedType.name
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

    private def getParentPersistenceDataOrDefault(
                                              parentRelationOpt: Option[Either[ExpandParentMarker, ReferenceValuePersistenceData]]
                                          ): Either[ExpandParentMarker, ReferenceValuePersistenceData] =
        parentRelationOpt.getOrElse(Right(ReferenceValuePersistenceData(None)))

    private def parentPersitanceDataToFinal(parentType: Option[ObjectEntitySuperType],
                                            parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                            typeName: String
                                           ): Option[ReferenceValuePersistenceDataFinal] =
        parentType match
            case Some(parentDef) =>
                parentPersistenceData match
                    case Right(ReferenceValuePersistenceData(columnName)) =>
                        Some(ReferenceValuePersistenceDataFinal(
                            columnName.getOrElse(columnNameFromFieldName("parent__" + parentDef.name)),
                            tableReferenceFactory.createForType(parentDef.name)
                        ))
                    case Left(ExpandParentMarkerSingle) =>
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
                                fieldsParsedData.columnName.getOrElse(columnNameFromFieldName("super_parent__" +
                                    superParentDef.name)),
                                tableReferenceFactory.createForType(superParentDef.name)
                            ))
                    case Left(ExpandParentMarkerTotal) =>
                        None
                    case _ =>
                        None
            case None =>
                parentPersistenceData match
                    case Right(ReferenceValuePersistenceData(columnName)) =>
                        if (columnName.isDefined)
                            throw new ConsistencyException(s"Type $typeName has no super type, but there is reference " +
                                s"column defined!")
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
        )
            throw new ConsistencyException(itemDescriptionProvider() + " defined with inconsistent persistence " +
                s"type ${fieldPersistType.columnType} where field type is ${fieldType.name}!")

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

    private def mergeFieldsPersistenceData(fields: Map[String, FieldType],
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
                                                                    fieldType: FieldType,
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
                                                                  itemType: ArrayItemType,
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


    private def toFieldPersistenceDataFinal(fieldPersitenceData: ValuePersistenceData,
                                            fieldName: String,
                                            fieldType: FieldType,
                                            typeName: String,
                                            prefix: String = ""): ValuePersistenceDataFinal =
        fieldType.valueType match
            case pritimiveType: RootPrimitiveTypeDefinition =>
                toPrimitivePersistenceDataFinal(pritimiveType, fieldPersitenceData, fieldName, prefix,
                    () => s"Field $fieldName of type $typeName")
            case subObjectType: SimpleObjectTypeDefinition =>
                fieldPersitenceData match
                    case SimpleObjectValuePersistenceData(parentRelation, fieldsPersistenceMap) =>
                        if (fieldsPersistenceMap.keys.exists(!subObjectType.allFields.contains(_)))
                            throw new ConsistencyException(s"Field $fieldName of type $typeName contains " +
                                s"persistent data for fields not defined in type definition!")

                        //TODO: check expand markers logic!!!
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
                                    primitivePersistenceData, fieldName, prefix,
                                    () => s"Field $fieldName of type $typeName")
                            case otherType => throw new ConsistencyException("Only CustomPrimitiveTypeDefinition could be " +
                                s"saved in primitive columns! Trying to save $otherType as $primitivePersistenceData.")
                    case SimpleObjectValuePersistenceData(parentRelation, fieldsPersistenceMap) =>
                        refType.referencedType.valueType match
                            case objectType: ObjectTypeDefinition =>
                                //TODO: check expand markers logic!!!
                                createSimpleObjectFromParent(objectType.fields, fieldsPersistenceMap,
                                    objectType.parent, fieldName, parentRelation, prefix,
                                    s" $fieldName fields of $typeName SimpleObject")
                            case otherType =>
                                throw new ConsistencyException("Only ObjectTypePersistenceData could be " +
                                    s"saved in denormalized columns! Trying to save $otherType as $otherType.")
                    case _: ColumnPersistenceData =>
                        toReferencePersistenceDataFinal(refType, fieldPersitenceData, fieldName, prefix,
                            () => s"Field $fieldName of type $typeName")
            case _ =>
                throw new ConsistencyException(s"Type definition is not found! ${fieldType.valueType}")

    private def toArrayItemValuePersistenceDataFinal(persistenceData: ArrayValuePersistenceData,
                                                     itemType: ArrayItemTypeDefinitions,
                                                     typeName: String
                                                    ): PrimitiveValuePersistenceDataFinal | ReferenceValuePersistenceDataFinal =
        val defaultColumnName = columnNameFromFieldName("value")
        itemType match
            case primitiveType: RootPrimitiveTypeDefinition =>
                toPrimitivePersistenceDataFinal(primitiveType, persistenceData, defaultColumnName, "", () => s"Item $itemType of type $typeName")
            case refType: TypeReferenceDefinition =>
                toReferencePersistenceDataFinal(refType, persistenceData, defaultColumnName, "", () => s"Item $itemType of type $typeName")



    private def createSimpleObjectFromParent(fields: Map[String, FieldType],
                                             subFieldsPersistenceDataMap: Map[String, ValuePersistenceData],
                                             parent: Option[ObjectEntitySuperType],
                                             fieldName: String,
                                             parentPersistenceData: Either[ExpandParentMarker, ReferenceValuePersistenceData],
                                             prefix: String,
                                             typeName: String): SimpleObjectValuePersistenceDataFinal =

        val fieldsAndParentPersistenceData = mergeFieldsAndParentPersistenceData(parent, fields, parentPersistenceData,
            subFieldsPersistenceDataMap, ColumnsNamesChecker(), typeName, s"$prefix${fieldName}__")

        SimpleObjectValuePersistenceDataFinal(fieldsAndParentPersistenceData)



    private def toReferencePersistenceDataFinal(fieldType: TypeReferenceDefinition,
                                                persistenceData: ValuePersistenceData,
                                                prefix: String,
                                                defaultColumnName: String,
                                                itemDescriptionProvider: () => String) =

        persistenceData match
            case ColumnPersistenceData(columnName) =>
                val columnNameFinal = prefix + columnName.getOrElse(defaultColumnName)
                ReferenceValuePersistenceDataFinal(
                    columnNameFinal,
                    tableReferenceFactory.createForType(fieldType.referencedType.name)
                )
            case _ =>
                throw new ConsistencyException(itemDescriptionProvider() + s" parsed as not reference type " +
                    s"whereas field type is TypeReferenceDefinition!")

    private def toPrimitivePersistenceDataFinal(fieldType: RootPrimitiveTypeDefinition,
                                                persistenceData: ValuePersistenceData,
                                                prefix: String, 
                                                defaultColumnName: String,
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
        val persisType = persisTypeOpt.getOrElse( primitiveTypeDefinitionToFieldType.getOrElse(rootFieldType,
                throw new NoTypeFound(rootFieldType.name)))

        PrimitiveValuePersistenceDataFinal(columnNameFinal, persisType)
                    
    private def tableNameFromTypeName(typeName: String): String = typeName.toLowerCase().replace(".", "_")
    private def columnNameFromFieldName(fieldName: String): String = camelCaseToSnakeCase(fieldName)
    private def camelCaseToSnakeCase(name: String): String = name.replaceAll("[A-Z]", "_$0").toLowerCase()
                
