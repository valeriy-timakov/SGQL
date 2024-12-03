package my.valerii_timakov.sgql.services

import scala.collection.immutable.LazyList

sealed class ExpandParentMarker

object ExpandParentMarkerSingle extends ExpandParentMarker

object ExpandParentMarkerTotal extends ExpandParentMarker

sealed abstract class PersistenceFieldType(val name: String):
    def getType: PersistenceFieldType = this

object LongFieldType extends PersistenceFieldType("long")

object IntFieldType extends PersistenceFieldType("integer")

object ShortIntFieldType extends PersistenceFieldType("short")

object ByteFieldType extends PersistenceFieldType("byte")

object DoubleFieldType extends PersistenceFieldType("double")

object FloatFieldType extends PersistenceFieldType("float")

object DecimalFieldType extends PersistenceFieldType("decimal")

object BooleanFieldType extends PersistenceFieldType("boolean")

object DateFieldType extends PersistenceFieldType("date")

object DateTimeFieldType extends PersistenceFieldType("datetime")

object DateTimeWithTimeZoneFieldType extends PersistenceFieldType("datetime_tz")

object TimeFieldType extends PersistenceFieldType("time")

object TimeWithTimeZoneFieldType extends PersistenceFieldType("time_tx")

object UUIDFieldType extends PersistenceFieldType("uuid")

object TextFieldType extends PersistenceFieldType("text")

object BLOBFieldType extends PersistenceFieldType("blob")

final case class StringFieldType(maxLength: Int) extends PersistenceFieldType("string"):
    override def getType: PersistenceFieldType = StringFieldType

object StringFieldType extends PersistenceFieldType("string")

final case class FixedStringFieldType(maxLength: Int) extends PersistenceFieldType("fixed_string"):
    override def getType: PersistenceFieldType = StringFieldType

object FixedStringFieldType extends PersistenceFieldType("string")

val allEmptyTypes = List(LongFieldType, IntFieldType, DoubleFieldType, FloatFieldType, BooleanFieldType, DateFieldType, 
    DateTimeFieldType, TimeFieldType, UUIDFieldType, TextFieldType, BLOBFieldType)

sealed trait ValuePersistenceData:
    val columnName: Option[String]
    
case class ColumnPersistenceData(
    columnName: Option[String]
) extends ValuePersistenceData

case class PrimitiveValuePersistenceData(
    columnName: Option[String],
    columnType: Option[PersistenceFieldType],
) extends ValuePersistenceData

case class SimpleObjectValuePersistenceData(
    parentRelation: Either[ExpandParentMarker, ReferenceValuePersistenceData],
    fieldsMap: Map[String, ValuePersistenceData],
) extends ValuePersistenceData:
    val columnName: Option[String] = None


case class ReferenceValuePersistenceData(
    columnName: Option[String],
) 

type ArrayValuePersistenceData = PrimitiveValuePersistenceData | ColumnPersistenceData
case class FieldPersistenceData (
    fieldName: String, 
    columnData: ValuePersistenceData
)

trait AbstractTypePersistenceData extends ItemData

trait TypePersistenceData extends AbstractTypePersistenceData:
    def tableName: Option[String]
    def idColumn: Option[ValuePersistenceData]

private class PrimitiveTypePersistenceDataPartial(
    val tableName: Option[String],
    val idColumn: Option[PrimitiveValuePersistenceData],
    val valueColumn: Option[PrimitiveValuePersistenceData],
) 

case class PrimitiveTypePersistenceData(
    typeName: String,
    tableName: Option[String],
    idColumn: Option[PrimitiveValuePersistenceData],
    valueColumn: Option[PrimitiveValuePersistenceData],
) extends TypePersistenceData:
    def this(typeName: String, data: PrimitiveTypePersistenceDataPartial) =
        this(typeName, data.tableName, data.idColumn, data.valueColumn)

class ArrayItemPersistenceData(
    val tableName: Option[String],
    val idColumn: Option[PrimitiveValuePersistenceData],
    val valueColumn: Option[ArrayValuePersistenceData],
)

class ArrayTypePersistenceData(
    val typeName: String,
    val itemsMap: Map[String, ArrayItemPersistenceData],
) extends AbstractTypePersistenceData

class ObjectTypePersistenceData(
    val typeName: String,
    val tableName: Option[String],
    val idColumn: Option[PrimitiveValuePersistenceData],
    val fields: Map[String, ValuePersistenceData],
    val parentRelation: Option[ExpandParentMarker],
) extends TypePersistenceData

type RootPackagePersistenceData = RootPackageData[AbstractTypePersistenceData]
type PackagePersistenceData = PackageData[AbstractTypePersistenceData]

trait ItemData:
    def typeName: String
    def pair(prefix: String): (String, this.type) = (prefix + typeName, this)

class RootPackageData[ItemDataType <: ItemData](val packages: List[PackageData[ItemDataType]], val items: List[ItemDataType]):
    def toNamed(name: String): PackageData[ItemDataType] = PackageData(name, packages, items)

    def typesCount(): Int = packages.foldLeft(items.size)((acc, p) => acc + p.typesCount())

    def toMap: Map[String, ItemDataType] = toPairsStream.toMap

    private def toPairsStream: LazyList[(String, ItemDataType)] =
        items.map(_.pair("")).to(LazyList) ++ packages.flatMap(_.toPairsStream("")).to(LazyList)

case class PackageData[ItemDataType <: ItemData](name: String, override val packages: List[PackageData[ItemDataType]], override val items: List[ItemDataType]) extends RootPackageData[ItemDataType](packages, items):
    def toPairsStream(parentPrefix: String): LazyList[(String, ItemDataType)] =
        val currentPrefix = parentPrefix + name + TypesDefinitionsParser.NAMESPACES_DILIMITER
        items.map(_.pair(currentPrefix)).to(LazyList) ++ packages.flatMap(_.toPairsStream(currentPrefix)).to(LazyList)



object PersistenceConfigParser extends DefinitionsParser[RootPackagePersistenceData]:

    private def itemName: Parser[String] = log("""[\w_]+""".r, "itemName")
    private def typeRefName: Parser[String] = log("""[\w_]+([\w_.]+[\w_]+)_""".r, "typeRefName")
    private def stringType: Parser[StringFieldType] = log(
        "varchar" ~> "(" ~> """\d+""".r <~ ")" ^^ { maxLength => StringFieldType(maxLength.toInt) },
        "stringType")
    
    private def simpleFieldType: Parser[PersistenceFieldType] = log(stringType | allEmptyTypes
        .map(t => t.name ^^^ t)
        .foldLeft(failure("Not filed type!"): Parser[PersistenceFieldType])(_ | _) 
        ^^ (t => t), 
        "simpleFieldType")
    
    private def primitiveValuePersistenceData: Parser[PrimitiveValuePersistenceData] = log(
        (opt(itemName) <~ ":") ~ opt(simpleFieldType) ^^ { case columnName ~ columnType => PrimitiveValuePersistenceData(columnName, columnType) },
        "primitiveValuePersistenceData")   
    
    private def columnPersistenceData: Parser[ColumnPersistenceData] = log(
        opt(itemName) <~ opt("@") ^^ { columnName => ColumnPersistenceData(columnName) },
        "columnPersistenceData")
    
    private def inheritenceReferenceData: Parser[ReferenceValuePersistenceData] = log(
        opt(itemName) ~ "@" ^^ { case columnName ~ _ => ReferenceValuePersistenceData(columnName) },
        "inheritenceReferenceData")
    
    private def primitiveTypeData: Parser[PrimitiveTypePersistenceDataPartial] = log(
        itemName ~ opt("," ~> primitiveValuePersistenceData) ~ opt("," ~> primitiveValuePersistenceData) ^^ {
            case tableName ~ idColumn ~ valueColumn => new PrimitiveTypePersistenceDataPartial(Some(tableName), idColumn, valueColumn)
        },
        "primitiveTypeData")
    
    private def primitiveType: Parser[PrimitiveTypePersistenceData] = log(
        (itemName <~ "(") ~ (primitiveTypeData | primitiveValuePersistenceData) <~ ")" ^^ {
            case typeName ~ (simpleValuePersData: PrimitiveTypePersistenceDataPartial) =>
                new PrimitiveTypePersistenceData(typeName, simpleValuePersData)
            case typeName ~ (simpleValuePersData: PrimitiveValuePersistenceData) =>
                PrimitiveTypePersistenceData(typeName, None, None, Some(simpleValuePersData))
        },
        "primitiveType")

    private def arrayItemValueData: Parser[ArrayValuePersistenceData] = log(
        (primitiveValuePersistenceData | columnPersistenceData).asInstanceOf[Parser[ArrayValuePersistenceData]], 
        "arrayItemValueData")

    private def arrayItemDataPartial: Parser[ArrayItemPersistenceData] = log(
        itemName ~ opt("," ~> primitiveValuePersistenceData) ~ opt("," ~> arrayItemValueData) ^^ {
            case tableName ~ idColumn ~ valueColumn => ArrayItemPersistenceData(Some(tableName), idColumn, valueColumn)
        },
        "arrayItemDataPartial")
    
    private def arrayItemPersistenceData: Parser[(String, ArrayItemPersistenceData)] = log(
        ("<" ~> typeRefName <~ "|") ~ ( arrayItemDataPartial <~ ">") ^^ { 
            case typeName ~ data => typeName -> data },
        "arrayItemPersistenceData")
    
    private def arrayType: Parser[ArrayTypePersistenceData] = log(
        (itemName <~ "(") ~ rep(arrayItemPersistenceData <~ opt(",")) <~ ")" ^^ {
            case typeName ~ data => new ArrayTypePersistenceData(typeName, data.toMap) },
        "arrayType")

    private def fieldData: Parser[FieldPersistenceData] = log(
        itemName ~ (simpleObjectData | ("(" ~> (primitiveValuePersistenceData | columnPersistenceData) <~ ")"))
            ^^ {
                case fieldName ~ fieldData => FieldPersistenceData(fieldName, fieldData)
            }, "fieldData")

    private def fieldsData: Parser[List[FieldPersistenceData]] = log(
        "{" ~> rep(fieldData <~ opt(",")) <~ "}", "fieldsData")

    private def expandParentSingle: Parser[ExpandParentMarker] = log("_", "expandParentSingle") ^^ { _ => ExpandParentMarkerSingle }

    private def expandParentTotal: Parser[ExpandParentMarker] = log("__", "expandParentTotal") ^^ { _ => ExpandParentMarkerTotal }
    
    private def simpleObjectPersistenceDataWithExpantion: Parser[SimpleObjectValuePersistenceData] = log(
        (expandParentSingle | expandParentTotal ) ~ opt(fieldsData) ^^ {
            case expandData ~ fieldsOpt =>
                SimpleObjectValuePersistenceData(Left(expandData), fieldsOpt.getOrElse(List())
                    .map(f => f.fieldName -> f.columnData).toMap)
        }, "simpleObjectPersistenceDataWithExpantion")

    private def simpleObjectPersistenceDataWithReference: Parser[SimpleObjectValuePersistenceData] = log(
        opt(":" ~> inheritenceReferenceData) ~ fieldsData ^^ {
            case inheritanceData ~ fields =>
                val parentRelation = inheritanceData match
                    case Some(refData: ReferenceValuePersistenceData) => Right(refData)
                    case None => Left(ExpandParentMarkerSingle)
                SimpleObjectValuePersistenceData(parentRelation, fields.map(f => f.fieldName -> f.columnData).toMap)
        }, "simpleObjectPersistenceDataWithReference")

    private def simpleObjectData: Parser[SimpleObjectValuePersistenceData] = log(
        simpleObjectPersistenceDataWithExpantion | simpleObjectPersistenceDataWithReference , "simpleObjectData")

    private def tableWithIdData: Parser[(String, Option[PrimitiveValuePersistenceData])] = log(
        "(" ~> itemName ~ opt("," ~> primitiveValuePersistenceData) <~ ")" ^^ {
            case tableName ~ idColumn => (tableName, idColumn)
        }, "tableWithIdData")
    
    private def objectType: Parser[ObjectTypePersistenceData] = log(
        itemName ~ opt(tableWithIdData) ~ opt(expandParentSingle | expandParentTotal) ~ fieldsData
        ^^ {
            case typeName ~ tableData ~ inheritanceDataOpt ~ fields => 
                val fieldsMap = fields.map(f => f.fieldName -> f.columnData).toMap
                tableData match 
                    case Some((tableName, idColumn)) => new ObjectTypePersistenceData(typeName, Some(tableName), 
                        idColumn, fieldsMap, inheritanceDataOpt)
                    case None => new ObjectTypePersistenceData(typeName, None, None, fieldsMap, inheritanceDataOpt)
        },
        "objectType")
    private def anyItem: Parser[AbstractTypePersistenceData] = 
        log(objectType | arrayType | primitiveType, "anyItem")
        
    protected def packageContent: Parser[RootPackagePersistenceData] = log(
        rep(anyItem | singleTypePackageItem | packageItem) ^^ {
            typesAndPackages =>
                var types = List[AbstractTypePersistenceData]()
                var packages = List[PackagePersistenceData]()
                typesAndPackages.foreach {
                    case packageItem: PackagePersistenceData => packages = packages :+ packageItem
                    case typeItem: AbstractTypePersistenceData => types = types :+ typeItem
                }
                RootPackageData[AbstractTypePersistenceData](packages, types)
        }, "packageContent")
    
    private def packageItem: Parser[PackagePersistenceData] = log((typeRefName <~ "{") ~ packageContent <~ "}" ^^ {
        case packageName ~ packageContent => packageContent.toNamed(packageName)
    }, "packageItem")
    
    private def singleTypePackageItem: Parser[PackagePersistenceData] = log(("""[\w_]+([\w_.]+[\w_]+)__(_=\.[\w_]+\s)""".r <~ ".") ~ anyItem ^^ {
        case packageName ~ typeData => PackageData[AbstractTypePersistenceData](packageName, List(), List(typeData))
    }, "singleTypePackageItem")

    val callStack = new scala.collection.mutable.Stack[String]() 
    var logId: Int = 0
    val debugLogEnabled: Boolean = false

    private def log[I](p: Parser[I], name: String): Parser[I] = Parser { in =>
        logId += 1
        callStack.push(name)
        val inLogId = logId
        val inBefore = in.source.toString.substring(0, in.offset)
        val isShort = in.source.toString.substring(in.offset, in.offset + Math.min(50, in.source.length() - in.offset))
        val callsLine = callStack.reverse.zipWithIndex.map { case (elem, index) => s"{$index}: $elem" }.mkString(":\n\t-> ")
        val red = "\u001b[0;31m"
        val reset = "\u001b[0m"
        if (debugLogEnabled) {
            print(s"[$inLogId]>>>$callsLine at ${in.offset} (${in.pos})  input: $inBefore")
            print(red + " |> " + reset)
            println(isShort)
        }

        val r = p(in)
        if (debugLogEnabled) {
            if (!r.successful) {
                System.err.println(s"[$inLogId]<<<$name result: $r")

            } else {
                System.out.println(s"[$inLogId]<<<$name result: $r")
            }
        }
        callStack.pop()
        r
    }

/*^^ {
        case name => logger.debug(s"found ${name}: " + name)
            name
    }*/
    
    