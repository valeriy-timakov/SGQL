package my.valerii_timakov.sgql.services

import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.LazyList
import scala.util.parsing.combinator.RegexParsers

class ExpandParentMarker

object ExpandParentMarkerSingle extends ExpandParentMarker

object ExpandParentMarkerTotal extends ExpandParentMarker

sealed abstract class FieldType(val name: String)

object LongFieldType extends FieldType("long")

object IntFieldType extends FieldType("integer")

object DoubleFieldType extends FieldType("double")

object FloatFieldType extends FieldType("float")

object BooleanFieldType extends FieldType("boolean")

object DateFieldType extends FieldType("date")

object DateTimeFieldType extends FieldType("datetime")

object TimeFieldType extends FieldType("time")

object UUIDFieldType extends FieldType("uuid")

object TextFieldType extends FieldType("text")

object BLOBFieldType extends FieldType("blob")

final case class StringFieldType(maxLength: Int) extends FieldType("String")

val allEmptyTypes = List(LongFieldType, IntFieldType, DoubleFieldType, FloatFieldType, BooleanFieldType, DateFieldType, 
    DateTimeFieldType, TimeFieldType, UUIDFieldType, TextFieldType, BLOBFieldType)

sealed trait ValuePersistenceData:
    val columnName: Option[String]

case class SimpleValuePersistenceData(
    columnName: Option[String],
    columnType: Option[FieldType],
) extends ValuePersistenceData

case class ReferenceValuePersistenceData(
    columnName: Option[String],
    referenceTable: Option[String],
) extends ValuePersistenceData

case class SimpleObjectValuePersistenceData(
    parentRelation: Either[ExpandParentMarker, ReferenceValuePersistenceData],
    fieldsMap: Map[String, ValuePersistenceData], 
) extends ValuePersistenceData:
    val columnName: Option[String] = None

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
    val idColumn: Option[SimpleValuePersistenceData],
    val valueColumn: Option[SimpleValuePersistenceData],
) 

class PrimitiveTypePersistenceData(
    val typeName: String,
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val valueColumn: Option[SimpleValuePersistenceData],
) extends TypePersistenceData:
    def this(typeName: String, data: PrimitiveTypePersistenceDataPartial) =
        this(typeName, data.tableName, data.idColumn, data.valueColumn)

class SimpleTypePersistenceDataPartial(
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val valueColumn: Option[ValuePersistenceData],
)

//for use as part of ArrayTypePersistenceData only
class SimpleTypePersistenceData(
    val typeName: String,
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val valueColumn: Option[ValuePersistenceData],
) extends TypePersistenceData:
    def this(typeName: String, data: SimpleTypePersistenceDataPartial) = 
        this(typeName, data.tableName, data.idColumn, data.valueColumn)

class ArrayTypePersistenceData(
    val typeName: String, 
    val data: Seq[SimpleTypePersistenceData],
) extends AbstractTypePersistenceData

class ObjectTypePersistenceData(
    val typeName: String,
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val fields: Map[String, ValuePersistenceData],
    val parentRelation: Either[ExpandParentMarker, ReferenceValuePersistenceData],
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
        val currentPrefix = parentPrefix + name + "."
        items.map(_.pair(currentPrefix)).to(LazyList) ++ packages.flatMap(_.toPairsStream(currentPrefix)).to(LazyList)



object PersistenceConfigParser extends DefinitionsParser[RootPackagePersistenceData]:

    private def itemName: Parser[String] = log("""[\w_]+""".r, "itemName")
    private def typeRefName: Parser[String] = log("""[\w_]+([\w_.]+[\w_]+)?""".r, "typeRefName")
    private def stringType: Parser[StringFieldType] = log(
        "varchar" ~> "(" ~> """\d+""".r <~ ")" ^^ { maxLength => StringFieldType(maxLength.toInt) },
        "stringType")
    
    private def simpleFieldType: Parser[FieldType] = log(stringType | allEmptyTypes
        .map(t => t.name ^^^ t)
        .foldLeft(failure("Not filed type!"): Parser[FieldType])(_ | _) 
        ^^ (t => t), 
        "simpleFieldType")
    private def simpleFieldPersistenceData: Parser[SimpleValuePersistenceData] = log(
        opt(itemName) ~ opt(":" ~> simpleFieldType) ^^ { case columnName ~ columnType => SimpleValuePersistenceData(columnName, columnType) },
        "simpleFieldPersistenceData")   
    
    private def referenceFieldPersistenceData: Parser[ReferenceValuePersistenceData] = log(
        opt(itemName) ~ "@" ~ opt(itemName) ^^ { case tableName ~ _ ~ refTableName => ReferenceValuePersistenceData(tableName, refTableName) },
        "ReferenceFieldPersistenceData")
    
    private def primitiveTypeData: Parser[PrimitiveTypePersistenceDataPartial] = log(
        itemName ~ opt("," ~> simpleFieldPersistenceData) ~ opt("," ~> simpleFieldPersistenceData) ^^ {
            case tableName ~ idColumn ~ valueColumn => new PrimitiveTypePersistenceDataPartial(Some(tableName), idColumn, valueColumn)
        },
        "simpleValueData")
    
    private def primitiveTypePersistenceData: Parser[PrimitiveTypePersistenceData] = log(
        (itemName <~ "(") ~ (primitiveTypeData | simpleFieldPersistenceData) <~ ")" ^^ {
            case typeName ~ (simpleValuePersData: PrimitiveTypePersistenceDataPartial) =>
                new PrimitiveTypePersistenceData(typeName, simpleValuePersData)
            case typeName ~ (simpleValuePersData: SimpleValuePersistenceData) =>
                new PrimitiveTypePersistenceData(typeName, None, None, Some(simpleValuePersData))
        },
        "primitiveTypePersistenceData")

    private def simpleValueData: Parser[SimpleTypePersistenceDataPartial] = log(
        itemName ~ opt("," ~> simpleFieldPersistenceData) ~ opt("," ~> (simpleFieldPersistenceData | referenceFieldPersistenceData)) ^^ {
            case tableName ~ idColumn ~ valueColumn => new SimpleTypePersistenceDataPartial(Some(tableName), idColumn, valueColumn)
        },
        "simpleValueData")
    
    private def arrayItemPersistenceData: Parser[SimpleTypePersistenceData] = log(
        ("<" ~> typeRefName <~ "|") ~ ( simpleValueData <~ ">") ^^ { 
            case typeName ~ data => new SimpleTypePersistenceData(typeName, data) },
        "arrayItemPersistenceData")
    
    private def arrayTypePersistenceData: Parser[ArrayTypePersistenceData] = log(
        (itemName <~ "(") ~ rep(arrayItemPersistenceData <~ opt(",")) <~ ")" ^^ {
            case typeName ~ data => new ArrayTypePersistenceData(typeName, data) },
        "arrayTypePersistenceData")

    private def objectFieldPersistenceData: Parser[FieldPersistenceData] = log(
        itemName ~ ("(" ~> (simpleObjectPersistenceData | referenceFieldPersistenceData | simpleFieldPersistenceData) <~ ")")
            ^^ {
                case fieldName ~ fieldData => FieldPersistenceData(fieldName, fieldData)
            }, "objectFieldPersistenceData")

    private def tableWithIdData: Parser[(String, Option[SimpleValuePersistenceData])] = log(
        "(" ~> itemName ~ opt("," ~> simpleFieldPersistenceData) <~ ")" ^^ {
            case tableName ~ idColumn => (tableName, idColumn)
        }, "tableWithIdData")

    private def fieldsData: Parser[List[FieldPersistenceData]] = log(
        "{" ~> rep(objectFieldPersistenceData <~ opt(",")) <~ "}", "fieldsData")

    private def expandParentSingle: Parser[ExpandParentMarker] = log("""\s+_\s+""".r, "expandParent") ^^ { _ => ExpandParentMarkerSingle }

    private def expandParentTotal: Parser[ExpandParentMarker] = log("""\s+__\s+""".r, "expandParent") ^^ { _ => ExpandParentMarkerTotal }
    
    private def simpleObjectPersistenceData: Parser[SimpleObjectValuePersistenceData] = log(
        opt(expandParentSingle | expandParentTotal | ("(" ~> referenceFieldPersistenceData <~ ")") ) ~ fieldsData ^^ {
            case inheritanceData ~ fields => 
                val parentRelation = inheritanceData match
                    case Some(refData: ReferenceValuePersistenceData) => Right(refData)
                    case Some(expandParentMarker: ExpandParentMarker) => Left(expandParentMarker)
                    case None => Left(ExpandParentMarkerSingle)
                SimpleObjectValuePersistenceData(parentRelation, fields.map(f => f.fieldName -> f.columnData).toMap)
        }, "simpleObjectPersistenceData")
    
    private def objectTypePersistenceData: Parser[ObjectTypePersistenceData] = log(
        itemName ~ opt(tableWithIdData) ~ (expandParentSingle | expandParentTotal | ("(" ~> referenceFieldPersistenceData <~ ")") ) ~ fieldsData
        ^^ {
            case typeName ~ tableData ~ inheritanceData ~ fields => 
                val fieldsMap = fields.map(f => f.fieldName -> f.columnData).toMap
                val parentRelation = inheritanceData match
                    case refData: ReferenceValuePersistenceData => Right(refData)
                    case expandParentMarker: ExpandParentMarker => Left(expandParentMarker)
                tableData match 
                    case Some((tableName, idColumn)) => new ObjectTypePersistenceData(typeName, Some(tableName), idColumn, fieldsMap, parentRelation)
                    case None => new ObjectTypePersistenceData(typeName, None, None, fieldsMap, parentRelation)
        },
        "objectTypePersistenceData")
    private def anyItem: Parser[AbstractTypePersistenceData] = 
        log(objectTypePersistenceData | arrayTypePersistenceData | primitiveTypePersistenceData, "anyItem")
        
    protected def packageContent: Parser[RootPackagePersistenceData] = log(rep(anyItem | singleTypePackageItem | packageItem) ^^ {
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
    
    private def singleTypePackageItem: Parser[PackagePersistenceData] = log(("""[\w_]+([\w_.]+[\w_]+)??(?=\.[\w_]+\s)""".r <~ ".") ~ anyItem ^^ {
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
        val callsLine = callStack.reverse.mkString(":\n\t-> ")
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
    
    