package my.valerii_timakov.sgql.services

import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.LazyList
import scala.util.parsing.combinator.RegexParsers


sealed abstract class FieldType(val name: String)

object LongFieldType extends FieldType("Long")

object IntFieldType extends FieldType("Int")

object DoubleFieldType extends FieldType("Double")

object FloatFieldType extends FieldType("Float")

object BooleanFieldType extends FieldType("Boolean")

object DateFieldType extends FieldType("Date")

object DateTimeFieldType extends FieldType("DateTime")

object TimeFieldType extends FieldType("Time")

object UUIDFieldType extends FieldType("UUID")

object BLOBFieldType extends FieldType("BLOB")

final case class StringFieldType(maxLength: Int) extends FieldType("String")

val allEmptyTypes = List(LongFieldType, IntFieldType, DoubleFieldType, FloatFieldType, BooleanFieldType, DateFieldType, 
    DateTimeFieldType, TimeFieldType, UUIDFieldType, BLOBFieldType)

trait ValuePersistenceData:
    val columnName: Option[String]

class SimpleValuePersistenceData(
    val columnName: Option[String],
    val columnType: Option[FieldType],
) extends ValuePersistenceData

class ReferenceValuePersistenceData(
    val columnName: Option[String],
    val referenceTable: Option[String],
) extends ValuePersistenceData
class FieldPersistenceData (
    val fieldName: String, 
    val columnName: ValuePersistenceData
)

trait AbstractTypePersistanceData extends ItemData

trait TypePersistanceData extends AbstractTypePersistanceData:
    def tableName: Option[String]
    def idColumn: Option[ValuePersistenceData]

class SimpleValuePersistanceData(
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val valueColumn: Option[ValuePersistenceData],
)

class SimpleTypePersistanceData(
    val typeName: String,
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val valueColumn: Option[ValuePersistenceData],
) extends TypePersistanceData:
    def this(typeName: String, data: SimpleValuePersistanceData) = 
        this(typeName, data.tableName, data.idColumn, data.valueColumn)

class ArrayTypePersistanceData(
    val typeName: String, 
    val data: Seq[SimpleTypePersistanceData],
) extends AbstractTypePersistanceData

class ObjectTypePersistanceData(
    val typeName: String,
    val tableName: Option[String],
    val idColumn: Option[SimpleValuePersistenceData],
    val fields: List[FieldPersistenceData],
) extends TypePersistanceData

type RootPackagePersistanceData = RootPackageData[AbstractTypePersistanceData]
type PackagePersistanceData = PackageData[AbstractTypePersistanceData]

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



object PersistenceConfigParser extends DefinitionsParser[RootPackagePersistanceData]:

    private def itemName: Parser[String] = log("""[\w_]+""".r, "itemName")
    private def typeRefName: Parser[String] = log("""[\w_]+([\w_.]+[\w_]+)?""".r, "typeRefName")
    private def stringType: Parser[StringFieldType] = log(
        "String" ~> "(" ~> """\d+""".r <~ ")" ^^ { maxLength => StringFieldType(maxLength.toInt) }, 
        "stringType")
    
    private def simpleFieldType: Parser[FieldType] = log(stringType | allEmptyTypes
        .map(t => t.name ^^^ t)
        .foldLeft(failure("Not filed type!"): Parser[FieldType])(_ | _) 
        ^^ (t => t), 
        "simpleFieldType")
    private def simpleFieldPersistenceData: Parser[SimpleValuePersistenceData] = log(
        opt(itemName) ~ opt(":" ~> simpleFieldType) ^^ { case valueName ~ typeData => new SimpleValuePersistenceData(valueName, typeData) },
        "simpleFieldPersistenceData")    
    private def referenceFieldPersistenceData: Parser[ReferenceValuePersistenceData] = log(
        opt(itemName) ~ "@" ~ opt(itemName) ^^ { case tableName ~ _ ~ refTableName => new ReferenceValuePersistenceData(tableName, refTableName) },
        "ReferenceFieldPersistenceData")    
    private def simpleValueData: Parser[SimpleValuePersistanceData] = log(
        itemName ~ opt("," ~> simpleFieldPersistenceData) ~ opt("," ~> (simpleFieldPersistenceData | referenceFieldPersistenceData)) ^^ {
            case tableName ~ idColumn ~ valueColumn => new SimpleValuePersistanceData(Some(tableName), idColumn, valueColumn)
        },
        "simpleValueData")
    private def simpleTypePersistanceData: Parser[SimpleTypePersistanceData] = log(
        (itemName <~ ":") ~ (simpleValueData | simpleFieldPersistenceData) ^^ {
            case typeName ~ (simpleValuePersData: SimpleValuePersistanceData) => new SimpleTypePersistanceData(typeName, simpleValuePersData)
            case typeName ~ (simpleValuePersData: SimpleValuePersistenceData)=> 
                new SimpleTypePersistanceData(typeName, None, None, Some(simpleValuePersData))
        },
        "simpleTypePersistanceData")
    
    private def arrayItemPersistenceData: Parser[SimpleTypePersistanceData] = log(
        ("<" ~> typeRefName <~ "|") ~ ( simpleValueData <~ ">") ^^ { 
            case typeName ~ data => new SimpleTypePersistanceData(typeName, data) },
        "arrayItemPersistenceData")
    
    private def arrayTypePersistenceData: Parser[ArrayTypePersistanceData] = log(
        (itemName <~ "(") ~ rep(arrayItemPersistenceData <~ opt(",")) <~ ")" ^^ {
            case typeName ~ data => new ArrayTypePersistanceData(typeName, data) },
        "arrayTypePersistenceData")

    private def objectFieldPersistanceData: Parser[FieldPersistenceData] = log(
        itemName ~ ("(" ~> (simpleFieldPersistenceData | referenceFieldPersistenceData) <~ ")")
            ^^ {
                case typeName ~ fieldData => new FieldPersistenceData(typeName, fieldData)
            }, "objectFieldPersistanceData")
    
    private def objectTypePersistanceData: Parser[ObjectTypePersistanceData] = log(
        itemName ~ opt("(" ~> itemName ~ opt("," ~> simpleFieldPersistenceData) <~ ")") ~ 
            ("{" ~> rep(objectFieldPersistanceData <~ opt(",")) <~ "}") 
        ^^ {
            case typeName ~ tableData ~ fields => tableData match 
                case Some(tableName ~ idColumn) => new ObjectTypePersistanceData(typeName, Some(tableName), idColumn, fields)
                case None => new ObjectTypePersistanceData(typeName, None, None, fields)
        },
        "objectTypePersistanceData")
    private def anyItem: Parser[AbstractTypePersistanceData] = 
        log(objectTypePersistanceData | arrayTypePersistenceData | simpleTypePersistanceData, "anyItem")
    protected def packageContent: Parser[RootPackagePersistanceData] = log(rep(anyItem | singleTypePackageItem | packageItem) ^^ {
        typesAndPackages =>
            var types = List[AbstractTypePersistanceData]()
            var packages = List[PackagePersistanceData]()
            typesAndPackages.foreach {
                case packageItem: PackagePersistanceData => packages = packages :+ packageItem
                case typeItem: AbstractTypePersistanceData => types = types :+ typeItem
            }
            RootPackageData[AbstractTypePersistanceData](packages, types)
    }, "packageContent")
    private def packageItem: Parser[PackagePersistanceData] = log((typeRefName <~ "{") ~ packageContent <~ "}" ^^ {
        case packageName ~ packageContent => packageContent.toNamed(packageName)
    }, "packageItem")
    private def singleTypePackageItem: Parser[PackagePersistanceData] = log(("""[\w_]+([\w_.]+[\w_]+)??(?=\.[\w_]+:)""".r <~ ".") ~ anyItem ^^ {
        case packageName ~ typeData => PackageData[AbstractTypePersistanceData](packageName, List(), List(typeData))
    }, "singleTypePackageItem")


    private def log[I](p: Parser[I], name: String): Parser[I] = Parser { in =>
        logger.debug(s">$name input: ${in.source.toString} at ${in.offset} (${in.pos})${in.source.toString.substring(in.offset)}")
        val r = p(in)
        logger.debug(s"<$name result: ${r}")
        r
    }
    
    