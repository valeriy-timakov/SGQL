package my.valerii_timakov.sgql.services


import com.typesafe.scalalogging.LazyLogging
import my.valerii_timakov.sgql.entity.{ArrayTypeDefinition, Error, ObjectTypeDefinition, TypesDefinitionsParseError}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.parsing.combinator.*
import scala.util.parsing.input.{Reader, StreamReader}


type AnyTypeDef = String | ArrayData | ObjectData
case class FieldData(name: String, typeDef: AnyTypeDef)
case class ObjectData(parentTypeName: Option[String], fields: List[FieldData])
case class ArrayData(parentTypeName: Option[String], elementTypesNames: List[AnyTypeDef])

trait TypeData:
    def typeName: String
    def pair(prefix: String): (String, TypeData) = (prefix + typeName, this)
case class ObjectTypeData(typeName: String, idType: Option[String], data: ObjectData) extends  TypeData
case class PrimitiveTypeData(typeName: String, idType: Option[String], parentTypeName: String) extends  TypeData
case class ArrayTypeData(
        typeName: String, 
        idType: Option[String],
        definition: ArrayData, 
    ) extends  TypeData
class RootPackageData(val packages: List[PackageData], val types: List[TypeData]):
    def toNamed(name: String): PackageData = PackageData(name, packages, types)
    def typesCount(): Int =
        packages.foldLeft( types.size )( (acc, p) => acc + p.typesCount() )    
    def toMap: Map[String, TypeData] = toPairsStream.toMap
    private def toPairsStream: LazyList[(String, TypeData)] =
        types.map(_.pair("")).to(LazyList) ++ packages.flatMap(_.toPairsStream("")).to(LazyList)

case class PackageData(name: String, override val packages: List[PackageData], override val types: List[TypeData]) extends RootPackageData(packages, types):
    def toPairsStream(parentPrefix: String): LazyList[(String, TypeData)] =
        val currentPrefix = parentPrefix + name + "."
        types.map(_.pair(currentPrefix)).to(LazyList) ++ packages.flatMap(_.toPairsStream(currentPrefix)).to(LazyList)

object TypesDefinitionsParser extends RegexParsers with LazyLogging {

    def parse(input: String): Either[TypesDefinitionsParseError, RootPackageData] =
        mapParseResult(parseAll(packageContent, input))

    def parse(input: Reader[Char]): Either[TypesDefinitionsParseError, RootPackageData] =
        mapParseResult(parseAll(packageContent, input))

    private def log[I](p: Parser[I], name: String): Parser[I] = Parser { in =>
        logger.debug(s">>>$name input: ${in.source.toString} at ${in.offset} (${in.pos})${in.source.toString.substring(in.offset)}")
        val r = p(in)
        logger.debug(s"<<<$name result: $r")
        r
    } /*^^ {
        case name => logger.debug(s"found ${name}: " + name)
            name
    }*/
        
    private def itemName: Parser[String] = log("""\w+""".r, "itemName")
    private def typeRefName: Parser[String] = log("""[\w.]+""".r, "typeRefName")
    private def typeName: Parser[String] = log(itemName <~ ":", "typeName")
    private def idType: Parser[String] = log("(" ~> itemName <~ ")", "idType")
    private def typeDef: Parser[AnyTypeDef] = log(typeRefName | arrayDef | objectDef, "typeDef")
    private def field: Parser[FieldData] = log((itemName <~ ":") ~ typeDef <~ opt(",") ^^ {
        case name ~ typeName => FieldData(name, typeName)
    }, "field")
    private def fields: Parser[List[FieldData]] = log("{" ~> rep(field) <~ "}", "fields")
    private def array: Parser[List[AnyTypeDef]] = log("[" ~> rep(typeDef <~ opt(",")) <~ "]", "array")
    private def arrayDef: Parser[ArrayData] = log(opt(typeName) ~ array ^^ {
        case parent ~ elements => ArrayData(parent, elements)
    }, "arrayDef")
    private def objectDef: Parser[ObjectData] = log(opt(typeRefName) ~ fields ^^ {
        case parent ~ fields => ObjectData(parent, fields)
    }, "objectDef")
    private def objectType: Parser[ObjectTypeData] = log(typeName ~ opt(typeRefName) ~ opt(idType) ~ fields ^^ {
        case typeName ~ parent ~ idType ~ fields => ObjectTypeData(typeName, idType, ObjectData(parent, fields))
    }, "objectType")
    private def arrayType: Parser[ArrayTypeData] = log(typeName ~ opt(typeRefName) ~  opt(idType) ~ array ^^ {
        case name ~ parent ~ idType ~ elements => ArrayTypeData(name, idType, ArrayData(parent, elements))
    }, "arrayType")
    private def primitiveType: Parser[PrimitiveTypeData] = log(typeName ~ typeRefName ~ opt(idType) ^^ {
        case name ~ typeName ~ idType => PrimitiveTypeData(name, idType, typeName)
    }, "primitiveType")
    private def anyTypeItem: Parser[TypeData] = log(arrayType | objectType | primitiveType, "anyItem")
    private def packageContent: Parser[RootPackageData] = log(rep(anyTypeItem | singleTypePackageItem | packageItem) ^^ {
        typesAndPackages =>
            var types = List[TypeData]()
            var packages = List[PackageData]()
            typesAndPackages.foreach {
                case packageItem: PackageData => packages = packages :+ packageItem
                case typeItem: TypeData => types = types :+ typeItem
            }
            RootPackageData(packages, types)
    }, "packageContent")
    private def packageItem: Parser[PackageData] = log((typeRefName <~ "{") ~ packageContent <~ "}" ^^ {
        case packageName ~ packageContent => packageContent.toNamed(packageName)
    }, "packageItem")
    private def singleTypePackageItem: Parser[PackageData] = log(("""[\w.]+?(?=\.\w+:)""".r <~ ".") ~ anyTypeItem ^^ {
        case packageName ~ typeData => PackageData(packageName, List(), List(typeData))
    }, "singleTypePackageItem")
    
    private def mapParseResult(result: ParseResult[RootPackageData] ): Either[TypesDefinitionsParseError, RootPackageData] =
        result match
            case Success(result, _) => Right(result)
            case Failure(e, next) => Left(TypesDefinitionsParseError(
                s"Failure: $e at ${next.pos.longString} line, position: ${next.pos.column}"))
            case Error(e, next) => Left(TypesDefinitionsParseError(
                s"Error:  $e at ${next.pos.longString} line, position: ${next.pos.column}"))

}
