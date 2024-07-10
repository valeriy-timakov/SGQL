package my.valerii_timakov.sgql.services


import com.typesafe.scalalogging.LazyLogging
import my.valerii_timakov.sgql.entity.{ArrayTypeDefinition, Error, ObjectTypeDefinition, TypesDefinitionsParseError}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.parsing.combinator.*
import scala.util.parsing.input.{Reader, StreamReader}


type AnyTypeDef = ReferenceData | ArrayData | ObjectData
case class ReferenceData(refTypeName: String, refFieldName: Option[String])
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
class TypesRootPackageData(val packages: List[TypesPackageData], val types: List[TypeData]):
    def toNamed(name: String): TypesPackageData = TypesPackageData(name, packages, types)
    def typesCount(): Int =
        packages.foldLeft( types.size )( (acc, p) => acc + p.typesCount() )    
    def toMap: Map[String, TypeData] = toPairsStream.toMap
    private def toPairsStream: LazyList[(String, TypeData)] =
        types.map(_.pair("")).to(LazyList) ++ packages.flatMap(_.toPairsStream("")).to(LazyList)

case class TypesPackageData(name: String, override val packages: List[TypesPackageData], override val types: List[TypeData]) extends TypesRootPackageData(packages, types):
    def toPairsStream(parentPrefix: String): LazyList[(String, TypeData)] =
        val currentPrefix = parentPrefix + name + "."
        types.map(_.pair(currentPrefix)).to(LazyList) ++ packages.flatMap(_.toPairsStream(currentPrefix)).to(LazyList)

abstract class DefinitionsParser[Result] extends RegexParsers with LazyLogging:

    def parse(input: String): Either[TypesDefinitionsParseError, Result] =
        mapParseResult(parseAll(packageContent, input))

    def parse(input: Reader[Char]): Either[TypesDefinitionsParseError, Result] =
        mapParseResult(parseAll(packageContent, input))

    protected def packageContent: Parser[Result]

    private def mapParseResult(result: ParseResult[Result] ): Either[TypesDefinitionsParseError, Result] =
        result match
            case Success(result, _) => Right(result)
            case Failure(e, next) => Left(TypesDefinitionsParseError(
                s"Failure: $e at ${next.pos.longString} line, position: ${next.pos.column}"))
            case Error(e, next) => Left(TypesDefinitionsParseError(
                s"Error:  $e at ${next.pos.longString} line, position: ${next.pos.column}"))
    

object TypesDefinitionsParser extends DefinitionsParser[TypesRootPackageData]:

    var logId: Int = 0

    private def log[I](p: Parser[I], name: String): Parser[I] = Parser { in =>
        logId += 1
        var inLogId = logId
        val inBefore = in.source.toString.substring(0, in.offset)
        val isShort = in.source.toString.substring(in.offset, in.offset + Math.min(40, in.source.length() - in.offset))
        print(s"[$inLogId]>>>$name at ${in.offset} (${in.pos})  input: $inBefore")
        val red = "\u001b[0;31m"
        val reset = "\u001b[0m"
        print(red + " |> " + reset)
        println(isShort)



        //logger.debug(s"[$inLogId]>>>$name at ${in.offset} (${in.pos})  input: $inBefore | $isShort")
        val r = p(in)
        //logger.debug(s"[$inLogId]<<<$name result: $r")
        System.out.println(s"[$inLogId]<<<$name result: $r")
        r
    } /*^^ {
        case name => logger.debug(s"found ${name}: " + name)
            name
    }*/
        
    private def itemName: Parser[String] = log("""[\w_]+""".r, "itemName")
    private def typeRefName: Parser[String] = log("""[\w_]+([\w_.]+[\w_]+)?""".r, "typeRefName")
    private def typeReference: Parser[ReferenceData] = log(typeRefName ~ opt("+" ~> itemName) ^^ {
        case refTypeName ~ refFieldName => ReferenceData(refTypeName, refFieldName)
    }, "typeReference")
    private def typeName: Parser[String] = log(itemName <~ ":", "typeName")
    private def idType: Parser[String] = log("(" ~> itemName <~ ")", "idType")
    private def typeDef: Parser[AnyTypeDef] = log(arrayDef | objectDef | typeReference, "typeDef")
    private def field: Parser[FieldData] = log((itemName <~ ":") ~ typeDef <~ opt(",") ^^ {
        case name ~ typeName => FieldData(name, typeName)
    }, "field")
    private def fields: Parser[List[FieldData]] = log("{" ~> rep(field) <~ "}", "fields")
    private def array: Parser[List[AnyTypeDef]] = log("[" ~> rep(typeDef <~ opt(",")) <~ "]", "array")
    private def arrayDef: Parser[ArrayData] = log(opt(typeRefName) ~ array ^^ {
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
    protected def packageContent: Parser[TypesRootPackageData] = log(rep(anyTypeItem | singleTypePackageItem | packageItem) ^^ {
        typesAndPackages =>
            var types = List[TypeData]()
            var packages = List[TypesPackageData]()
            typesAndPackages.foreach {
                case packageItem: TypesPackageData => packages = packages :+ packageItem
                case typeItem: TypeData => types = types :+ typeItem
            }
            TypesRootPackageData(packages, types)
    }, "packageContent")
    private def packageItem: Parser[TypesPackageData] = log((typeRefName <~ "{") ~ packageContent <~ "}" ^^ {
        case packageName ~ packageContent => packageContent.toNamed(packageName)
    }, "packageItem")
    private def singleTypePackageItem: Parser[TypesPackageData] = log(("""[\w_]+([\w_.]+[\w_]+)??(?=\.[\w_]+:)""".r <~ ".") ~ anyTypeItem ^^ {
        case packageName ~ typeData => TypesPackageData(packageName, List(), List(typeData))
    }, "singleTypePackageItem")


