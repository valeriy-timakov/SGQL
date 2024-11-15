package my.valerii_timakov.sgql.services


import com.typesafe.scalalogging.LazyLogging
import my.valerii_timakov.sgql.entity.{Error, TypesDefinitionsParseError}
import my.valerii_timakov.sgql.entity.domain.type_definitions.{ArrayTypeDefinition, ObjectTypeDefinition}
import my.valerii_timakov.sgql.services.TypesDefinitionsParser.IdTypeRef

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.parsing.combinator.*
import scala.util.parsing.input.{Reader, StreamReader}


type AnyTypeDef = ReferenceData | SimpleObjectData

case class ReferenceData(refTypeName: String, refFieldName: Option[String], unique: Boolean = false)
case class FieldData(name: String, typeDef: AnyTypeDef)
case class ObjectData(idOrParent: Either[IdTypeRef, String], fields: List[FieldData])
case class ArrayData(idOrParent: Either[IdTypeRef, String], elementTypesNames: List[ReferenceData])

case class SimpleObjectData(parent: Option[String], fields: List[FieldData])

case class TypeNameData(name: String, mandatoryConreteType: Boolean)

trait TypeData:
    def typeName: String
    def pair(prefix: String): (String, TypeData) = (prefix + typeName, this)
    
case class ObjectTypeData(typeName: String, 
                          data: ObjectData, 
                          mandatoryConreteType: Boolean) extends  TypeData

case class PrimitiveTypeData(typeName: String, 
                             idType: Option[String], 
                             parentTypeName: String, 
                             mandatoryConreteType: Boolean) extends  TypeData

case class ArrayTypeData(typeName: String,    
                         definition: ArrayData,         
                         mandatoryConreteType: Boolean,  
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
        val currentPrefix = parentPrefix + name + TypesDefinitionsParser.NAMESPACES_DILIMITER
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
    val debugLogEnabled: Boolean = false
    final val NAMESPACES_DILIMITER = '.'

    private def log[I](p: Parser[I], name: String): Parser[I] = Parser { in =>
        logId += 1
        val inLogId = logId
        val inBefore = in.source.toString.substring(0, in.offset)
        val isShort = in.source.toString.substring(in.offset, in.offset + Math.min(40, in.source.length() - in.offset))
        val red = "\u001b[0;31m"
        val reset = "\u001b[0m"
        if (debugLogEnabled) {
            print(s"[$inLogId]>>>$name at ${in.offset} (${in.pos})  input: $inBefore")
            print(red + " |> " + reset)
            println(isShort)
        }

        val r = p(in)
        if (debugLogEnabled) {
            System.out.println(s"[$inLogId]<<<$name result: $r")
        }
        r
    }
    case class IdTypeRef(name: String)
        
    private def itemName: Parser[String] = log("""[\w_]+""".r, "itemName")
    private def typeRefName: Parser[String] = log("""[\w_]+([\w_.]+[\w_]+)?""".r, "typeRefName")
    private def typeReference: Parser[ReferenceData] = log(opt("$") ~ typeRefName ~ opt("+" ~> itemName) ^^ {
        case uniqueMark ~ refTypeName ~ refFieldName => ReferenceData(refTypeName, refFieldName, uniqueMark.isDefined)
    }, "typeReference")
    private def typeName: Parser[TypeNameData] = log(itemName ~ opt("!") <~ ":"  ^^ {
        case name ~ mandatoryConcreteOpt => TypeNameData(name, mandatoryConcreteOpt.isDefined)
    }, "typeName")
    private def idType: Parser[IdTypeRef] = log(("(" ~> itemName <~ ")") ^^ { name => IdTypeRef(name) }, "idType")
//    private def typeDef: Parser[AnyTypeDef] = log(simpleObjectType | typeReference, "typeDef")
    private def field: Parser[FieldData] = log((itemName <~ ":") ~ (simpleObjectType | typeReference) <~ opt(",") ^^ {
        case name ~ typeName => FieldData(name, typeName)
    }, "field")
    private def fields: Parser[List[FieldData]] = log("{" ~> rep(field) <~ "}", "fields")
    private def array: Parser[List[ReferenceData]] = log("[" ~> rep(typeReference <~ opt(",")) <~ "]", "array")
//    private def arrayDef: Parser[ArrayData] = log((typeRefName | idType) ~ array ^^ {
//        case idOrParent ~ elements => idOrParent match
//            case idTypeRef: IdTypeRef => ArrayData(Left(idTypeRef), elements)
//            case parentName: String => ArrayData(Right(parentName), elements)
//    }, "arrayDef")
    private def simpleObjectType: Parser[SimpleObjectData] = log(opt(typeRefName) ~ fields ^^ {
        case Some(ObjectTypeDefinition.name) ~ fields => SimpleObjectData(None, fields)
        case parent ~ fields => SimpleObjectData(parent, fields)
    }, "simpleObjectType")
    private def objectType: Parser[ObjectTypeData] = log(typeName ~ ((opt(ObjectTypeDefinition.name) ~> idType) | typeRefName) ~ fields ^^ {
        case typeName ~ idOrParent ~ fields => idOrParent match
            case idTypeRef: IdTypeRef => ObjectTypeData(typeName.name, ObjectData(Left(idTypeRef), fields), typeName.mandatoryConreteType)
            case parentName: String => ObjectTypeData(typeName.name, ObjectData(Right(parentName), fields), typeName.mandatoryConreteType)
    }, "objectType")
    private def arrayType: Parser[ArrayTypeData] = log(typeName ~ ((opt(ArrayTypeDefinition.name) ~> idType) | typeRefName) ~ array ^^ {
        case typeName ~ idOrParent ~ elements => idOrParent match
            case idTypeRef: IdTypeRef =>  ArrayTypeData(typeName.name, ArrayData(Left(idTypeRef), elements), typeName.mandatoryConreteType)
            case parentName: String => ArrayTypeData(typeName.name, ArrayData(Right(parentName), elements), typeName.mandatoryConreteType)
    }, "arrayType")
    private def primitiveType: Parser[PrimitiveTypeData] = log(typeName ~ typeRefName ~ opt(idType) ^^ {
        case typeName ~ parentTypeName ~ idType => PrimitiveTypeData(typeName.name, idType.map(_.name), parentTypeName, typeName.mandatoryConreteType)
    }, "primitiveType")
    private def anyTypeItem: Parser[TypeData] = log(arrayType | objectType | primitiveType, "anyItem")
    protected def packageContent: Parser[TypesRootPackageData] = log(rep((anyTypeItem | singleTypePackageItem | packageItem) <~ opt(",")) ^^ {
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


