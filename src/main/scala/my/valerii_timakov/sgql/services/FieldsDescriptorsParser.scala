package my.valerii_timakov.sgql.services

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_definitions.EntityIdTypeDefinition
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, EntitySuperType, EntityType}
import my.valerii_timakov.sgql.entity.read_modiriers.{AllGetFieldsDescriptor, AndSearchCondition, BetweenSearchCondition, CombinedSearchCondition, EqSearchCondition, GeSearchCondition, GetDescriptorChainCell, GtSearchCondition, InSearchCondition, LeSearchCondition, LikeSearchCondition, ListGetFieldsDescriptor, LtSearchCondition, NestedGetFieldsDescriptor, NotSearchCondition, ObjectGetFieldsDescriptor, OrSearchCondition, Range, SearchCondition, SearchFieldChainCell, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
import my.valerii_timakov.sgql.entity.{AbstractTypeError, GetFieldsParseError, SearchConditionParseError, TypeNotFountError}
import my.valerii_timakov.sgql.exceptions.WrongStateExcetion
import my.valerii_timakov.sgql.services.{CrudRepository, SearchConditionsParser, TypesDefinitionProvider}
import spray.json.JsValue

import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class FieldsDescriptorsParser(conf: Config):

    private val searchParamName = conf.getString("search-param-name")
    private val fieldsParamName = conf.getString("fields-param-name")
    private val fieldsDelimiter = conf.getString("fields-delimiter")
    private val searchPathPrefix = conf.getString("search-path-prefix")
    private val subobjectStartMark = conf.getString("subobject-start-mark")
    private val subobjectEndMark = conf.getString("subobject-end-mark")
    private val intervalMark = conf.getString("interval-mark")
    private val delimiters = List(fieldsDelimiter, searchPathPrefix, subobjectStartMark, subobjectEndMark, intervalMark)
        .map(Pattern.quote)
        .mkString("|")
    private val delimitersPattern = s"($delimiters)".r

    
    def parse(getFieldsOpt: Option[String]): Either[GetFieldsParseError, Try[ObjectGetFieldsDescriptor]] =
        getFieldsOpt match
            case Some(fields_) =>
                val fields = fields_.trim
                if (fields.nonEmpty && fields.startsWith(subobjectStartMark))
                    parseObjectDescriptor(fields.substring(1), None)
                        .map(_.map(_._1.asInstanceOf[ObjectGetFieldsDescriptor]))
                else
                    Left(GetFieldsParseError("Starting object sign not found!"))
            case None =>
                Right(Success(ObjectGetFieldsDescriptor(Left(AllGetFieldsDescriptor))))
    
    private def parseObjectDescriptor(
                                         getFields: String,
                                         fieldName: Option[String]
                                     ): Either[GetFieldsParseError, Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, String)]] = 
        var stopFound = false
        val fields: ArrayBuffer[NestedGetFieldsDescriptor] = ArrayBuffer()
        var currPortion = getFields
        var nextDescriptorOpt: Option[NestedGetFieldsDescriptor] = None
        var nextFrom: Option[Int] = None
        var nextTo: Option[Int] = None
        var boundaryMarkFound: Boolean = false
    
        def parseBoundary(value: String): Either[GetFieldsParseError, Int] =
            if (value.isEmpty)
                Right(0)
            else
                try
                    Right(value.toInt)
                catch
                    case e: Exception => Left(GetFieldsParseError(s"Invalid boundary value $value!"))
    
        def filterFailures[T](
                                 input: Either[GetFieldsParseError, Try[T]]
                             )(
                                 onSuccess: T => Option[Either[GetFieldsParseError, Try[T]]]
                             ): Option[Either[GetFieldsParseError, Try[T]]] =
            input match
                case Left(res) =>
                    Some(Left(res))
                case Right(Failure(res)) =>
                    Some(Right(Failure(res)))
                case Right(Success(res)) =>
                    onSuccess(res)
    
    
        def isFailed[T](input: Either[GetFieldsParseError, Try[T]]): Boolean =
            input.map(_.isFailure).getOrElse(true)
    
    
        def getNextMatch(query: String): (String, Option[(String, String)]) =
            delimitersPattern.findFirstMatchIn(query) match
                case Some(matched) =>
                    val before = query.substring(0, matched.start)
                    val delimiter = matched.matched
                    val after = query.substring(matched.end)
                    (before, Some(delimiter, after))
                case None =>
                    (query, None)
    
        //should be called only if {boundaryMarkFound} is true - not check here for performance
        def setNextBoundary(currMatch: String): Either[GetFieldsParseError, Try[Unit]] =
            boundaryMarkFound = false
            val boundaryValue = parseBoundary(currMatch) match
                case Right(value) =>
                    value
                case Left(error) =>
                    return Left(error)
            if (nextFrom.isEmpty)
                nextFrom = Some(boundaryValue)
                Right(Success(()))
            else if (nextTo.isEmpty)
                nextTo = Some(boundaryValue)
                Right(Success(()))
            else
                Left(GetFieldsParseError(s"Unexpected '$intervalMark' before $nextDescriptorOpt!" +
                    s"Both boundaries to $boundaryValue defined, where other value already set: ${nextTo.get}"))
    
        def addDataToNextDescriptor(data: String, rawParsedData: String): Either[GetFieldsParseError, Try[Unit]] =
            if (nextDescriptorOpt.isDefined)
                if (boundaryMarkFound)
                    val res = setNextBoundary(data)
                    if (isFailed(res))
                        return res
            else
                if (data.isBlank)
                    return Left(GetFieldsParseError(s"Empty field name in start of $rawParsedData!"))
                nextDescriptorOpt = Some(SingleGetFieldsDescriptor(data, true, Nil))
            boundaryMarkFound = true
            Right(Success(()))
    
        def addToFieldsNextDecriptor(currMatch: String): Either[GetFieldsParseError, Try[Unit]] =
            if (nextDescriptorOpt.isDefined)
                val nextDescriptor = nextDescriptorOpt.get
                nextDescriptorOpt = None
                if (boundaryMarkFound)
                    val res = setNextBoundary(currMatch)
                    if (isFailed(res))
                        return res
                    val result = ListGetFieldsDescriptor(nextDescriptor.fieldName, nextFrom, nextTo)
                    nextFrom = None
                    nextTo = None
                    fields += result
                else
                    fields += nextDescriptor
                Right(Success(()))
            else if (nextFrom.isDefined || nextTo.isDefined)
                nextFrom = None
                nextTo = None
                Right(Failure(WrongStateExcetion(s"One of or both nextFrom=$nextFrom and nextTo=$nextTo are set but no descriptor defined!")))
            else
                fields += SingleGetFieldsDescriptor(currMatch, true, Nil)
                Right(Success(()))
    
        while
            val (before, delimiterAndNextPotionOpt) = getNextMatch(currPortion)
            delimiterAndNextPotionOpt match
                case Some(delimiter, nextPortion) =>
                    delimiter match
                        case `fieldsDelimiter` =>
                            val res = addToFieldsNextDecriptor(before)
                            if (isFailed(res))
                                return res.asInstanceOf[Either[GetFieldsParseError, Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, String)]]]
                            currPortion = nextPortion
                        case `subobjectStartMark` =>
                            if (nextDescriptorOpt.isDefined || nextFrom.isDefined || nextTo.isDefined || boundaryMarkFound)
                                return Left(GetFieldsParseError(s"Unexpected $subobjectStartMark before $nextPortion!" +
                                    s"nextDescriptorOpt=$nextDescriptorOpt, nextFrom=$nextFrom, nextTo=$nextTo, boundaryMarkFound=$boundaryMarkFound"))
                            if (before.isEmpty)
                                return Left(GetFieldsParseError(s"Empty field name of object before $nextPortion!"))
                            val err = filterFailures(
                                parseObjectDescriptor(nextPortion, Some(before))
                            ) { (descriptor, nextPortion) =>
                                nextDescriptorOpt = Some(descriptor.asInstanceOf[SubObjectGetFieldsDescriptor])
                                currPortion = nextPortion
                                None
                            }
                            if (err.isDefined)
                                return err.get
                        case `subobjectEndMark` =>
                            val res = addToFieldsNextDecriptor(before)
                            if (isFailed(res))
                                return res.asInstanceOf[Either[GetFieldsParseError, Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, String)]]]
                            currPortion = nextPortion
                            stopFound = true
                        case `intervalMark` =>
                            val res = addDataToNextDescriptor(before, currPortion)
                            if (isFailed(res))
                                return res.asInstanceOf[Either[GetFieldsParseError, Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, String)]]]
                            currPortion = nextPortion
                        case _ =>
                            return Right(Failure(WrongStateExcetion(s"Unexpected delimiter $delimiter, before $nextPortion!")))
                case None =>
                    return Left(GetFieldsParseError("Unexpected end of object!"))
            !stopFound
        do ()
    
        fieldName match
            case Some(fieldName) =>
                Right(Success(SubObjectGetFieldsDescriptor(fieldName, None, fields.toList), currPortion))
            case None =>
                Right(Success(ObjectGetFieldsDescriptor(Right(fields.toList)), currPortion))
    