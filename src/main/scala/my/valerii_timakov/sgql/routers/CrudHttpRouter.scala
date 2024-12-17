package my.valerii_timakov.sgql.routers

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.*
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Language
import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.{Directives, Route}
import akka.util.Timeout
import com.typesafe.config.Config
import my.valerii_timakov.sgql.actors.CrudActor
import my.valerii_timakov.sgql.actors.CrudActor.*
import my.valerii_timakov.sgql.entity.{Error, GetFieldsParseError}
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId, EntityValue}
import my.valerii_timakov.sgql.entity.read_modiriers.{AllGetFieldsDescriptor, GetFieldsDescriptor, ListGetFieldsDescriptor, ObjectGetFieldsDescriptor, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
import my.valerii_timakov.sgql.exceptions.WrongStateExcetion
import my.valerii_timakov.sgql.services.MessageSource
import spray.json.{JsArray, JsValue}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

class CrudHttpRouter(
                        appActor: ActorRef[CrudActor.CrudMessage],
                        implicit val system: ActorSystem[_],
                        val messageSource: MessageSource,
                        conf: Config,
                    ):

    import akka.actor.typed.scaladsl.AskPattern.*
    implicit val timeout: Timeout = Timeout(5.seconds)

    private val searchParamName = conf.getString("search-param-name")
    private val fieldsParamName = conf.getString("fields-param-name")
    private val fieldsDelimiter = conf.getString("fields-delimiter")
    private val searchPathPrefix = conf.getString("search-path-prefix")
    private val subobjectStartMark = conf.getString("subobject-start-mark")
    private val subobjectEndMark = conf.getString("subobject-end-mark")
    private val intervalFromMark = conf.getString("interval-from-mark")
    private val intervalToMark = conf.getString("interval-to-mark")
    private val delimiters = List(fieldsDelimiter, searchPathPrefix, subobjectStartMark, subobjectEndMark,
        intervalFromMark, intervalToMark).mkString(",")
    private val delimitersPattern = s"($delimiters)".r

    val route: Route =
        pathPrefix("crud" / Segment) { objectType =>
            path(Segment) { objectId =>
                get {
                    parameterMap { params =>
                        val result: Future[Either[Error, Try[Option[Entity[_, _, _]]]]] =
                            val fieldsOpt = params.get(fieldsParamName).map(_.split(fieldsDelimiter).toList)
                            appActor ? (GetMessage(objectType, objectId, fieldsOpt, _))
                        onSuccess(result) {
                            case Left(error) =>
                                complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                            case Right(Failure(exception)) =>
                                complete(StatusCodes.InternalServerError, exception.getMessage)
                            case Right(Success(None)) =>
                                complete(StatusCodes.NotFound)
                            case Right(Success(Some(entity))) =>
                                complete(StatusCodes.OK, entity.toJson)
                        }
                    }
                } ~
                    put {
                        entity(as [JsValue]) { requestEntity =>
                            val result: Future[Either[Error, Try[Option[Unit]]]] =
                                appActor ? (UpdateMessage(objectType, objectId, requestEntity, _))
                            onSuccess(result) {
                                case Left(error) =>
                                    complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                                case Right(Failure(exception)) =>
                                    complete(StatusCodes.InternalServerError, exception.getMessage)
                                case Right(Success(None)) =>
                                    complete(StatusCodes.NotFound)
                                case Right(Success(Some(_))) =>
                                    complete(StatusCodes.NoContent)
                            }
                        }
                    } ~
                    delete {
                        extractRequestEntity { _ =>
                            val result: Future[Either[Error, Try[Option[Unit]]]] =
                                appActor ? (DeleteMessage(objectType, objectId, _))
                            onSuccess(result) {
                                case Left(error) =>
                                    complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                                case Right(Failure(exception)) =>
                                    complete(StatusCodes.InternalServerError, exception.getMessage)
                                case Right(Success(None)) =>
                                    complete(StatusCodes.NotFound)
                                case Right(Success(Some(entity))) =>
                                    complete(StatusCodes.NoContent)
                            }
                        }
                    }
            } ~
            path(searchPathPrefix) {
                get {
                    parameterMap { params =>
                        val result: Future[Either[Error, Try[Seq[Entity[_, _, _]]]]] =
                            val fieldsOpt = params.get(fieldsParamName).map(_.split(fieldsDelimiter).toList)
                            appActor ? (SearchMessage(objectType, params.get(searchParamName), fieldsOpt, _))
                        onSuccess(result) {
                            case Left(error) =>
                                complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                            case Right(Failure(exception)) =>
                                complete(StatusCodes.InternalServerError, exception.getMessage)
                            case Right(Success(entities)) =>
                                complete(StatusCodes.OK, JsArray(entities.map(_.toJson).toVector))
                        }
                    }
                }
            } ~
            pathEnd {
                post {
                    entity(as [JsValue]) { requestEntity =>
                        val result: Future[Either[Error, Try[EntityId[_, _]]]] =
                            appActor ? (CreateMessage(objectType, requestEntity, _))
                        onSuccess(result) {
                            case Left(error) =>
                                complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                            case Right(Failure(exception)) =>
                                complete(StatusCodes.InternalServerError, exception.getMessage)
                            case Right(Success(id)) =>
                                complete(StatusCodes.OK, id.toJson)
                        }
                    }
                }
            }
        }



    private def parseGetFieldsDescriptor(getFieldsOpt: Option[String]): Try[Either[GetFieldsParseError, GetFieldsDescriptor]] =
        getFieldsOpt match
            case Some(fields) =>
                parseObjectDescriptor(fields, None).map(_.map(_._1))
            case None =>
                Success(Right(AllGetFieldsDescriptor))



    private def parseObjectDescriptor(
                                         getFields: String,
                                         fieldName: Option[String]
                                     ): Try[Either[GetFieldsParseError, (ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]] = {
        var stopFound = false
        val fields: ArrayBuffer[GetFieldsDescriptor] = ArrayBuffer()
        var currPortionOpt: Option[String] = Some(getFields)
        while
            currPortionOpt.foreach(currPortion =>
                val (before, delimiterAndNextPotionOpt) = getNextMatch(currPortion)
                delimiterAndNextPotionOpt match
                    case Some(`fieldsDelimiter`, nextPortion) =>
                        fields += SingleGetFieldsDescriptor(before)
                        currPortionOpt = Some(nextPortion)
                    case Some(`subobjectStartMark`, nextPortion) =>
                        val err = filterFailures(
                            parseObjectDescriptor(nextPortion, Some(before))
                        ) {  (descriptor, nextPortion) =>
                            fields += descriptor
                            currPortionOpt = nextPortion
                            None
                        }
                        if (err.isDefined)
                            return err.get
                    case Some(`subobjectEndMark`, nextPortion) =>
                        stopFound = true
                        currPortionOpt = Some(nextPortion)
                    case Some(`intervalToMark`, nextPortion) =>
                        val err = filterFailures(
                            parseIntervalDescriptor(nextPortion, before, false)
                        ) { (intervalDescriptor, nextPortionFinalAndobjectRelatedMarkAtEndOpt) =>
                                fields += intervalDescriptor
                                nextPortionFinalAndobjectRelatedMarkAtEndOpt match
                                    case Some((nextPortion, objectRelatedMarkAtEnd)) =>
                                        objectRelatedMarkAtEnd match
                                            case Some(`subobjectEndMark`) =>
                                                stopFound = true
                                                currPortionOpt = Some(nextPortion)
                                                None
                                            case Some(`subobjectStartMark`) =>
                                                filterFailures(
                                                    parseObjectDescriptor(nextPortion, Some(before))
                                                ) {  (descriptor, nextPortion) =>
                                                    fields += descriptor
                                                    currPortionOpt = nextPortion
                                                    None
                                                }.map(_.map(_.asInstanceOf[Either[GetFieldsParseError, (ListGetFieldsDescriptor, Option[(String, Option[String])])]]))
                                            case None =>
                                                currPortionOpt = Some(nextPortion)
                                                None
                                            case Some(delimiter) =>
                                                return Failure(WrongStateExcetion(s"Unexpected delimiter $delimiter, before $nextPortion!"))
                                    case None =>
                                        currPortionOpt = None
                                        None
                        }
                        if (err.isDefined)
                            return err.get.map(_.asInstanceOf[Either[GetFieldsParseError, (ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]])
                    case Some(`intervalFromMark`, nextPortion) =>
                        parseIntervalDescriptor(nextPortion, before, true) match
                            case Failure(res) =>
                                return Failure(res)
                            case Success(Left(res)) =>
                                return Success(Left(res))
                            case Success(Right((intervalDescriptor, nextPortion, objectRelatedMarkAtEnd))) =>
                                fields += intervalDescriptor
                                currPortionOpt = nextPortion
                    case Some(delimiter, nextPortion) =>
                        return Failure(WrongStateExcetion(s"Unexpected delimiter $delimiter, before $nextPortion!"))
                    case None =>
                        if (fieldName.isEmpty)
                            currPortionOpt = None
                            stopFound = true
                        else
                            return Success(Left(GetFieldsParseError("Unexpected end of object!")))
            )
            !stopFound
        do ()

        fieldName match
            case Some(fieldName) =>
                Success(Right(SubObjectGetFieldsDescriptor(fieldName, fields.toSeq), currPortionOpt))
            case None =>
                Success(Right(ObjectGetFieldsDescriptor(fields.toSeq), currPortionOpt))
    }

    private def filterFailures[T](
                                     input: Try[Either[GetFieldsParseError, T]]
                                 )(
                                     onSuccess: T => Option[Try[Either[GetFieldsParseError, T]]]
                                 ): Option[Try[Either[GetFieldsParseError, T]]] =
        input match
            case Failure(res) =>
                Some(Failure(res))
            case Success(Left(res)) =>
                Some(Success(Left(res)))
            case Success(Right(res)) =>
                onSuccess(res)

    private def parseIntervalDescriptor(
                                            nextPortion: String,
                                            fieldName: String,
                                            firstValueIsFrom: Boolean,
                                            fields: ArrayBuffer[GetFieldsDescriptor]
                                       ): Either[Try[Either[GetFieldsParseError, (ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]], Option[String]] =
        var stopFound = false
        val (err, nextPortionOpt) = filterFailures(
            parseIntervalDescriptor(nextPortion, fieldName, firstValueIsFrom)
        ) { (intervalDescriptor, nextPortionFinalAndobjectRelatedMarkAtEndOpt) =>
            fields += intervalDescriptor
            nextPortionFinalAndobjectRelatedMarkAtEndOpt match
                case Some((nextPortion, objectRelatedMarkAtEnd)) =>
                    objectRelatedMarkAtEnd match
                        case Some(`subobjectEndMark`) =>
                            stopFound = true
                            (None, Some(nextPortion))
                        case Some(`subobjectStartMark`) =>
                            filterFailures(
                                parseObjectDescriptor(nextPortion, Some(before))
                            ) {  (descriptor, nextPortion) =>
                                fields += descriptor
                                currPortionOpt = nextPortion
                                None
                            }.map(_.map(_.asInstanceOf[Either[GetFieldsParseError, (ListGetFieldsDescriptor, Option[(String, Option[String])])]]))
                        case None =>
                            currPortionOpt = Some(nextPortion)
                            None
                        case Some(delimiter) =>
                            return Failure(WrongStateExcetion(s"Unexpected delimiter $delimiter, before $nextPortion!"))
                case None =>
                    currPortionOpt = None
                    None
        }
        if (err.isDefined)
            Left(err.get.map(_.asInstanceOf[Either[GetFieldsParseError, (ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]]))
        else 
            Right(currPortionOpt)


    private def parseIntervalDescriptor(
                                          nextPortion: String,
                                          fieldName: String,
                                          firstValueIsFrom: Boolean
                                      ): Try[Either[GetFieldsParseError, (ListGetFieldsDescriptor, Option[(String, Option[String])])]] =
        val (before, delimiterAndRestOpt) = getNextMatch(nextPortion)
        val firstValueInt =
            try
                before.toInt
            catch
                case e: Exception => return Success(Left(GetFieldsParseError(s"Invalid fitst interval value $before!")))
        val (secondValueInt, nextPortionFinalAndobjectRelatedMarkAtEnd) = delimiterAndRestOpt match
            case Some((delimiter, nextPortion)) =>
                val (before, delimiterAndRestOpt) = getNextMatch(nextPortion)
                val secondValueInt =
                    try
                        before.toInt
                    catch
                        case e: Exception => return Success(Left(GetFieldsParseError(s"Invalid second interval value $before!")))
                delimiterAndRestOpt match
                    case Some(`intervalToMark`, nextPortion) =>
                        if (firstValueIsFrom)
                            (Some(secondValueInt), Some(nextPortion, None))
                        else
                            return Success(Left(GetFieldsParseError(s"Unexpected doubled $intervalToMark!")))
                    case Some(`intervalFromMark`, nextPortion) =>
                        if (firstValueIsFrom)
                            return Success(Left(GetFieldsParseError(s"Unexpected doubled $intervalFromMark!")))
                        else
                            (Some(secondValueInt), Some(nextPortion, None))
                    case Some((`subobjectStartMark`, nextPortion)) =>
                        (Some(secondValueInt), Some(nextPortion, Some(subobjectStartMark)))
                    case Some((`subobjectEndMark`, nextPortion)) =>
                        (Some(secondValueInt), Some(nextPortion, Some(subobjectEndMark)))
            case None =>
                (None, None)
        if (firstValueIsFrom)
            Success(Right((ListGetFieldsDescriptor(fieldName, secondValueInt, Some(firstValueInt)), nextPortionFinalAndobjectRelatedMarkAtEnd)))
        else
            Success(Right((ListGetFieldsDescriptor(fieldName, Some(firstValueInt), secondValueInt), nextPortionFinalAndobjectRelatedMarkAtEnd)))


    private def getNextMatch(query: String): (String, Option[(String, String)]) =
        delimitersPattern.findFirstMatchIn(query) match
            case Some(matched) =>
                val before = query.substring(0, matched.start)
                val delimiter = matched.matched
                val after = query.substring(matched.end)
                (before, Some(delimiter, after))
            case None =>
                (query, None)

