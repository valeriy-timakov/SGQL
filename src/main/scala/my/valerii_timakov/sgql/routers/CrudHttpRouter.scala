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
import my.valerii_timakov.sgql.entity as err
import my.valerii_timakov.sgql.entity.GetFieldsParseError
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId, EntityValue}
import my.valerii_timakov.sgql.entity.read_modiriers.{AllGetFieldsDescriptor, GetFieldsDescriptor, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, ObjectGetFieldsDescriptor, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
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
    
    private val javaDuration = conf.getDuration("timeout")
    private implicit val timeout: Timeout = Timeout(Duration(javaDuration.toMillis, MILLISECONDS))
            
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
                        val result: Future[Either[err.Error, Try[Option[Entity[_, _, _]]]]] =
                            appActor ? (GetMessage(objectType, objectId, params.get(fieldsParamName), _))
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
                            val result: Future[Either[err.Error, Try[Option[Unit]]]] =
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
                            val result: Future[Either[err.Error, Try[Option[Unit]]]] =
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
                        val result: Future[Either[err.Error, Try[Seq[Entity[_, _, _]]]]] =
                            appActor ? (SearchMessage(objectType, params.get(searchParamName), params.get(fieldsParamName), _))
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
                        val result: Future[Either[err.Error, Try[EntityId[_, _]]]] =
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

