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
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId}
import my.valerii_timakov.sgql.services.MessageSource
import org.slf4j.LoggerFactory
import spray.json.{JsArray, JsValue}

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
    private val intervalMark = conf.getString("interval-mark")

    private val logger = LoggerFactory.getLogger(getClass)

    val route: Route =
        pathPrefix("crud" / Segment) { objectType =>
            path(Segment) { objectId =>
                get {
                    parameterMap { params =>
                        try
                            val fieldsToGet = params.get(fieldsParamName)
                            val result: Future[Either[err.Error, Try[Option[Entity[_, _, _]]]]] =
                                appActor ? (GetMessage(objectType, objectId, fieldsToGet, _))
                            onSuccess(result) {
                                case Left(error) =>
                                    processBadRequest("get", error, objectType, Some(objectId), Map("fieldsToGet" -> fieldsToGet))
                                case Right(Failure(exception)) =>
                                    logger.error("Entity {}({}) get error! fieldsToGet: {}", objectType, objectId, fieldsToGet, exception)
                                    complete(StatusCodes.InternalServerError, exception.getMessage)
                                case Right(Success(None)) =>
                                    logger.warn("Entity {}({}) not found!", objectType, objectId)
                                    complete(StatusCodes.NotFound)
                                case Right(Success(Some(entity))) =>
                                    logger.info("Entity {}({}) successfully got: {}", objectType, objectId, entity)
                                    complete(StatusCodes.OK, entity.toJson)
                            }
                        catch
                            case e: Exception =>
                                logger.error("Error get object!", e)
                                complete(StatusCodes.InternalServerError, messageSource.getMessage(e.getMessage, Language("en")))
                    }
                } ~
                    put {
                        entity(as [JsValue]) { requestEntity =>
                            try
                                val result: Future[Either[err.Error, Try[Option[Unit]]]] =
                                    appActor ? (UpdateMessage(objectType, objectId, requestEntity, _))
                                onSuccess(result) {
                                    case Left(error) =>
                                        processBadRequest("update", error, objectType, Some(objectId), Map("requestEntity" -> requestEntity))
                                    case Right(Failure(exception)) =>
                                        logger.error("Entity {}({}) update error! requestEntity: {}", objectType, objectId, requestEntity, exception)
                                        complete(StatusCodes.InternalServerError, exception.getMessage)
                                    case Right(Success(None)) =>
                                        logger.warn("Entity {}({}) not found to update!", objectType, objectId)
                                        complete(StatusCodes.NotFound)
                                    case Right(Success(Some(_))) =>
                                        logger.info("Entity {}({}) successfully updated: {}", objectType, objectId, requestEntity)
                                        complete(StatusCodes.NoContent)
                                }
                            catch
                                case e: Exception =>
                                    logger.error("Error update object!", e)
                                    complete(StatusCodes.InternalServerError, messageSource.getMessage(e.getMessage, Language("en")))
                        }
                    } ~
                    delete {
                        extractRequestEntity { _ =>
                            try
                                val result: Future[Either[err.Error, Try[Option[Unit]]]] =
                                    appActor ? (DeleteMessage(objectType, objectId, _))
                                onSuccess(result) {
                                    case Left(error) =>
                                        processBadRequest("delete", error, objectType, Some(objectId))
                                    case Right(Failure(exception)) =>
                                        logger.error("Entity {}({}) delete error!", objectType, objectId, exception)
                                        complete(StatusCodes.InternalServerError, exception.getMessage)
                                    case Right(Success(None)) =>
                                        logger.warn("Entity {}({}) not found to delete!", objectType, objectId)
                                        complete(StatusCodes.NotFound)
                                    case Right(Success(Some(entity))) =>
                                        logger.info("Entity {}({}) successfully deleted.", objectType, objectId)
                                        complete(StatusCodes.NoContent)
                                }
                            catch
                                case e: Exception =>
                                    logger.error("Error detete object!", e)
                                    complete(StatusCodes.InternalServerError, messageSource.getMessage(e.getMessage, Language("en")))
                        }
                    }
            } ~
            path(searchPathPrefix) {
                get {
                    parameterMap { params =>
                        try
                            val searchQuery = params.get(searchParamName)
                            val fieldsToGet = params.get(fieldsParamName)
                            val result: Future[Either[err.Error, Try[Seq[Entity[_, _, _]]]]] =
                                appActor ? (SearchMessage(objectType, searchQuery, fieldsToGet, _))
                            onSuccess(result) {
                                case Left(error) =>
                                    processBadRequest("search", error, objectType, None,
                                        Map("searchQuery" -> searchQuery, "fieldsToGet" -> fieldsToGet))
                                case Right(Failure(exception)) =>
                                    logger.error("Entity {} search error! searchQuery: {}, fieldsToGet: {}", objectType,
                                        searchQuery, fieldsToGet, exception)
                                    complete(StatusCodes.InternalServerError, exception.getMessage)
                                case Right(Success(entities)) =>
                                    logger.info("Entities {} successfully found by {}: {}", objectType, searchQuery, result)
                                    complete(StatusCodes.OK, JsArray(entities.map(_.toJson).toVector))
                            }
                        catch
                            case e: Exception =>
                                logger.error("Error search object!", e)
                                complete(StatusCodes.InternalServerError, messageSource.getMessage(e.getMessage, Language("en")))
                    }
                }
            } ~
            pathEnd {
                post {
                    entity(as [JsValue]) { requestEntity =>
                        try
                            val result: Future[Either[err.Error, Try[EntityId[_, _]]]] =
                                appActor ? (CreateMessage(objectType, requestEntity, _))
                            onSuccess(result) {
                                case Left(error) =>
                                    processBadRequest("create", error, objectType, None, Map("requestEntity" -> requestEntity))
                                case Right(Failure(exception)) =>
                                    logger.error("Entity {} create error! requestEntity: ", objectType, requestEntity, exception)
                                    complete(StatusCodes.InternalServerError, exception.getMessage)
                                case Right(Success(id)) =>
                                    logger.info("Entity {}({}) successfully created.", objectType, id)
                                    complete(StatusCodes.OK, id.toJson)
                            }
                        catch
                            case e: Exception =>
                                logger.error("Error creating object!", e)
                                complete(StatusCodes.InternalServerError, messageSource.getMessage(e.getMessage, Language("en")))
                    }
                }
            }
        }

    private def processBadRequest(
                                     operationName: String,
                                     error: err.Error,
                                     objectType: String,
                                     objectId: Option[String],
                                     additionalParams: Map[String, Object] = Map.empty
                                 ): Route =
        val errMessage = messageSource.getMessage(error.message, Language("en"))
        val idLog = objectId.map(id => s"($id)").getOrElse("")
        val additionalParamsLog = additionalParams.map { case (k, v) => s"$k=${v.toString}" }.mkString(", ")
        logger.warn("Entity {}{} {} error! {} {}", objectType, idLog, operationName, errMessage, additionalParamsLog)
        complete(StatusCodes.BadRequest, errMessage)

