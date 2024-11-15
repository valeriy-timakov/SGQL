package my.valerii_timakov.sgql.routers

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.*
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Language
import akka.http.scaladsl.server.Directives.{entity, *}
import akka.http.scaladsl.server.{Directives, Route}
import akka.util.Timeout
import my.valerii_timakov.sgql.actors.CrudActor
import my.valerii_timakov.sgql.actors.CrudActor.*
import my.valerii_timakov.sgql.entity.Error
import my.valerii_timakov.sgql.entity.domain.type_values.EntityFieldType
import my.valerii_timakov.sgql.entity.domain.type_definitions.Entity
import my.valerii_timakov.sgql.services.MessageSource

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

class CrudHttpRouter(
                        appActor: ActorRef[CrudActor.CrudMessage],
                        implicit val system: ActorSystem[_],
                        val messageSource: MessageSource
                    ):

    import akka.actor.typed.scaladsl.AskPattern.*
    import my.valerii_timakov.sgql.entity.json.*
    implicit val timeout: Timeout = Timeout(5.seconds)

    val route: Route =
        pathPrefix("crud" / Segment) { objectType =>
            path(Segment) { objectId =>
                get {
                    parameterMap { params =>
                        val result: Future[Either[Error, Try[Option[Entity]]]] =
                            appActor ? (GetMessage(objectType, objectId, params.get("fields"), _))
                        onSuccess(result) {
                            case Left(error) =>
                                complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                            case Right(Failure(exception)) =>
                                complete(StatusCodes.InternalServerError, exception.getMessage)
                            case Right(Success(None)) =>
                                complete(StatusCodes.NotFound)
                            case Right(Success(Some(entity))) =>
                                complete(StatusCodes.OK, entity)
                        }
                    }
                } ~
                    put {
                        entity(as [EntityFieldType]) { requestEntity =>
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
                        extractRequestEntity { requestEntity =>
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
            path("search") {
                get {
                    parameterMap { params =>
                        val result: Future[Either[Error, Try[Seq[Entity]]]] =
                            appActor ? (SearchMessage(objectType, params.get("search"), params.get("fields"), _))
                        onSuccess(result) {
                            case Left(error) =>
                                complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                            case Right(Failure(exception)) =>
                                complete(StatusCodes.InternalServerError, exception.getMessage)
                            case Right(Success(entities)) =>
                                complete(StatusCodes.OK, entities)
                        }
                    }
                }
            } ~
            pathEnd {
                post {
                    entity(as [EntityFieldType]) { requestEntity =>
                        val result: Future[Either[Error, Try[Entity]]] =
                            appActor ? (CreateMessage(objectType, requestEntity, _))
                        onSuccess(result) {
                            case Left(error) =>
                                complete(StatusCodes.BadRequest, messageSource.getMessage(error.message, Language("en")))
                            case Right(Failure(exception)) =>
                                complete(StatusCodes.InternalServerError, exception.getMessage)
                            case Right(Success(entity)) =>
                                complete(StatusCodes.OK, entity)
                        }
                    }
                }
            }
        }
