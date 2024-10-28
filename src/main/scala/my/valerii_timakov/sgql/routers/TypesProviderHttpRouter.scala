package my.valerii_timakov.sgql.routers

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.*
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, entity, get, onSuccess, path, pathEnd, pathPrefix, *}
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.{Directives, Route}
import akka.util.Timeout
import my.valerii_timakov.sgql.actors.TypesProviderActor
import my.valerii_timakov.sgql.actors.TypesProviderActor.*
import my.valerii_timakov.sgql.entity.{AbstractNamedEntityType, EntityType}
import my.valerii_timakov.sgql.services.MessageSource

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

class TypesProviderHttpRouter(
                                 appActor: ActorRef[TypesProviderActor.Command],
                                 implicit val system: ActorSystem[_],
                                 val messageSource: MessageSource
                             ):

    import akka.actor.typed.scaladsl.AskPattern.*
    import my.valerii_timakov.sgql.entity.json.*
    implicit val timeout: Timeout = Timeout(5.seconds)

    val route: Route =
        path("hello") {
            get {
                val result = appActor ? (GetAllPersist.apply)
                complete(result.toString)
            }
        } ~
        pathPrefix("type") {
            path(Segment) { name =>
                get {
                    val result: Future[Option[AbstractNamedEntityType]] =
                        appActor ? (Get(name, _))
                    onSuccess(result) {
                        case None =>
                            complete(StatusCodes.NotFound)
                        case Some(typeDefinition: AbstractNamedEntityType) =>
                            complete(StatusCodes.OK, typeDefinition)
                    }
                }
            } ~
            pathEnd {
                get {
                    val result: Future[Seq[AbstractNamedEntityType]] = appActor ? GetAll.apply
                    onSuccess(result)(types => complete(StatusCodes.OK, types))
                }
            }
        }