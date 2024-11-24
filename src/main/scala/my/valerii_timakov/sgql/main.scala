package my.valerii_timakov.sgql

import akka.actor.typed.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import my.valerii_timakov.sgql.actors.MainActor
import my.valerii_timakov.sgql.entity.domain.type_definitions.GlobalSerializationData
import my.valerii_timakov.sgql.routers.{CrudHttpRouter, TypesProviderHttpRouter}
import my.valerii_timakov.sgql.services.*

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationLong
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.io.StdIn
import scala.language.postfixOps

@main def main(args: String*): Unit =

    lazy val conf: Config = ConfigFactory.load("application.config.yaml")
        .withFallback(ConfigFactory.load("default.application.config.conf"))

    GlobalSerializationData.initJson(conf.getConfig("serialization.json"))

    implicit lazy val typesDefinitionsLoader: TypesDefinitionsLoader = TypesDefinitionsLoaderImpl(conf.getConfig("type-definitions"))
    implicit lazy val typesPersistenceConfigLoader: PersistenceConfigLoader = PersistenceConfigLoaderImpl(conf.getConfig("persistence"))
    lazy val typesDefinitionProvider: TypesDefinitionProvider = TypesDefinitionProvider.create
    val typeNameMaxLength = conf.getInt("type-definitions.type-name-max-length").toShort
    val fieldMaxLength = conf.getInt("type-definitions.field-max-length").toShort
    lazy val crudRepository = CrudRepositoriesFactory.createRopository(conf.getConfig("persistence"), typeNameMaxLength, fieldMaxLength)
    crudRepository.init(typesDefinitionProvider)
    lazy val messageSource: MessageSource =  MessageSourceImpl()


    implicit val system: ActorSystem[MainActor.Message] = ActorSystem(MainActor(crudRepository, typesDefinitionProvider), "main-system")
    implicit val executionContextExecutor: ExecutionContextExecutor = system.executionContext
    val mainActor: ActorRef[MainActor.Message] = system
    val crudHttpRouter = CrudHttpRouter(mainActor, system, messageSource)
    val typesProviderHttpRouter = TypesProviderHttpRouter(mainActor, system, messageSource)
    val httpServer: HttpServer = new HttpServer(crudHttpRouter, typesProviderHttpRouter, conf.getConfig("http"))


    def terminate(): Future[Unit] =
        httpServer.stop()
            .map(_ => system.terminate())

    httpServer.onDestroy.onComplete { _ =>
        terminate().onComplete { - =>
            println("The application stop initiated by HTTP request.")
            System.exit(0)
        }
    }

    sys.addShutdownHook {
        println("The application stop initiated by JVM shutdown hook.")
        Await.ready(terminate(), conf.getDuration("shutdown-timeout").toMillis millis)
    }

    println(s"Server now online. Please navigate to http://localhost:${conf.getConfig("http").getInt("port")}/hello\nPress RETURN to stop...")
    StdIn.readLine()
    terminate()


