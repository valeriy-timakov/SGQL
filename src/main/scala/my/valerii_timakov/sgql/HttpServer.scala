package my.valerii_timakov.sgql

import akka.Done
import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.*
import akka.util.Timeout
import com.typesafe.config.Config
import my.valerii_timakov.sgql.actors.MainActor
import my.valerii_timakov.sgql.routers.{CrudHttpRouter, TypesProviderHttpRouter}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.language.postfixOps

class HttpServer(
                    crudHttpRouter: CrudHttpRouter, 
                    typesProviderHttpRouter: TypesProviderHttpRouter,
                    conf: Config
)(implicit val system: ActorSystem[MainActor.Message]):

    private implicit val timeout: Timeout = Timeout(conf.getDuration("response-timeout").toMillis millis)
    private implicit val executionContext: ExecutionContextExecutor = system.executionContext
    
    private lazy val destroyPromise = Promise[Unit]()

    private val bindingFuture: Future[Http.ServerBinding] = Http().newServerAt(conf.getString("host"), conf.getInt("port")).bind(
        crudHttpRouter.route ~
            typesProviderHttpRouter.route ~
            path("terminate") {
                post {
                    destroyPromise.success(())
                    complete("")
                }
            })
    
    def stop(): Future[Done] = 
        bindingFuture.flatMap(_.unbind())
        
    def onDestroy: Future[Unit] = 
        destroyPromise.future
