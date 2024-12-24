package my.valerii_timakov.sgql.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.typesafe.config.Config
import my.valerii_timakov.sgql.services.{CrudRepository, TypesDefinitionProvider}

class MainActor(
    context: ActorContext[MainActor.Message],
    repository: CrudRepository,
    typesDefinitionProvider: TypesDefinitionProvider, 
    conf: Config
)  extends AbstractBehavior[MainActor.Message](context):
    
    val crudActor: ActorRef[CrudActor.CrudMessage] = 
        context.spawn(CrudActor(repository, typesDefinitionProvider, conf.getConfig("http.protocol")), "crud-actor")
        
    val typesProviderActor: ActorRef[TypesProviderActor.Command] = 
        context.spawn(TypesProviderActor(typesDefinitionProvider), "types-actor")
    
    import MainActor.*
    override def onMessage(msg: Message): Behavior[Message] =
        msg match
            case msg: CrudActor.CrudMessage =>
                crudActor ! msg
            case msg: TypesProviderActor.Command =>
                typesProviderActor ! msg
            case _ => 
                context.log.info("MainActor received unknown message")
        this
    
object MainActor:
    trait Message
    def apply(repository: CrudRepository, typesDefinitionProvider: TypesDefinitionProvider, conf: Config): Behavior[Message] =
        Behaviors.setup(context => new MainActor(context, repository, typesDefinitionProvider, conf))
