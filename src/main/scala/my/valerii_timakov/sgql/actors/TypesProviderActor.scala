package my.valerii_timakov.sgql.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, EntityType}
import my.valerii_timakov.sgql.services.{TypePersistenceDataFinal, TypesDefinitionProvider}

import scala.util.Try

class TypesProviderActor(
    context: ActorContext[TypesProviderActor.Command],
    typesDefinitionProvider: TypesDefinitionProvider
) extends AbstractBehavior[TypesProviderActor.Command](context):
    
    import TypesProviderActor.*

    override def onMessage(command: Command): Behavior[Command] = command match
        case Get(name, replyTo) =>
            context.log.info(s"Get type $name")
            replyTo ! typesDefinitionProvider.getType(name)
            this
        case GetAll(replyTo) =>
            context.log.info(s"Get all types")
            replyTo ! typesDefinitionProvider.getAllTypes
            this
        case GetAllPersist(replyTo) =>
            context.log.info(s"Get all types")
            replyTo ! typesDefinitionProvider.getAllPersistenceDataMap
            this


object TypesProviderActor:
    def apply(typesDefinitionProvider: TypesDefinitionProvider): Behavior[Command] =
        Behaviors.setup(context => new TypesProviderActor(context, typesDefinitionProvider))
    sealed trait Command extends MainActor.Message
    final case class Get(name: String, replyTo: ActorRef[Option[AbstractEntityType[_, _, _]]]) extends Command
    final case class GetAll(replyTo: ActorRef[Seq[AbstractEntityType[_, _, _]]]) extends Command
    final case class GetAllPersist(replyTo: ActorRef[Map[String, TypePersistenceDataFinal]]) extends Command