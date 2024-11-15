package my.valerii_timakov.sgql.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import my.valerii_timakov.sgql.entity.{AbstractTypeError, Error, TypeNotFountError}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayType, BinaryType, EntityFieldType, EntityId}
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractEntityType, AbstractNamedEntityType, EntityType, NamedEntitySuperType, Entity}
import my.valerii_timakov.sgql.entity.read_modiriers.{SearchCondition, GetFieldsDescriptor}
import my.valerii_timakov.sgql.services.{CrudRepository, TypesDefinitionProvider}

import scala.util.{Failure, Success, Try}


class CrudActor(
                   context: ActorContext[CrudActor.CrudMessage],
                   repository: CrudRepository,
                   typesDefinitionProvider: TypesDefinitionProvider, 
) extends AbstractBehavior[CrudActor.CrudMessage](context):

    import CrudActor.*

    override def onMessage(msg: CrudMessage): Behavior[CrudMessage] =
        msg match
            case CreateMessage(entityTypeName, data, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    Right(repository.create(entityType, data))
                }
                this
            case UpdateMessage(entityTypeName, idStr, data, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        Right(repository.update(entityType, Entity(id, data)))
                    }
                }
                this
            case DeleteMessage(entityTypeName, idStr, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        Right(repository.delete(entityType, id))
                    }
                }
                this
            case GetMessage(entityTypeName, idStr, getFields, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        parseGetFieldsDescriptor(getFields, entityType) { getFields =>
                            Right(repository.get(entityType, id, getFields))
                        }
                    }
                }
                this
            case SearchMessage(entityTypeName, searchQuery, getFields, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseGetFieldsDescriptor(getFields, entityType) { getFields =>
                        parseSearchCondition(searchQuery, entityType) { searchQuery =>
                            Right(repository.find(entityType, searchQuery, getFields))
                        }
                    }
                }
                this
                
    private def getType[Res](entityTypeName: String)
                            (typeMapper: EntityType[?] => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        typesDefinitionProvider.getType(entityTypeName) match
            case None =>
                Left(TypeNotFountError(entityTypeName))
            case Some(_: NamedEntitySuperType) =>
                Left(AbstractTypeError(entityTypeName))
            case Some(entityType: EntityType[?]) =>
                typeMapper(entityType)
                
    private def parseId[Res](entityType: EntityType[?], idStr: String)
                            (idMapper: EntityId => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        entityType.valueType.idType.parse(idStr) match
            case Left(error) =>
                Left(error)
            case Right(id) =>
                idMapper(id)
                
    private def parseGetFieldsDescriptor[Res](getFields: Option[String], entityType: EntityType[?])
                                             (getFieldsDescriptorMapper: GetFieldsDescriptor => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        typesDefinitionProvider.parseGetFieldsDescriptor(getFields, entityType) match
            case Failure(ex) =>
                Right(Failure(ex))
            case Success(Left(error)) =>
                Left(error)
            case Success(Right(getFields)) =>
                getFieldsDescriptorMapper(getFields)
                
    private def parseSearchCondition[Res](searchQuery: Option[String], entityType: EntityType[?])
                                         (searchConditionMapper: SearchCondition => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        typesDefinitionProvider.parseSearchCondition(searchQuery, entityType) match
            case Failure(ex) =>
                Right(Failure(ex))
            case Success(Left(error)) =>
                Left(error)
            case Success(Right(searchQuery)) =>
                searchConditionMapper(searchQuery)

object CrudActor:
    def apply(repository: CrudRepository, typesDefinitionProvider: TypesDefinitionProvider): Behavior[CrudMessage] =
        Behaviors.setup(context => new CrudActor(context, repository, typesDefinitionProvider))
    sealed trait CrudMessage extends MainActor.Message

    final case class CreateMessage(entityTypeName: String, data: EntityFieldType, 
                                   replyTo: ActorRef[Either[Error, Try[Entity]]]) extends CrudMessage

    final case class UpdateMessage(entityTypeName: String, id: String, data: EntityFieldType, 
                                   replyTo: ActorRef[Either[Error, Try[Option[Unit]]]]) extends CrudMessage

    final case class DeleteMessage(entityTypeName: String, id: String, 
                                   replyTo: ActorRef[Either[Error, Try[Option[Unit]]]]) extends CrudMessage

    final case class GetMessage(entityTypeName: String, id: String, getFieldsQuery: Option[String], 
                                replyTo: ActorRef[Either[Error, Try[Option[Entity]]]]) extends CrudMessage

    final case class SearchMessage(entityTypeName: String, searchQuery: Option[String], getFieldsQuery: Option[String], 
                                 replyTo: ActorRef[Either[Error, Try[Seq[Entity]]]]) extends CrudMessage