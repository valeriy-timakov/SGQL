package my.valerii_timakov.sgql.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import my.valerii_timakov.sgql.entity.{AbstractTypeError, Error, TypeNotFountError}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, BinaryValue, Entity, EntityId, EntityValue}
import my.valerii_timakov.sgql.entity.domain.type_definitions.EntityIdTypeDefinition
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, EntitySuperType, EntityType}
import my.valerii_timakov.sgql.entity.read_modiriers.{GetFieldsDescriptor, SearchCondition}
import my.valerii_timakov.sgql.services.{CrudRepository, TypesDefinitionProvider}
import spray.json.JsValue

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
                    entityType.valueType.parseValue(data) match
                        case Left(error) =>
                            Left(error)
                        case Right(value) =>
                            Right(repository.create(entityType, value))
                }
                this
            case UpdateMessage(entityTypeName, idStr, data, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        entityType.valueType.parseValue(data) match
                            case Left(error) =>
                                Left(error)
                            case Right(value) =>
                                entityType.createEntity(id, value) match
                                    case Left(error) =>
                                        Left(error)
                                    case Right(entity) =>
                                        Right(repository.update(entity))
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
                            (typeMapper: EntityType[_, _, _] => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        typesDefinitionProvider.getType(entityTypeName) match
            case None =>
                Left(TypeNotFountError(entityTypeName))
            case Some(_: EntitySuperType[_, _, _]) =>
                Left(AbstractTypeError(entityTypeName))
            case Some(entityType: EntityType[_, _, _]) =>
                typeMapper(entityType)
                
    private def parseId[Res](entityType: EntityType[_, _, _], idStr: String)
                            (idMapper: EntityId[_, _] => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        val idDef: EntityIdTypeDefinition[_] = entityType.valueType.idType
        idDef.parse(idStr) match
            case Left(error) =>
                Left(error)
            case Right(id) =>
                idMapper(id)
                
    private def parseGetFieldsDescriptor[Res](getFields: Option[String], entityType: EntityType[_, _, _])
                                             (getFieldsDescriptorMapper: GetFieldsDescriptor => Either[Error, Try[Res]])
    : Either[Error, Try[Res]] =
        typesDefinitionProvider.parseGetFieldsDescriptor(getFields, entityType) match
            case Failure(ex) =>
                Right(Failure(ex))
            case Success(Left(error)) =>
                Left(error)
            case Success(Right(getFields)) =>
                getFieldsDescriptorMapper(getFields)
                
    private def parseSearchCondition[Res](searchQuery: Option[String], entityType: EntityType[_, _, _])
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

    final case class CreateMessage(entityTypeName: String, data: JsValue,
                                   replyTo: ActorRef[Either[Error, Try[Entity[_, _, _]]]]) extends CrudMessage

    final case class UpdateMessage(entityTypeName: String, id: String, data: JsValue,
                                   replyTo: ActorRef[Either[Error, Try[Option[Unit]]]]) extends CrudMessage

    final case class DeleteMessage(entityTypeName: String, id: String, 
                                   replyTo: ActorRef[Either[Error, Try[Option[Unit]]]]) extends CrudMessage

    final case class GetMessage(entityTypeName: String, id: String, getFieldsQuery: Option[String], 
                                replyTo: ActorRef[Either[Error, Try[Option[Entity[_, _, _]]]]]) extends CrudMessage

    final case class SearchMessage(entityTypeName: String, searchQuery: Option[String], getFieldsQuery: Option[String], 
                                 replyTo: ActorRef[Either[Error, Try[Seq[Entity[_, _, _]]]]]) extends CrudMessage