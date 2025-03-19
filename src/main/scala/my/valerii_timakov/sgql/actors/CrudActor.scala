package my.valerii_timakov.sgql.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity
import my.valerii_timakov.sgql.entity.domain.type_definitions.EntityIdTypeDefinition
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractEntityType, EntitySuperType, EntityType}
import my.valerii_timakov.sgql.entity.read_modiriers.{AllGetFieldsDescriptor, ListGetFieldsDescriptor, NestedGetFieldsDescriptor, ObjectGetFieldsDescriptor, SearchCondition, SingleGetFieldsDescriptor, SubObjectGetFieldsDescriptor}
import my.valerii_timakov.sgql.entity.{AbstractTypeError, GetFieldsParseError, TypeNotFountError}
import my.valerii_timakov.sgql.exceptions.WrongStateExcetion
import my.valerii_timakov.sgql.services.{CrudRepository, TypesDefinitionProvider}
import spray.json.JsValue

import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}


class CrudActor(
                   context: ActorContext[CrudActor.CrudMessage],
                   repository: CrudRepository,
                   typesDefinitionProvider: TypesDefinitionProvider,
                   conf: Config,
) extends AbstractBehavior[CrudActor.CrudMessage](context):

    private val searchParamName = conf.getString("search-param-name")
    private val fieldsParamName = conf.getString("fields-param-name")
    private val fieldsDelimiter = conf.getString("fields-delimiter")
    private val searchPathPrefix = conf.getString("search-path-prefix")
    private val subobjectStartMark = conf.getString("subobject-start-mark")
    private val subobjectEndMark = conf.getString("subobject-end-mark")
    private val intervalFromMark = conf.getString("interval-from-mark")
    private val intervalToMark = conf.getString("interval-to-mark")
    private val delimiters = List(fieldsDelimiter, searchPathPrefix, subobjectStartMark, subobjectEndMark,
        intervalFromMark, intervalToMark)
        .map(Pattern.quote)
        .mkString("|")
    private val delimitersPattern = s"($delimiters)".r

    import CrudActor.*

    override def onMessage(msg: CrudMessage): Behavior[CrudMessage] =
        def processAndWrapError[Res](process: () => Res, errorMessage: String): Either[entity.Error, Try[Res]] =
            val res =
                try
                    Success(process())
                catch
                    case e: Exception =>
                        context.log.error(errorMessage, e)
                        Failure(e)
            Right(res)

        msg match
            case CreateMessage(entityTypeName, data, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    entityType.typeDefinition.parseValue(data) match
                        case Left(error) =>
                            Left(error)
                        case Right(value) =>
                            processAndWrapError(() => repository.create(entityType, value), "Error creating entity!")
                }
                this
            case UpdateMessage(entityTypeName, idStr, data, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        entityType.typeDefinition.parseValue(data) match
                            case Left(error) =>
                                Left(error)
                            case Right(value) =>
                                val entity = entityType.createEntity(id, value)
                                processAndWrapError(() => repository.update(entity), "Error editing entity!")
                    }
                }
                this
            case DeleteMessage(entityTypeName, idStr, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        processAndWrapError(() => repository.delete(entityType, id), "Error deleting entity!")
                    }
                }
                this
            case GetMessage(entityTypeName, idStr, getFields, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseId(entityType, idStr) { id =>
                        parseAndProcessGetFieldsDescriptor(getFields, entityType) { getFields =>
                            processAndWrapError(() => repository.get(entityType, id, getFields), "Error getting entity!")
                        }
                    }
                }
                this
            case SearchMessage(entityTypeName, searchQuery, getFields, replyTo) =>
                replyTo ! getType(entityTypeName) { entityType =>
                    parseAndProcessGetFieldsDescriptor(getFields, entityType) { getFields =>
                        parseSearchCondition(searchQuery, entityType) { searchQuery =>
                            processAndWrapError(() => repository.find(entityType, searchQuery, getFields), "Error sjearching entities!")
                        }
                    }
                }
                this
                
    private def getType[Res](entityTypeName: String)
                            (typeMapper: EntityType[_, _, _] => Either[entity.Error, Try[Res]])
    : Either[entity.Error, Try[Res]] =
        typesDefinitionProvider.getType(entityTypeName) match
            case None =>
                Left(TypeNotFountError(entityTypeName))
            case Some(_: EntitySuperType[_, _, _]) =>
                Left(AbstractTypeError(entityTypeName))
            case Some(entityType: EntityType[_, _, _]) =>
                typeMapper(entityType)
                
    private def parseId[Res, ID <: EntityId[_, ID]](entityType: EntityType[ID, _, _], idStr: String)
                            (idMapper: ID => Either[entity.Error, Try[Res]])
    : Either[entity.Error, Try[Res]] =
        val idDef: EntityIdTypeDefinition[ID] = entityType.typeDefinition.idType
        idDef.parse(idStr) match
            case Left(error) =>
                Left(error)
            case Right(id) =>
                idMapper(id)
                
    private def parseAndProcessGetFieldsDescriptor[Res](getFields: Option[String], entityType: EntityType[_, _, _])
                                             (getFieldsDescriptorMapper: ObjectGetFieldsDescriptor => Either[entity.Error, Try[Res]])
    : Either[entity.Error, Try[Res]] =
        val parsedDescriptorResult = parseGetFieldsDescriptor(getFields)
        flatMap(
            parsedDescriptorResult.map(_.map(parsedDescriptor =>
                val res = typesDefinitionProvider.validateGetFieldsDescriptor(parsedDescriptor, entityType)
                    .flatMap(_ => getFieldsDescriptorMapper(parsedDescriptor))
                res 
                )))
        
    private def flatMap[Res](input: Either[entity.Error, Try[Either[entity.Error, Try[Res]]]]): Either[entity.Error, Try[Res]] = 
        input match
            case Left(error) =>
                Left(error)
            case Right(Failure(ex)) =>
                Right(Failure(ex))
            case Right(Success(res)) =>
                res 
                        


    private def parseGetFieldsDescriptor(getFieldsOpt: Option[String]): Either[GetFieldsParseError, Try[ObjectGetFieldsDescriptor]] =
        getFieldsOpt match
            case Some(fields_) =>
                val fields = fields_.trim
                if (fields.nonEmpty && fields.startsWith(subobjectStartMark))
                    parseObjectDescriptor(fields.substring(1), None)
                        .map(_.map(_._1.asInstanceOf[ObjectGetFieldsDescriptor]))
                else
                    Left(GetFieldsParseError("Starting object sign not found!"))
            case None =>
                Right(Success(ObjectGetFieldsDescriptor(Left(AllGetFieldsDescriptor))))

    private def parseObjectDescriptor(
                                         getFields: String,
                                         fieldName: Option[String]
                                     ): Either[GetFieldsParseError, Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]] = {
        var stopFound = false
        val fields: ArrayBuffer[NestedGetFieldsDescriptor] = ArrayBuffer()
        var currPortionOpt: Option[String] = Some(getFields)
        var nextDescriptorOpt: Option[NestedGetFieldsDescriptor] = None
        var nextFrom: Option[Int] = None
        var nextTo: Option[Int] = None
        var nextMarkOpt: Option[String] = None

        def parseBoundary(value: String): Either[GetFieldsParseError, Int] =
            try
                Right(value.toInt)
            catch
                case e: Exception => Left(GetFieldsParseError(s"Invalid boundary value $value!"))

        def filterFailures[T](
                                 input: Either[GetFieldsParseError, Try[T]]
                             )(
                                 onSuccess: T => Option[Either[GetFieldsParseError, Try[T]]]
                             ): Option[Either[GetFieldsParseError, Try[T]]] =
            input match
                case Left(res) =>
                    Some(Left(res))
                case Right(Failure(res)) =>
                    Some(Right(Failure(res)))
                case Right(Success(res)) =>
                    onSuccess(res)


        def isFailed[T](input: Either[GetFieldsParseError, Try[T]]): Boolean =
            input.map(_.isFailure).getOrElse(true)


        def getNextMatch(query: String): (String, Option[(String, String)]) =
            delimitersPattern.findFirstMatchIn(query) match
                case Some(matched) =>
                    val before = query.substring(0, matched.start)
                    val delimiter = matched.matched
                    val after = query.substring(matched.end)
                    (before, Some(delimiter, after))
                case None =>
                    (query, None)

        def setNextBoundary(currMatch: String): Either[GetFieldsParseError, Try[Unit]] =
            nextMarkOpt match
                case Some(nextMark) =>
                    nextMarkOpt = None
                    val boundaryValue = parseBoundary(currMatch) match
                        case Right(value) => value
                        case Left(error) => return Left(error)
                    nextMark match
                        case `intervalFromMark` =>
                            if (nextFrom.isEmpty)
                                nextFrom = Some(boundaryValue)
                                Right(Success(()))
                            else
                                Left(GetFieldsParseError(s"Unexpected $intervalFromMark before $nextDescriptorOpt!" +
                                    s"New from $boundaryValue defined, where other value already set: ${nextFrom.get}"))
                        case `intervalToMark` =>
                            if (nextTo.isEmpty)
                                nextTo = Some(boundaryValue)
                                Right(Success(()))
                            else
                                Left(GetFieldsParseError(s"Unexpected $intervalToMark before $nextDescriptorOpt!" +
                                    s"New to $boundaryValue defined, where other value already set: ${nextTo.get}"))
                        case _ =>
                            Right(Failure(WrongStateExcetion(s"Unexpected $nextMarkOpt before $nextDescriptorOpt!")))
                case None =>
                    Right(Failure(WrongStateExcetion(s"Call to setNextBoundary when no nextMarkOpt is defined!")))

        def addDataToNextDescriptor(data: String, nextDataDescriber: String, rawParsedData: String): Either[GetFieldsParseError, Try[Unit]] =
            if (nextDescriptorOpt.isDefined)
                if (data.isBlank)
                    if (nextMarkOpt.isDefined)
                        return Left(GetFieldsParseError(s"Empty value before of $rawParsedData, after ${nextMarkOpt.get}!"))
                else
                    val res = setNextBoundary(data)
                    if (isFailed(res))
                        return res
            else
                if (data.isBlank)
                    return Left(GetFieldsParseError(s"Empty field name in start of $rawParsedData!"))
                nextDescriptorOpt = Some(SingleGetFieldsDescriptor(data))
            nextMarkOpt = Some(nextDataDescriber)
            Right(Success(()))

        def addToFieldsNextDecriptor(currMatch: String): Try[Unit] =
            if (nextDescriptorOpt.isDefined)
                val nextDescriptor = nextDescriptorOpt.get
                nextDescriptorOpt = None
                setNextBoundary(currMatch).map(_.map { _ =>
                    val resDescriptor =
                        if (nextFrom.isDefined || nextTo.isDefined)
                            val result = ListGetFieldsDescriptor(nextDescriptor.fieldName, nextFrom, nextTo)
                            nextFrom = None
                            nextTo = None
                            result
                        else
                            nextDescriptor
                    fields += resDescriptor
                })
                Success(())
            else if (nextFrom.isDefined || nextTo.isDefined)
                nextFrom = None
                nextTo = None
                Failure(new WrongStateExcetion(s"One of or both nextFrom=$nextFrom and nextTo=$nextTo are set but no descriptor defined!"))
            else
                fields += SingleGetFieldsDescriptor(currMatch)
                Success(())

        while
            currPortionOpt.foreach(currPortion =>
                val (before, delimiterAndNextPotionOpt) = getNextMatch(currPortion)
                delimiterAndNextPotionOpt match
                    case Some(delimiter, nextPortion) =>
                        delimiter match
                            case `fieldsDelimiter` =>
                                val res = addToFieldsNextDecriptor(before)
                                if (res.isFailure)
                                    return Right(res.asInstanceOf[Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]])
                                currPortionOpt = Some(nextPortion)
                            case `subobjectStartMark` =>
                                if (nextDescriptorOpt.isDefined || nextFrom.isDefined || nextTo.isDefined || nextMarkOpt.isDefined)
                                    return Left(GetFieldsParseError(s"Unexpected $subobjectStartMark before $nextPortion!" +
                                        s"nextDescriptorOpt=$nextDescriptorOpt, nextFrom=$nextFrom, nextTo=$nextTo, nextMarkOpt=$nextMarkOpt"))
                                if (before.isEmpty)
                                    return Left(GetFieldsParseError(s"Empty field name of object before $nextPortion!"))
                                val err = filterFailures(
                                    parseObjectDescriptor(nextPortion, Some(before))
                                ) { (descriptor, nextPortion) =>
                                    nextDescriptorOpt = Some(descriptor.asInstanceOf[SubObjectGetFieldsDescriptor])
                                    currPortionOpt = nextPortion
                                    None
                                }
                                if (err.isDefined)
                                    return err.get
                            case `subobjectEndMark` =>
                                val res = addToFieldsNextDecriptor(before)
                                if (res.isFailure)
                                    return Right(res.get.asInstanceOf[Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]])
                                currPortionOpt = Some(nextPortion)
                                stopFound = true
                            case `intervalToMark` | `intervalFromMark` =>
                                val res = addDataToNextDescriptor(before, delimiter, currPortion)
                                if (isFailed(res))
                                    return res.asInstanceOf[Either[GetFieldsParseError, Try[(ObjectGetFieldsDescriptor | SubObjectGetFieldsDescriptor, Option[String])]]]
                                currPortionOpt = Some(nextPortion)
                            case _ =>
                                return Right(Failure(WrongStateExcetion(s"Unexpected delimiter $delimiter, before $nextPortion!")))
                    case None =>
                        return Left(GetFieldsParseError("Unexpected end of object!"))
            )
            !stopFound
        do ()

        fieldName match
            case Some(fieldName) =>
                Right(Success(SubObjectGetFieldsDescriptor(fieldName, Right(fields.toList)), currPortionOpt))
            case None =>
                Right(Success(ObjectGetFieldsDescriptor(Right(fields.toList)), currPortionOpt))
    }

    private def parseSearchCondition[Res](searchQuery: Option[String], entityType: EntityType[_, _, _])
                                         (searchConditionMapper: SearchCondition => Either[entity.Error, Try[Res]])
    : Either[entity.Error, Try[Res]] =
        typesDefinitionProvider.parseSearchCondition(searchQuery, entityType) match
            case Failure(ex) =>
                Right(Failure(ex))
            case Success(Left(error)) =>
                Left(error)
            case Success(Right(searchQuery)) =>
                searchConditionMapper(searchQuery)


object CrudActor:
    def apply(repository: CrudRepository, typesDefinitionProvider: TypesDefinitionProvider, conf: Config): Behavior[CrudMessage] =
        Behaviors.setup(context => new CrudActor(context, repository, typesDefinitionProvider, conf))
    sealed trait CrudMessage extends MainActor.Message

    final case class CreateMessage(entityTypeName: String, data: JsValue,
                                   replyTo: ActorRef[Either[entity.Error, Try[EntityId[_, _]]]]) extends CrudMessage

    final case class UpdateMessage(entityTypeName: String, id: String, data: JsValue,
                                   replyTo: ActorRef[Either[entity.Error, Try[Option[Unit]]]]) extends CrudMessage

    final case class DeleteMessage(entityTypeName: String, id: String, 
                                   replyTo: ActorRef[Either[entity.Error, Try[Option[Unit]]]]) extends CrudMessage

    final case class GetMessage(entityTypeName: String, id: String, getFieldsQuery: Option[String], 
                                replyTo: ActorRef[Either[entity.Error, Try[Option[Entity[_, _, _]]]]]) extends CrudMessage

    final case class SearchMessage(entityTypeName: String, searchQuery: Option[String], getFieldsQuery: Option[String], 
                                 replyTo: ActorRef[Either[entity.Error, Try[Seq[Entity[_, _, _]]]]]) extends CrudMessage


