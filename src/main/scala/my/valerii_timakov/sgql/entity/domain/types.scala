package my.valerii_timakov.sgql.entity.domain.types

import my.valerii_timakov.sgql.entity.TypesConsistencyError
import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractTypeDefinition, ArrayTypeDefinition, CustomPrimitiveTypeDefinition, EntityTypeDefinition, FieldValueTypeDefinition, ItemValueTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, CustomPrimitiveValue, Entity, EntityId, EntityValue, FilledEntityId, ItemValue, ObjectValue, RootPrimitiveValue, ValueTypes}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, WrongStateExcetion}
import my.valerii_timakov.sgql.services.{ArrayTypePersistenceDataFinal, ObjectTypePersistenceDataFinal, PrimitiveTypePersistenceDataFinal, TypePersistenceDataFinal}
import spray.json.{JsArray, JsObject, JsString, JsValue}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*


private class TypesMaps(
                           val runVersion: Short,
                           val byNameMap: java.util.Map[String, AbstractEntityType[_, _, _]] = new java.util.HashMap(),
                           val byIdMap: java.util.Map[Long, AbstractEntityType[_, _, _]] = new java.util.HashMap(),
                       )

trait GlobalTypesMap:
    def init(types: Seq[AbstractEntityType[_, _, _]], version: Short, typesPersistenceData: Map[String, TypePersistenceDataFinal]): Unit
    def getTypeByName(name: String): Option[AbstractEntityType[_, _, _]]
    def getTypeById(id: Long): Option[AbstractEntityType[_, _, _]]
    def getAllTypes: Seq[AbstractEntityType[_, _, _]]

object GlobalTypesMap extends GlobalTypesMap:
    private var maps: Option[TypesMaps] = None
    private final val VERSION_SHIFT = 64-16

    def init(types: Seq[AbstractEntityType[_, _, _]], version: Short, typesPersistenceData: Map[String, TypePersistenceDataFinal]): Unit =
        if maps.nonEmpty then throw new ConsistencyException("Types already initialized!")
        maps = Some(new TypesMaps(version,
            new java.util.HashMap(types.size),
            new java.util.HashMap(types.size)
        ))
        types.foreach(tmpType => {
            if byNameMap.containsKey(tmpType.name) then throw new ConsistencyException(s"Type ${tmpType.name} already exists!")
            val id = (byIdMap.size + 1) | (runVersion.toLong << VERSION_SHIFT)
            tmpType.initId(id)
            val persistenceData = typesPersistenceData.getOrElse(tmpType.name,
                throw new ConsistencyException(s"Type ${tmpType.name} has no persistence data!"))
            (tmpType, persistenceData) match
                case (objectType: AbstractObjectEntityType[_, _], persistenceData: ObjectTypePersistenceDataFinal) =>
                    objectType._persistenceData = Some(persistenceData)
                case (primitiveType: CustomPrimitiveEntityType[_, _, _], persistenceData: PrimitiveTypePersistenceDataFinal) =>
                    primitiveType._persistenceData = Some(persistenceData)
                case (primitiveType: PrimitiveEntitySuperType[_, _, _], persistenceData: PrimitiveTypePersistenceDataFinal) =>
                    primitiveType._persistenceData = Some(persistenceData)
                case (arrayType: ArrayEntityType[_, _], persistenceData: ArrayTypePersistenceDataFinal) =>
                    arrayType._persistenceData = Some(persistenceData)
                case (arrayType: ArrayEntitySuperType[_, _], persistenceData: ArrayTypePersistenceDataFinal) =>
                    arrayType._persistenceData = Some(persistenceData)
                case _ =>
                    throw new ConsistencyException(s"Type $tmpType and persistence data $persistenceData are not compatible!")
            byNameMap.put(tmpType.name, tmpType)
            byIdMap.put(id, tmpType)
        })

    private def byNameMap: java.util.Map[String, AbstractEntityType[_, _, _]] = 
        maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!")).byNameMap
    private def byIdMap: java.util.Map[Long, AbstractEntityType[_, _, _]] = 
        maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!")).byIdMap
    private def runVersion: Short = 
        maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!")).runVersion

    def getTypeByName(name: String): Option[AbstractEntityType[_, _, _]] = Option(byNameMap.get(name))
    def getTypeById(id: Long): Option[AbstractEntityType[_, _, _]] = Option(byIdMap.get(id))
    lazy val getAllTypes: Seq[AbstractEntityType[_, _, _]] = byNameMap.values().asScala.toSeq



sealed trait AbstractType:
    def valueType: AbstractTypeDefinition

sealed trait FieldValueType extends AbstractType:
    def valueType: FieldValueTypeDefinition[_]

sealed trait ItemValueType extends FieldValueType:
    def valueType: ItemValueTypeDefinition[_]
    def name: String = valueType.name

case class SimpleObjectType[ID <: FilledEntityId[_, ID]](valueType: SimpleObjectTypeDefinition[ID]) extends FieldValueType

case class ReferenceType[ID <: FilledEntityId[_, ID]](valueType: TypeReferenceDefinition[ID]) extends ItemValueType

case class BackReferenceType[ID <: FilledEntityId[_, ID]](valueType: TypeBackReferenceDefinition[ID]) extends FieldValueType

sealed abstract class AbstractNamedType extends AbstractType:
    private var id: Option[Long] = None
    def name: String
    private[types] def initId(id: Long): Unit =
        if (this.id.isDefined) throw new ConsistencyException(s"Type $name already has id ${this.id}, when trying to set $id!")
        this.id = Some(id)
    def getId: Long = id.getOrElse(throw new ConsistencyException(s"Type $name has no id yet!"))

case class RootPrimitiveType[V <: RootPrimitiveValue[V]](valueType: RootPrimitiveTypeDefinition[V]) extends AbstractNamedType, ItemValueType

sealed abstract class AbstractEntityType[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes] extends AbstractNamedType:
    private[types] var _persistenceData: Option[TypePersistenceDataFinal] = None
    def persistenceData: TypePersistenceDataFinal = 
        _persistenceData.getOrElse(throw new ConsistencyException(s"Type $name has no persistence data!"))
    override def valueType: EntityTypeDefinition[ID, VT, V]
    def toJson: JsValue =
        val kind = this match
            case value: EntityType[_, _, _] => "EntityType"
            case value: ArrayEntitySuperType[_, _] => "ArrayEntitySuperType"
            case value: ObjectEntitySuperType[_, _] => "ObjectEntitySuperType"
            case value: PrimitiveEntitySuperType[_, _, _] => "PrimitiveEntitySuperType"
        JsObject(
            "kind" -> JsString(kind),
            "name" -> JsString(name),
            "valueType" -> valueType.toJson
        )
        
object AbstractEntityType:
    def toJson(obj: Seq[AbstractEntityType[_, _, _]]): JsValue = JsArray(obj.map(_.toJson).toVector)

sealed trait AbstractObjectEntityType[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]] extends AbstractEntityType[ID, VT, Map[String, EntityValue]]:
    def valueType: ObjectTypeDefinition[ID, VT]
    override lazy val persistenceData: ObjectTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[ObjectTypePersistenceDataFinal]

sealed abstract class EntityType[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes]
    extends AbstractEntityType[ID, VT, V]:
    override def valueType: EntityTypeDefinition[ID, VT, V]
    def createEntity(id: EntityId[_, _], value: V): Either[TypesConsistencyError, Entity[ID, VT, V]]
    def parseEntity(id: ID, valueData: JsValue): Either[my.valerii_timakov.sgql.entity.SingleMessageError, Entity[ID, VT, V]] =
        valueType.parseValue(valueData).flatMap(createEntity(id, _))
    protected def checkValue(value: ValueTypes ): Either[TypesConsistencyError, V] =
        value match
            case value: V =>
                Right(value)
            case _ =>
                Left(TypesConsistencyError(s"Wrong value type for entity $name: $value!"))
    protected def checkId(id: EntityId[_, _]): Either[TypesConsistencyError, ID] =
        id match
            case id: ID =>
                Right(id)
            case _ =>
                Left(TypesConsistencyError(s"Wrong ID type for entity $name: $id!"))
//    def toJson(value: V): JsValue
//    def parseValue(data: JsValue): Either[ValueParseError, V]
//    def toJson(entity: VT): JsValue = JsObject(
//        "type" -> JsString(name),
//        "typeId" -> id.map(id => JsNumber(id)).getOrElse(JsNull),
//        "id" -> entity.id.typeDefinition.toJson(entity.id),
//        "value" -> toJson(entity.value),
//    )

case class CustomPrimitiveEntityType[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[V]](
    name: String,
    valueType: CustomPrimitiveTypeDefinition[ID, VT, V],
) extends EntityType[ID, VT, V]:
    override lazy val persistenceData: PrimitiveTypePersistenceDataFinal = 
        super.persistenceData.asInstanceOf[PrimitiveTypePersistenceDataFinal]
    def createEntity(id: EntityId[_, _], value: V): Either[TypesConsistencyError, CustomPrimitiveValue[ID, VT, V]] =
        checkId(id).map(id => CustomPrimitiveValue(id, value, this))
    def createEntityRaw(id: EntityId[_, _], value: RootPrimitiveValue[_]): Either[TypesConsistencyError, CustomPrimitiveValue[ID, VT, V]] =
        for {
            id <- checkId(id)
            value <- checkValue(value)
        } yield CustomPrimitiveValue(id, value, this)

case class ArrayEntityType[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
    name: String,
    valueType: ArrayTypeDefinition[ID, VT],
) extends EntityType[ID, VT, Seq[ItemValue]]:
    def createEntity(id: EntityId[_, _], value: Seq[ItemValue]):  Either[TypesConsistencyError, ArrayValue[ID, VT]] =
        checkId(id).map(id => ArrayValue(id, value, this))
    override lazy val persistenceData: ArrayTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[ArrayTypePersistenceDataFinal]

case class ObjectEntityType[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
    name: String,
    valueType: ObjectTypeDefinition[ID, VT],
) extends EntityType[ID, VT, Map[String, EntityValue]], AbstractObjectEntityType[ID, VT]:
    def createEntity(id:EntityId[_, _], value: Map[String, EntityValue]):  Either[TypesConsistencyError, ObjectValue[ID, VT]] =
        checkId(id).map(id => ObjectValue(id, value, this))

trait EntitySuperType[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes] extends AbstractEntityType[ID, VT, V]:
    def name: String
    def valueType: EntityTypeDefinition[ID, VT, V]
    @tailrec
    final def hasChild(entityType: AbstractEntityType[ID, _, _]): Boolean =
        entityType.getId == getId || (entityType.valueType.parent match
                case None => false
                case Some(parent) => hasChild(parent)
            )

case class PrimitiveEntitySuperType[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[V]](
    name: String,
    valueType: CustomPrimitiveTypeDefinition[ID, VT, V],
) extends EntitySuperType[ID, VT, V]:
    override lazy val persistenceData: PrimitiveTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[PrimitiveTypePersistenceDataFinal]

case class ArrayEntitySuperType[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
    name: String,
    valueType: ArrayTypeDefinition[ID, VT],
) extends EntitySuperType[ID, VT, Seq[ItemValue]]:
    override lazy val persistenceData: ArrayTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[ArrayTypePersistenceDataFinal]

case class ObjectEntitySuperType[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
    name: String,
    valueType: ObjectTypeDefinition[ID, VT],
) extends EntitySuperType[ID, VT, Map[String, EntityValue]], AbstractObjectEntityType[ID, VT]


