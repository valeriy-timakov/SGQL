package my.valerii_timakov.sgql.entity.domain.types

import my.valerii_timakov.sgql.entity.domain.type_definitions.{AbstractTypeDefinition, ArrayTypeDefinition, CustomPrimitiveTypeDefinition, EntityTypeDefinition, FieldTypeDefinition, FieldValueTypeDefinition, FieldsContainer, ItemValueTypeDefinition, ObjectTypeDefinition, RootPrimitiveTypeDefinition, SimpleObjectTypeDefinition, TypeBackReferenceDefinition, TypeReferenceDefinition}
import my.valerii_timakov.sgql.entity.domain.type_values.{ArrayValue, CustomPrimitiveValue, Entity, EntityId, EntityValue, ItemValue, ObjectValue, RootPrimitiveValue, ValueTypes}
import my.valerii_timakov.sgql.exceptions.{ConsistencyException, WrongStateExcetion}
import my.valerii_timakov.sgql.services.{AbstractObjectPersistenceData, ArrayTypePersistenceDataFinal, ObjectTypePersistenceDataFinal, OneValuePersistenceDataFinal, PrimitiveTypePersistenceDataFinal, PrimitiveValuePersistenceDataFinal, ReferenceValuePersistenceDataFinal, SimpleObjectValuePersistenceDataFinal, TypePersistenceDataFinal}
import spray.json.{JsArray, JsObject, JsString, JsValue}

import scala.annotation.tailrec
import scala.collection.mutable
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
    def getAllLeafObjectsSubtypes(entityType: ObjectEntitySuperType[_, _]): Set[ObjectEntityType[_, _]]

object GlobalTypesMap extends GlobalTypesMap:
    private var _maps: Option[TypesMaps] = None
    private val leafObjectSubTypesCache = mutable.Map[ObjectEntitySuperType[_, _], Set[ObjectEntityType[_, _]]]()
    private final val VERSION_SHIFT = 64-16

    def init(types: Seq[AbstractEntityType[_, _, _]], version: Short, typesPersistenceData: Map[String, TypePersistenceDataFinal]): Unit =
        def checkNullableCompatibility(fieldName: String, fieldDef: FieldTypeDefinition[_], persistenceData: OneValuePersistenceDataFinal): Unit =
            if fieldDef.isNullable ^ persistenceData.isNullable then
                throw new ConsistencyException(s"Field $fieldName persistence init failed! Type ${fieldDef.valueType} " +
                    s"and persistence data $persistenceData are not compatible in nullability!")
        def setFieldsPersistenceData(objectTypeDef: FieldsContainer, persistenceData: AbstractObjectPersistenceData): Unit =
            objectTypeDef.fields.foreach((fieldName, fieldDef) =>
                val fieldPersistenceDataOpt = persistenceData.fields.get(fieldName)
                (fieldDef.valueType, fieldPersistenceDataOpt) match
                    case (primitiveType: RootPrimitiveTypeDefinition[_], Some(persistenceData: PrimitiveValuePersistenceDataFinal)) =>
                        checkNullableCompatibility(fieldName, fieldDef, persistenceData)
                        fieldDef.setPersistenceData(persistenceData)
                    case (primitiveType: TypeReferenceDefinition[_], Some(persistenceData: ReferenceValuePersistenceDataFinal)) =>
                        checkNullableCompatibility(fieldName, fieldDef, persistenceData)
                        fieldDef.setPersistenceData(persistenceData)
                    case (_: TypeBackReferenceDefinition[_], None) =>
                    //do nothing
                    case (soType: SimpleObjectTypeDefinition[_], Some(persistenceData: SimpleObjectValuePersistenceDataFinal)) =>
                        fieldDef.setPersistenceData(persistenceData)
                        setFieldsPersistenceData(soType, persistenceData)
                    case _ =>
                        throw new ConsistencyException(s"Field ${fieldDef._1} persistence init failed! Type ${fieldDef.valueType} " +
                            s"and persistence data $fieldPersistenceDataOpt are not compatible!")
            )
        if _maps.nonEmpty then throw new ConsistencyException("Types already initialized!")
        _maps = Some(new TypesMaps(version,
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
                case (objectType: ObjectEntitySuperType[_, _], persistenceData: ObjectTypePersistenceDataFinal) =>
                    objectType.typeDefinition.parent.foreach( _.addDirectChild(objectType) )
                    objectType.setPersistenceData(persistenceData)
                    setFieldsPersistenceData(objectType.typeDefinition, persistenceData)
                case (objectType: ObjectEntityType[_, _], persistenceData: ObjectTypePersistenceDataFinal) =>
                    objectType.setPersistenceData(persistenceData)
                    setFieldsPersistenceData(objectType.typeDefinition, persistenceData)
                case (primitiveType: CustomPrimitiveEntityType[_, _, _], persistenceData: PrimitiveTypePersistenceDataFinal) =>
                    primitiveType.setPersistenceData(persistenceData)
                case (primitiveType: PrimitiveEntitySuperType[_, _, _], persistenceData: PrimitiveTypePersistenceDataFinal) =>
                    primitiveType.setPersistenceData(persistenceData)
                case (arrayType: ArrayEntityType[_, _], persistenceData: ArrayTypePersistenceDataFinal) =>
                    arrayType.setPersistenceData(persistenceData)
                case (arrayType: ArrayEntitySuperType[_, _], persistenceData: ArrayTypePersistenceDataFinal) =>
                    arrayType.setPersistenceData(persistenceData)
                case _ =>
                    throw new ConsistencyException(s"Type $tmpType and persistence data $persistenceData are not compatible!")
            byNameMap.put(tmpType.name, tmpType)
            byIdMap.put(id, tmpType)
        })

    private lazy val maps = _maps.getOrElse(throw new WrongStateExcetion("TypesMap not initialized!"))
    private def byNameMap: java.util.Map[String, AbstractEntityType[_, _, _]] =
        maps.byNameMap
    private def byIdMap: java.util.Map[Long, AbstractEntityType[_, _, _]] =
        maps.byIdMap
    private def runVersion: Short =
        maps.runVersion

    def getTypeByName(name: String): Option[AbstractEntityType[_, _, _]] = Option(byNameMap.get(name))
    def getTypeById(id: Long): Option[AbstractEntityType[_, _, _]] = Option(byIdMap.get(id))
    def getAllLeafObjectsSubtypes(entityType: ObjectEntitySuperType[_, _]): Set[ObjectEntityType[_, _]] =
        leafObjectSubTypesCache.getOrElseUpdate(entityType,
            maps.byIdMap.values().asScala
                .collect { case objectType: ObjectEntityType[_, _] => objectType }
                .filter(_.isChildOf(entityType))
                .toSet
        )
//    def getAllLeafArraysSubtypes(entityType: ArrayEntitySuperType[_, _]): Set[AbstractEntityType[_, _, _]] =
//        subTypesCache.getOrElseUpdate(entityType,
//            maps.byIdMap.values().asScala.filter( _ match
//                        case currObjectType: AbstractArrayEntityType[_, _] => currObjectType.isChildOf(entityType)
//                        case _ => false
//                    ).toSet
//        )
//    def getAllLeafPrimitiveSubtypes(entityType: PrimitiveEntitySuperType[_, _, _]): Set[AbstractEntityType[_, _, _]] =
//        subTypesCache.getOrElseUpdate(entityType,
//            maps.byIdMap.values().asScala.filter( _ match
//                        case currObjectType: AbstractPrimitiveEntityType[_, _, _] => currObjectType.isChildOf(entityType)
//                        case _ => false
//                    ).toSet
//        )
    lazy val getAllTypes: Seq[AbstractEntityType[_, _, _]] = byNameMap.values().asScala.toSeq



sealed trait AbstractType:
    def typeDefinition: AbstractTypeDefinition

sealed trait FieldValueType extends AbstractType:
    def typeDefinition: FieldValueTypeDefinition[_]

sealed trait ItemValueType extends FieldValueType:
    def typeDefinition: ItemValueTypeDefinition[_]
    def name: String = typeDefinition.name

case class SimpleObjectType[ID <: EntityId[_, ID]](typeDefinition: SimpleObjectTypeDefinition[ID]) extends FieldValueType

case class ReferenceType[ID <: EntityId[_, ID]](typeDefinition: TypeReferenceDefinition[ID]) extends ItemValueType

case class BackReferenceType[ID <: EntityId[_, ID]](typeDefinition: TypeBackReferenceDefinition[ID]) extends FieldValueType

sealed abstract class AbstractNamedType extends AbstractType:
    private var id: Option[Long] = None
    def name: String
    private[types] def initId(id: Long): Unit =
        if (this.id.isDefined) throw new ConsistencyException(s"Type $name already has id ${this.id}, when trying to set $id!")
        this.id = Some(id)
    def getId: Long = id.getOrElse(throw new ConsistencyException(s"Type $name has no id yet!"))

case class RootPrimitiveType[V <: RootPrimitiveValue[V]](typeDefinition: RootPrimitiveTypeDefinition[V]) extends AbstractNamedType, ItemValueType

sealed abstract class AbstractEntityType[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes] extends AbstractNamedType:
    private var _persistenceData: Option[TypePersistenceDataFinal] = None
    private[types] def setPersistenceData(data: TypePersistenceDataFinal): Unit =
        if (_persistenceData.isDefined) throw new ConsistencyException(s"Type $name already has persistence data!")
        _persistenceData = Some(data)
    def persistenceData: TypePersistenceDataFinal = 
        _persistenceData.getOrElse(throw new ConsistencyException(s"Type $name has no persistence data!"))
    override def typeDefinition: EntityTypeDefinition[ID, VT, V]
    def toJson: JsValue =
        val kind = this match
            case value: EntityType[_, _, _] => "EntityType"
            case value: ArrayEntitySuperType[_, _] => "ArrayEntitySuperType"
            case value: ObjectEntitySuperType[_, _] => "ObjectEntitySuperType"
            case value: PrimitiveEntitySuperType[_, _, _] => "PrimitiveEntitySuperType"
        JsObject(
            "kind" -> JsString(kind),
            "name" -> JsString(name),
            "valueType" -> typeDefinition.toJson
        )

    @tailrec
    final def isChildOfRaw(entityType: EntitySuperType[_, _, _]): Boolean =
        typeDefinition.parent match
            case None => false
            case Some(parent) => parent == entityType || parent.isChildOfRaw(entityType)

    protected def checkValue(value: ValueTypes ): V =
        value match
            case value: V =>
                value
            case _ =>
                throw new ConsistencyException(s"Wrong value type for entity $name: $value!")

    protected def checkId(id: EntityId[_, _]): ID =
        id match
            case id: ID =>
                id
            case _ =>
                throw new ConsistencyException(s"Wrong ID type for entity $name: $id!")

object AbstractEntityType:
    def toJson(obj: Seq[AbstractEntityType[_, _, _]]): JsValue = JsArray(obj.map(_.toJson).toVector)

sealed trait AbstractObjectEntityType[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]] extends AbstractEntityType[ID, VT, Map[String, EntityValue]]:
    def typeDefinition: ObjectTypeDefinition[ID, VT]
    override lazy val persistenceData: ObjectTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[ObjectTypePersistenceDataFinal]
    final def isChildOf(entityType: ObjectEntitySuperType[_, _]): Boolean =
        isChildOfRaw(entityType)

sealed trait AbstractPrimitiveEntityType[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[V]] extends AbstractEntityType[ID, VT, V]:
    def typeDefinition: CustomPrimitiveTypeDefinition[ID, VT, V]
    override lazy val persistenceData: PrimitiveTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[PrimitiveTypePersistenceDataFinal]
    final def isChildOf(entityType: PrimitiveEntitySuperType[_, _, _]): Boolean =
        isChildOfRaw(entityType)

sealed trait AbstractArrayEntityType[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]] extends AbstractEntityType[ID, VT, Seq[ItemValue]]:
    def typeDefinition: ArrayTypeDefinition[ID, VT]
    override lazy val persistenceData: ArrayTypePersistenceDataFinal =
        super.persistenceData.asInstanceOf[ArrayTypePersistenceDataFinal]
    final def isChildOf(entityType: ArrayEntitySuperType[_, _]): Boolean =
        isChildOfRaw(entityType)

sealed abstract class EntityType[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes]
    extends AbstractEntityType[ID, VT, V]:
    override def typeDefinition: EntityTypeDefinition[ID, VT, V]
    def createEntity(id: EntityId[_, _], value: V): Entity[ID, VT, V]
    def parseEntity(id: ID, valueData: JsValue): Either[my.valerii_timakov.sgql.entity.SingleMessageError, Entity[ID, VT, V]] =
        typeDefinition.parseValue(valueData).map(createEntity(id, _))
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
                                                                                                                                  typeDefinition: CustomPrimitiveTypeDefinition[ID, VT, V],
) extends EntityType[ID, VT, V], AbstractPrimitiveEntityType[ID, VT, V]:
    def createEntity(id: EntityId[_, _], value: V): CustomPrimitiveValue[ID, VT, V] =
        CustomPrimitiveValue(checkId(id), value, this)
    def createEntityRaw(id: EntityId[_, _], value: RootPrimitiveValue[_]): CustomPrimitiveValue[ID, VT, V] =
        CustomPrimitiveValue(checkId(id), checkValue(value), this)

case class ArrayEntityType[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
                                                                               name: String,
                                                                               typeDefinition: ArrayTypeDefinition[ID, VT],
) extends EntityType[ID, VT, Seq[ItemValue]], AbstractArrayEntityType[ID, VT]:
    def createEntity(id: EntityId[_, _], value: Seq[ItemValue]):  ArrayValue[ID, VT] =
        ArrayValue(checkId(id), value, this)

case class ObjectEntityType[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
                                                                                 name: String,
                                                                                 typeDefinition: ObjectTypeDefinition[ID, VT],
) extends EntityType[ID, VT, Map[String, EntityValue]], AbstractObjectEntityType[ID, VT]:
    def createEntityAndCheckFields(id:EntityId[_, _], value: Map[String, Option[EntityValue]]):  ObjectValue[ID, VT] =
        value
            .filter(_._2.isEmpty)
            .foreach((fieldName, fieldValueOpt) =>
                val fieldType = typeDefinition.fields.getOrElse(fieldName, throw new WrongStateExcetion(
                    s"Field $fieldName not found in object type $name to create entity!"))
                if !fieldType.isNullable then
                    throw new WrongStateExcetion(s"Field $fieldName value not found in provided fields to create entity of type $name!")
            )
        val checkedFieldsMap: Map[String, EntityValue] = value
            .collect { case (fieldName, Some(fieldValue)) => fieldName -> fieldValue }
        createEntity(id, checkedFieldsMap)
    def createEntity(id:EntityId[_, _], value: Map[String, EntityValue]):  ObjectValue[ID, VT] =
        ObjectValue(checkId(id), value, this)

abstract class EntitySuperType[ID <: EntityId[_, ID], VT <: Entity[ID, VT, V], V <: ValueTypes] extends AbstractEntityType[ID, VT, V]:
    private var _directChildren: List[AbstractEntityType[ID, _, V]] = Nil
    private[domain] def addDirectChild(child: AbstractEntityType[ID, _, V]): Unit =
        _directChildren = child :: _directChildren
    def directChildren: List[AbstractEntityType[ID, _, V]] =
        _directChildren
    def name: String
    def typeDefinition: EntityTypeDefinition[ID, VT, V]
    @tailrec
    final def hasChild[ID2 <: EntityId[_, ID2]](entityType: AbstractEntityType[ID2, _, _]): Boolean =
        entityType.getId == getId || (entityType.typeDefinition.parent match
                case None => false
                case Some(parent) => hasChild(parent)
            )

case class PrimitiveEntitySuperType[ID <: EntityId[_, ID], VT <: CustomPrimitiveValue[ID, VT, V], V <: RootPrimitiveValue[V]](
                                                                                                                                 name: String,
                                                                                                                                 typeDefinition: CustomPrimitiveTypeDefinition[ID, VT, V],
) extends EntitySuperType[ID, VT, V], AbstractPrimitiveEntityType[ID, VT, V]:
    override def directChildren: List[AbstractPrimitiveEntityType[ID, _, V]] =
        super.directChildren.asInstanceOf[List[AbstractPrimitiveEntityType[ID, _, V]]]

case class ArrayEntitySuperType[ID <: EntityId[_, ID], VT <: ArrayValue[ID, VT]](
                                                                                    name: String,
                                                                                    typeDefinition: ArrayTypeDefinition[ID, VT],
) extends EntitySuperType[ID, VT, Seq[ItemValue]], AbstractArrayEntityType[ID, VT]:
    override def directChildren: List[AbstractArrayEntityType[ID, _]] =
        super.directChildren.asInstanceOf[List[AbstractArrayEntityType[ID, _]]]

case class ObjectEntitySuperType[ID <: EntityId[_, ID], VT <: ObjectValue[ID, VT]](
                                                                                      name: String,
                                                                                      typeDefinition: ObjectTypeDefinition[ID, VT],
) extends EntitySuperType[ID, VT, Map[String, EntityValue]], AbstractObjectEntityType[ID, VT]:
    override def directChildren: List[AbstractObjectEntityType[ID, _]] =
        super.directChildren.asInstanceOf[List[AbstractObjectEntityType[ID, _]]]


