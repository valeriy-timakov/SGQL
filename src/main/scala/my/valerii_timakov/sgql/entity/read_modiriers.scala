package my.valerii_timakov.sgql.entity.read_modiriers

import my.valerii_timakov.sgql.entity.domain.type_values.EntityFieldType

sealed trait GetFieldsDescriptor
case class ObjectGetFieldsDescriptor(fields: Map[String, GetFieldsDescriptor])
case class ListGetFieldsDescriptor(fields: GetFieldsDescriptor, limit: Int, offset: Int)
case object AllGetFieldsDescriptor extends GetFieldsDescriptor

sealed trait SearchCondition
final case class EqSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class NeSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class GtSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class GeSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class LtSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class LeSearchCondition(fieldName: String, value: EntityFieldType) extends SearchCondition
final case class BetweenSearchCondition(fieldName: String, from: EntityFieldType, to: EntityFieldType) extends SearchCondition
final case class LikeSearchCondition(fieldName: String, value: String) extends SearchCondition
final case class InSearchCondition[T <: EntityFieldType](fieldName: String, value: List[T]) extends SearchCondition
final case class AndSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class OrSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition
