package my.valerii_timakov.sgql.entity.read_modiriers

import my.valerii_timakov.sgql.entity.domain.type_values.{EntityValue, ValueTypes}

sealed trait GetFieldsDescriptor
case object AllGetFieldsDescriptor extends GetFieldsDescriptor
case class ObjectGetFieldsDescriptor(fields: Seq[GetFieldsDescriptor]) extends GetFieldsDescriptor
case class SingleGetFieldsDescriptor(fieldName: String) extends GetFieldsDescriptor
case class ListGetFieldsDescriptor(repeatedField: GetFieldsDescriptor, limit: Option[Int], offset: Option[Int]) extends GetFieldsDescriptor
case class SubObjectGetFieldsDescriptor(fieldName: String, subFields: Seq[GetFieldsDescriptor]) extends GetFieldsDescriptor

sealed trait SearchCondition
final case class EmptySearchCondition() extends SearchCondition
final case class EqSearchCondition(fieldName: String, value: ValueTypes) extends SearchCondition
final case class NeSearchCondition(fieldName: String, value: ValueTypes) extends SearchCondition
final case class GtSearchCondition(fieldName: String, value: ValueTypes) extends SearchCondition
final case class GeSearchCondition(fieldName: String, value: ValueTypes) extends SearchCondition
final case class LtSearchCondition(fieldName: String, value: ValueTypes) extends SearchCondition
final case class LeSearchCondition(fieldName: String, value: ValueTypes) extends SearchCondition
final case class BetweenSearchCondition(fieldName: String, from: ValueTypes, to: ValueTypes) extends SearchCondition
final case class LikeSearchCondition(fieldName: String, value: String) extends SearchCondition
final case class InSearchCondition[T <: ValueTypes](fieldName: String, value: List[T]) extends SearchCondition
final case class AndSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class OrSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition
