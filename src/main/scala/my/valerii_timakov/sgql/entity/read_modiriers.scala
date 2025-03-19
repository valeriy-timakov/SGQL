package my.valerii_timakov.sgql.entity.read_modiriers

import my.valerii_timakov.sgql.entity.domain.type_values.{EntityValue, ValueTypes}

sealed trait GetFieldsDescriptor
sealed trait AllGetFieldsDescriptor extends GetFieldsDescriptor
sealed trait NestedGetFieldsDescriptor extends GetFieldsDescriptor:
    def fieldName: String
sealed trait AbstractObjectGetFieldsDescriptor extends GetFieldsDescriptor:
    def fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]]
object AllGetFieldsDescriptor extends AllGetFieldsDescriptor
case class ObjectGetFieldsDescriptor(fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]])
    extends AbstractObjectGetFieldsDescriptor
case class SubObjectGetFieldsDescriptor(fieldName: String, fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]])
    extends NestedGetFieldsDescriptor, AbstractObjectGetFieldsDescriptor
case class SingleGetFieldsDescriptor(fieldName: String) extends NestedGetFieldsDescriptor
case class ListGetFieldsDescriptor(fieldName: String, limit: Option[Int], offset: Option[Int]) extends NestedGetFieldsDescriptor
case class AllInReferenceGetFieldsDescriptor(fieldName: String) extends NestedGetFieldsDescriptor

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
