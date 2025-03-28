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
case class PrimitiveGetFieldsDescriptor(fieldName: String) extends NestedGetFieldsDescriptor, AbstractObjectGetFieldsDescriptor:
    def fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]] = Left(AllGetFieldsDescriptor)
case class SingleGetFieldsDescriptor(fieldName: String) extends NestedGetFieldsDescriptor
case class ListGetFieldsDescriptor(fieldName: String, limit: Option[Int], offset: Option[Int]) extends NestedGetFieldsDescriptor
case class ListSubObjectGetFieldsDescriptor(
                                               fieldName: String,
                                               fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]],
                                               limit: Option[Int],
                                               offset: Option[Int]
                                           ) extends NestedGetFieldsDescriptor
case class AllInReferenceGetFieldsDescriptor(fieldName: String) extends NestedGetFieldsDescriptor


private val subobjectFieldsDelimiter = "."
case class GetDescriptorChainCell[CFD <: NestedGetFieldsDescriptor](current: CFD, referer: Option[GetDescriptorChainCell[_ <: AbstractObjectGetFieldsDescriptor]] = None):
    lazy val fieldName: String = referer.map(_.fieldName + subobjectFieldsDelimiter).getOrElse("") + current.fieldName
    lazy val asParentPrefix: String = fieldName + subobjectFieldsDelimiter
    


sealed trait SearchCondition
final case class EmptySearchCondition() extends SearchCondition
final case class EqSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: ValueTypes) extends SearchCondition
final case class NeSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: ValueTypes) extends SearchCondition
final case class GtSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: ValueTypes) extends SearchCondition
final case class GeSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: ValueTypes) extends SearchCondition
final case class LtSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: ValueTypes) extends SearchCondition
final case class LeSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: ValueTypes) extends SearchCondition
final case class BetweenSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], from: ValueTypes, to: ValueTypes) extends SearchCondition
final case class LikeSearchCondition(field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: String) extends SearchCondition
final case class InSearchCondition[T <: ValueTypes](field: GetDescriptorChainCell[SingleGetFieldsDescriptor], value: List[_ <: ValueTypes]) extends SearchCondition
final case class AndSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class OrSearchCondition(conditions: List[SearchCondition]) extends SearchCondition
final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition
