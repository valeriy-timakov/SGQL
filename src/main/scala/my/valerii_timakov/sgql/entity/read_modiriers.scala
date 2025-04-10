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

    private def equalSearchFieldChainCell(searchFieldChainCell: SearchFieldChainCell): (Boolean, Option[SearchFieldChainCell]) =
        referer match
            case Some(ref) =>
                val (refererResult, nextSearchCellOpt) = ref.equalSearchFieldChainCell(searchFieldChainCell)
                if (refererResult)
                    nextSearchCellOpt
                        .map(nextSearchCell => (current.fieldName == nextSearchCell.fieldName, nextSearchCell.nextCell))
                        .getOrElse((false, None))
                else
                    (false, None)
            case None =>
                //on top of this referers chain
                (current.fieldName == searchFieldChainCell.fieldName, searchFieldChainCell.nextCell)
    
    override def equals(obj: Any): Boolean =
        obj match
            case searchFieldChainCell: SearchFieldChainCell =>
                val result = equalSearchFieldChainCell(searchFieldChainCell)
                result._1 && result._2.isEmpty
            case _ => 
                super.equals(obj)
    
case class SearchFieldChainCell(fieldName: String, subType: Option[String], nextCell: Option[SearchFieldChainCell] = None)
    

final case class Range(from: String, to: String)
sealed trait SearchCondition
sealed trait SingleFieldSearchCondition extends SearchCondition:
    def field: SearchFieldChainCell
final case class EqSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition
final case class GtSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition
final case class GeSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition
final case class LtSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition
final case class LeSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition
final case class BetweenSearchCondition(field: SearchFieldChainCell, value: Range) extends SingleFieldSearchCondition
final case class LikeSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition
final case class InSearchCondition[T <: ValueTypes](field: SearchFieldChainCell, value: Array[String]) extends SingleFieldSearchCondition
final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition
sealed trait CombinedSearchCondition extends SearchCondition:
    def conditions: List[SearchCondition]
    def ::(condition: SearchCondition): CombinedSearchCondition
final case class AndSearchCondition(conditions: List[SearchCondition]) extends CombinedSearchCondition:
    def ::(condition: SearchCondition): AndSearchCondition = AndSearchCondition(condition :: conditions)
object AndSearchCondition:
    def apply(conditions: SearchCondition*): AndSearchCondition = AndSearchCondition(conditions.toList)
final case class OrSearchCondition(conditions: List[SearchCondition]) extends CombinedSearchCondition:
    def ::(condition: SearchCondition): OrSearchCondition = OrSearchCondition(condition :: conditions)
object OrSearchCondition:
    def apply(conditions: SearchCondition*): OrSearchCondition = OrSearchCondition(conditions.toList)
