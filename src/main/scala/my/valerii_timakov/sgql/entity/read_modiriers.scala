package my.valerii_timakov.sgql.entity.read_modiriers

import my.valerii_timakov.sgql.entity.domain.type_values.{EntityValue, ValueTypes}

sealed trait GetFieldsDescriptor
sealed trait AllGetFieldsDescriptor extends GetFieldsDescriptor
sealed trait NestedGetFieldsDescriptor extends GetFieldsDescriptor:
    def fieldName: String
sealed trait NestedGetFieldsDescriptorExpanded extends GetFieldsDescriptor:
    def fieldName: String
sealed trait AbstractSingleFieldGetFieldsDescriptor extends NestedGetFieldsDescriptor, NestedGetFieldsDescriptorExpanded:
    def isGet: Boolean
sealed trait AbstractObjectGetFieldsDescriptor extends GetFieldsDescriptor:
    def fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]]
sealed trait AbstractObjectGetFieldsDescriptorExpanded extends GetFieldsDescriptor:
    def fields: List[NestedGetFieldsDescriptorExpanded]
sealed trait AbstractSubObjectGetFieldsDescriptor extends NestedGetFieldsDescriptor, AbstractObjectGetFieldsDescriptor:
    def subType: Option[String]
sealed trait AbstractSubObjectGetFieldsDescriptorExpanded extends NestedGetFieldsDescriptorExpanded, AbstractObjectGetFieldsDescriptorExpanded:
    def subType: Option[String]
    
object AllGetFieldsDescriptor extends AllGetFieldsDescriptor

case class ObjectGetFieldsDescriptor(fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]])
    extends AbstractObjectGetFieldsDescriptor

case class ObjectGetFieldsDescriptorExpanded(fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptorExpanded]])
    extends AbstractObjectGetFieldsDescriptorExpanded

case class SubObjectGetFieldsDescriptor(
    fieldName: String, 
    subType: Option[String], 
    fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]]
) extends AbstractSubObjectGetFieldsDescriptor

object SubObjectGetFieldsDescriptor:
    def apply(fieldName: String, subType: Option[String], fields: List[NestedGetFieldsDescriptor]): SubObjectGetFieldsDescriptor =
        SubObjectGetFieldsDescriptor(fieldName, subType, Right(fields))

case class SubObjectGetFieldsDescriptorExpanded(
    fieldName: String,
    subType: Option[String],
    fields: List[NestedGetFieldsDescriptorExpanded]
) extends NestedGetFieldsDescriptorExpanded, AbstractSubObjectGetFieldsDescriptorExpanded

//Field descriptor for referenced primitive type
case class PrimitiveGetFieldsDescriptor(
    fieldName: String,
    isGet: Boolean,
    searchPathes: List[SearchFieldChainCell]
) extends AbstractSingleFieldGetFieldsDescriptor, AbstractObjectGetFieldsDescriptor, AbstractObjectGetFieldsDescriptorExpanded:
    def fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]] = Left(AllGetFieldsDescriptor)
    
case class SingleGetFieldsDescriptor(
    fieldName: String,
    isGet: Boolean,
    searchPathes: List[SearchFieldChainCell]
) extends AbstractSingleFieldGetFieldsDescriptor
        
case class ListGetFieldsDescriptor(
    fieldName: String, 
    limit: Option[Int], 
    offset: Option[Int]
) extends AbstractSingleFieldGetFieldsDescriptor

case class ListSubObjectGetFieldsDescriptor(
    fieldName: String,
    subType: Option[String],
    fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]],
    limit: Option[Int],
    offset: Option[Int]
) extends AbstractSubObjectGetFieldsDescriptor

case class ListSubObjectGetFieldsDescriptorExpanded(
    fieldName: String,
    subType: Option[String],
    fields: List[NestedGetFieldsDescriptorExpanded], 
    limit: Option[Int],
    offset: Option[Int]
) extends NestedGetFieldsDescriptorExpanded, AbstractSubObjectGetFieldsDescriptorExpanded

case class AllInBackReferenceGetFieldsDescriptor(fieldName: String) extends NestedGetFieldsDescriptorExpanded


private val subObjectFieldsDelimiter = "."
case class GetDescriptorChainCell[CFD <: NestedGetFieldsDescriptorExpanded](
   current: CFD, 
   referer: Option[GetDescriptorChainCell[_ <: AbstractObjectGetFieldsDescriptorExpanded]] = None
):
    lazy val fieldName: String = referer.map(_.fieldName + subObjectFieldsDelimiter).getOrElse("") + current.fieldName
    lazy val asParentPrefix: String = fieldName + subObjectFieldsDelimiter

//    private def equalSearchFieldChainCell(searchFieldChainCell: SearchFieldChainCell): (Boolean, Option[SearchFieldChainCell]) =
//        referer match
//            case Some(ref) =>
//                val (refererResult, nextSearchCellOpt) = ref.equalSearchFieldChainCell(searchFieldChainCell)
//                if (refererResult)
//                    nextSearchCellOpt
//                        .map(nextSearchCell => (current.fieldName == nextSearchCell.fieldName, nextSearchCell.nextCell))
//                        .getOrElse((false, None))
//                else
//                    (false, None)
//            case None =>
//                //on top of this referers chain
//                (current.fieldName == searchFieldChainCell.fieldName, searchFieldChainCell.nextCell)
//    
//    override def equals(obj: Any): Boolean =
//        obj match
//            case searchFieldChainCell: SearchFieldChainCell =>
//                val result = equalSearchFieldChainCell(searchFieldChainCell)
//                result._1 && result._2.isEmpty
//            case _ => 
//                super.equals(obj)
    
case class SearchFieldChainCell(fieldName: String, subType: Option[String], nextCell: Option[SearchFieldChainCell] = None):
    override def equals(obj: Any): Boolean =
        obj match
            case getDescChainCell: GetDescriptorChainCell[_] =>
                getDescChainCell.equals(this)
            case _ =>
                super.equals(obj)
    

final case class Range(from: String, to: String)
sealed trait SearchCondition:
    /**
     * Translate field path to SQL
     * @param translateFieldPath - function to translate field path to SQL representation including table alias and column name. SHOULD ALWAYS BE CALLED BEFORE bindParameter!
     * @param bindParameter - function to parse parameter string representation, bind it to SQL statement and return parameter placeholder with correct number
     * @return SQL representation of the condition
     */    
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String
sealed trait SingleFieldSearchCondition extends SearchCondition:
    def field: SearchFieldChainCell
final case class EqSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " = " + bindParameter(value)
final case class GtSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " > " + bindParameter(value)
final case class GeSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " >= " + bindParameter(value)
final case class LtSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " < " + bindParameter(value)
final case class LeSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " <= " + bindParameter(value)
final case class BetweenSearchCondition(field: SearchFieldChainCell, value: Range) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " BETWEEN " + bindParameter(value.from) + " AND " + bindParameter(value.to)
final case class LikeSearchCondition(field: SearchFieldChainCell, value: String) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " LIKE " + bindParameter(value)
final case class InSearchCondition[T <: ValueTypes](field: SearchFieldChainCell, value: Array[String]) extends SingleFieldSearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        translateFieldPath(field) + " IN (" + bindParameter(value) + ")"
final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition:
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        "NOT (" + condition.toSQL(translateFieldPath, bindParameter) + ")"
sealed trait CombinedSearchCondition extends SearchCondition:
    def conditions: List[SearchCondition]
    def ::(condition: SearchCondition): CombinedSearchCondition
final case class AndSearchCondition(conditions: List[SearchCondition]) extends CombinedSearchCondition:
    def ::(condition: SearchCondition): AndSearchCondition = AndSearchCondition(condition :: conditions)
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        conditions.map("(" + _.toSQL(translateFieldPath, bindParameter) + ")").mkString(" AND ")
object AndSearchCondition:
    def apply(conditions: SearchCondition*): AndSearchCondition = AndSearchCondition(conditions.toList)
final case class OrSearchCondition(conditions: List[SearchCondition]) extends CombinedSearchCondition:
    def ::(condition: SearchCondition): OrSearchCondition = OrSearchCondition(condition :: conditions)
    def toSQL(translateFieldPath: SearchFieldChainCell => String, bindParameter: String | Array[String] => String): String =
        conditions.map(_.toSQL(translateFieldPath, bindParameter)).mkString(" OR ")
object OrSearchCondition:
    def apply(conditions: SearchCondition*): OrSearchCondition = OrSearchCondition(conditions.toList)
