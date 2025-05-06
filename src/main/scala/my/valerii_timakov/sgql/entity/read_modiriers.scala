package my.valerii_timakov.sgql.entity.read_modiriers

import my.valerii_timakov.sgql.entity.SearchConditionParseError
import my.valerii_timakov.sgql.entity.domain.type_values.{EntityValue, ValueTypes}
import my.valerii_timakov.sgql.entity.domain.types.{AbstractArrayEntityType, AbstractEntityType, AbstractObjectEntityType}
import my.valerii_timakov.sgql.services.SqlData

import scala.annotation.tailrec
object GlobalConstants:
    val primitiveTypeValueFieldNameForDsc = "value"
    val entityIdFieldNameForDsc = "id"
    val searchWildcard: String = "" + 0x07.toChar

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
sealed trait AbstractSubObjectGetFieldsDescriptor extends NestedGetFieldsDescriptor, AbstractObjectGetFieldsDescriptor
sealed trait AbstractSubObjectGetFieldsDescriptorExpanded extends NestedGetFieldsDescriptorExpanded, AbstractObjectGetFieldsDescriptorExpanded
    
object AllGetFieldsDescriptor extends AllGetFieldsDescriptor

case class ObjectGetFieldsDescriptor(fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]])
    extends AbstractObjectGetFieldsDescriptor

case class ObjectGetFieldsDescriptorExpanded(fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptorExpanded]])
    extends AbstractObjectGetFieldsDescriptorExpanded

case class SubObjectGetFieldsDescriptor(
    fieldName: String, 
    fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]]
) extends AbstractSubObjectGetFieldsDescriptor

object SubObjectGetFieldsDescriptor:
    def apply(fieldName: String, fields: List[NestedGetFieldsDescriptor]): SubObjectGetFieldsDescriptor =
        SubObjectGetFieldsDescriptor(fieldName, Right(fields))

case class SubObjectGetFieldsDescriptorExpanded(
    fieldName: String,
    fields: List[NestedGetFieldsDescriptorExpanded]
) extends NestedGetFieldsDescriptorExpanded, AbstractSubObjectGetFieldsDescriptorExpanded

//Field descriptor for referenced primitive type
case class PrimitiveGetFieldsDescriptor(
    fieldName: String,
    isGet: Boolean,
    searchPathes: List[FieldPathChainCell]
) extends AbstractSingleFieldGetFieldsDescriptor, AbstractObjectGetFieldsDescriptor, AbstractObjectGetFieldsDescriptorExpanded:
    def fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]] = Left(AllGetFieldsDescriptor)
    
case class SingleGetFieldsDescriptor(
    fieldName: String,
    isGet: Boolean,
    searchPathes: List[FieldPathChainCell]
) extends AbstractSingleFieldGetFieldsDescriptor
        
case class ListGetFieldsDescriptor(
    fieldName: String, 
    limit: Option[Int], 
    offset: Option[Int]
) extends AbstractSingleFieldGetFieldsDescriptor

case class ListSubObjectGetFieldsDescriptor(
    fieldName: String,
    fields: Either[AllGetFieldsDescriptor, List[NestedGetFieldsDescriptor]],
    limit: Option[Int],
    offset: Option[Int]
) extends AbstractSubObjectGetFieldsDescriptor

case class ListSubObjectGetFieldsDescriptorExpanded(
    fieldName: String,
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
    lazy val path: FieldPathChainCell = buildPath(None)
    @tailrec
    private def buildPath(next: Option[FieldPathChainCell]): FieldPathChainCell = 
        val currentNode = FieldPathChainCell(fieldName, next)
        referer match
            case Some(ref) => ref.buildPath(Some(currentNode))
            case None => currentNode
    
case class FieldPathChainCell(fieldName: String, nextCell: Option[FieldPathChainCell] = None):
    def subPath(nextFieldName: String): FieldPathChainCell =
        nextCell match
            case Some(nextCell) =>
                FieldPathChainCell(fieldName, Some(nextCell.subPath(nextFieldName)))
            case None =>
                FieldPathChainCell(fieldName, Some(FieldPathChainCell(nextFieldName, None)))
    

final case class Range[V](from: V, to: V)

sealed trait RawSearchCondition:
    def field: FieldPathChainCell

sealed trait RawMultyValuesSearchCondition extends RawSearchCondition:
    def values: List[String]
    
type ValueParser = String => Either[SearchConditionParseError, Any]
    
sealed trait RawSearchConditionSelfConstructable extends RawSearchCondition:
    def parseValueAndCreate(
                                     valueParser: ValueParser,
                                     isSet: Boolean
                                 ): Either[SearchConditionParseError, SingleFieldSearchCondition]

sealed trait RawSimpmpleStringSearchCondition extends RawSearchConditionSelfConstructable:
    def value: String
    def createForOneValue(value: Any): SingleFieldSearchCondition
    def createForSet(value: List[(AbstractArrayEntityType[_, _], Any)]): ArraySingleFieldSearchCondition
    def parseValueAndCreate(
        valueParsers: ValueParser | Map[AbstractArrayEntityType[_, _], ValueParser]
    ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        valueParsers match 
            case valueParser: ValueParser =>
                valueParser(value).map(parsedValue => createForOneValue(parsedValue))
            case valueParsersMap: Map[AbstractArrayEntityType[_, _], ValueParser] =>
                val (result, errors) = valueParsersMap
                    .map((arrType, valueParser) =>                        
                        arrType -> valueParser(value)
                    )
                    .foldLeft((Nil, Nil): (List[(AbstractArrayEntityType[_, _], Any)], List[(AbstractArrayEntityType[_, _], SearchConditionParseError)])) {
                        case ((acc, errors), (arrType, parseResult)) =>
                            parseResult match
                                case Right(value) => (arrType -> value :: acc, errors)
                                case Left(error) => (acc, arrType -> error :: errors)
                    }
                if (result.nonEmpty)
                    Right(createForSet(result))
                else 
                    val errorMessage = errors.map(e => s" - ${e._1.name}: ${e._2.message}").mkString(";\n")
                    Left(SearchConditionParseError(s"Error parsing value for array condition! All types parse fails: $errorMessage"))

final case class EqRawSearchCondition(field: FieldPathChainCell, value: String, allItems: Boolean) extends RawSimpmpleStringSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")

    def createForOneValue(value: Any): EqSearchCondition =
        EqSearchCondition(field, value)
    def createForSet(value: List[(AbstractArrayEntityType[_, _], Any)]): ArrayEqSearchCondition =
        ArrayEqSearchCondition(field, value, allItems)

final case class GtRawSearchCondition(field: FieldPathChainCell, value: String, allItems: Boolean) extends RawSimpmpleStringSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    
    def createForOneValue(value: Any): GtSearchCondition =
        GtSearchCondition(field, value)
    def createForSet(value: List[(AbstractArrayEntityType[_, _], Any)]): ArrayGtSearchCondition =
        ArrayGtSearchCondition(field, value, allItems)

final case class GeRawSearchCondition(field: FieldPathChainCell, value: String, allItems: Boolean) extends RawSimpmpleStringSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    
    def createForOneValue(value: Any): GeSearchCondition =
        GeSearchCondition(field, value)
    def createForSet(value: List[(AbstractArrayEntityType[_, _], Any)]): ArrayGeSearchCondition =
        ArrayGeSearchCondition(field, value, allItems)

final case class LtRawSearchCondition(field: FieldPathChainCell, value: String, allItems: Boolean) extends RawSimpmpleStringSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    
    def createForOneValue(value: Any): LtSearchCondition =
        LtSearchCondition(field, value)
    def createForSet(value: List[(AbstractArrayEntityType[_, _], Any)]): ArrayLtSearchCondition =
        ArrayLtSearchCondition(field, value, allItems)

final case class LeRawSearchCondition(field: FieldPathChainCell, value: String, allItems: Boolean) extends RawSimpmpleStringSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    
    def createForOneValue(value: Any, isSet: Boolean): LeSearchCondition =
        LeSearchCondition(field, value)
    def createForSet(value: List[(AbstractArrayEntityType[_, _], Any)]): ArrayLeSearchCondition =
        ArrayLeSearchCondition(field, value, allItems)

final case class BetweenRawSearchCondition(field: FieldPathChainCell, value: Range[String], allItems: Boolean) extends RawSearchConditionSelfConstructable:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    
    def parseValueAndCreate(
                               valueParser: ValueParser,
                               arrayTypesSet: Set[AbstractArrayEntityType[_, _]]
                           ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        if (arrayTypesSet.nonEmpty)
            valueParser(value.from).flatMap(fromValue =>
                valueParser(value.to).map(toValue =>
                    //List[(AbstractArrayEntityType[_, _], Range[Any])]
                    ArrayBetweenSearchCondition(field, Range(fromValue, toValue), allItems)
                )
            )
        else
            valueParser(value.from).flatMap(fromValue =>
                valueParser(value.to).map(toValue =>
                    BetweenSearchCondition(field, Range(fromValue, toValue))
                )
            )

final case class InRawSearchCondition(
                                         field: FieldPathChainCell, 
                                         values: List[String], 
                                         allItems: Boolean
                                     ) extends RawSearchConditionSelfConstructable, RawMultyValuesSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")

    def parseValueAndCreate(
        valueParser: ValueParser,
        isSet: Boolean
    ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        if (isSet)
            values.foldLeft(Right(Nil): Either[SearchConditionParseError, List[Any]]) {
                    case (Right(acc), valueItem) =>
                        valueParser(valueItem).map(_ :: acc)
                    case (left@Left(_), _) =>
                        left
                }
                .map(parsedValue =>
                    if allItems then
                        IsSubSetSearchCondition(field, parsedValue)
                    else
                        IsIntersectsSearchCondition(field, parsedValue)
                )
        else
            values.foldLeft(Right(Nil): Either[SearchConditionParseError, List[Any]]) {
                    case (Right(acc), valueItem) =>
                        valueParser(valueItem).map(_ :: acc)
                    case (left@Left(_), _) =>
                        left
                }
                .map(parsedValue => InSearchCondition(field, parsedValue))

final case class LikeRawSearchCondition(field: FieldPathChainCell, value: String, allItems: Boolean) extends RawSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def createForOneValue(isStringOrFieldTypes: Boolean | Set[AbstractArrayEntityType[_, _]],
                          isSet: Boolean): SingleFieldSearchCondition =
        (isStringOrFieldTypes, isSet) match
            case (isString: Boolean, false) =>
                LikeSearchCondition(field, value, isString)
            case (_, true) =>
                ArrayLikeSearchCondition(field, value, isStringOrFieldTypes, allItems)
            case _ =>
                throw new IllegalArgumentException(s"Invalid parameters for LikeRawSearchCondition! " +
                    s"isStringOrFieldTypes: $isStringOrFieldTypes, isSet: $isSet")

final case class IsOfTypeRawSearchCondition(field: FieldPathChainCell, entityTypeName: String, allItems: Boolean) extends RawSearchCondition:
    require(field != null, "Field path cannot be null")
    require(entityTypeName != null, "EntityTypeName cannot be null")
    def createForOneValue(entityType: AbstractEntityType[_, _, _],
                          isSet: Boolean): SingleFieldSearchCondition =
        if (isSet)
            ArrayIsOfTypeSearchCondition(field, entityType)
        else
            IsOfTypeSearchCondition(field, entityType)

sealed trait RawSetOnlySearchConditionSelfConstructable extends RawSearchCondition:
    def create(value: List[Any]): SingleFieldSearchCondition
    def values: List[String]
    def checkValueAndCreateForSet(
        valueParser: ValueParser
    ): Either[SearchConditionParseError, SingleFieldSearchCondition] =
        values.foldLeft(Right(Nil): Either[SearchConditionParseError, List[Any]]) {
                case (Right(acc), valueItem) =>
                    valueParser(valueItem).map(_ :: acc)
                case (left@Left(_), _) =>
                    left
            }
            .map(parsedValue => create(parsedValue))

final case class IsSubSetRawSearchCondition(
                                               field: FieldPathChainCell, 
                                               values: List[String]
                                           ) extends RawSetOnlySearchConditionSelfConstructable, RawMultyValuesSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def create(values: Map[AbstractArrayEntityType[_, _], List[Any]]): IsSubSetSearchCondition =
        IsSubSetSearchCondition(field, values)

final case class IsSuperSetRawSearchCondition(
                                                 field: FieldPathChainCell, 
                                                 values: List[String]
                                             ) extends RawSetOnlySearchConditionSelfConstructable, RawMultyValuesSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def create(values: Map[AbstractArrayEntityType[_, _], List[Any]]): IsSuperSetSearchCondition =
        IsSuperSetSearchCondition(field, values)

final case class IsEqualsSetRawSearchCondition(
                                                 field: FieldPathChainCell, 
                                                 values: List[String]
                                             ) extends RawSetOnlySearchConditionSelfConstructable, RawMultyValuesSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def create(values: Map[AbstractArrayEntityType[_, _], List[Any]]): IsEqualSetSearchCondition =
        IsEqualSetSearchCondition(field, values)

final case class IsIntersectsRawSearchCondition(
                                                   field: FieldPathChainCell, 
                                                   values: List[String]
                                               ) extends RawSetOnlySearchConditionSelfConstructable, RawMultyValuesSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def create(values: Map[AbstractArrayEntityType[_, _], List[Any]]): IsIntersectsSearchCondition =
        IsIntersectsSearchCondition(field, values)

final case class IsEmptyRawSearchCondition(
                                              field: FieldPathChainCell
                                          ) extends RawSearchCondition, RawMultyValuesSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def createForSet(): IsEmptySearchCondition =
        IsEmptySearchCondition(field)



sealed trait SearchCondition:
    /**
     * Translate field path to SQL
     * @param translateFieldPath - function to translate field path to SQL representation including table alias and column name. 
     *                           SHOULD ALWAYS BE CALLED BEFORE bindParameter, except case when bindParameter binds subtype!
     * @param bindParameter - function to parse parameter string representation, bind it to SQL statement and return parameter placeholder with correct number
     * @return SQL representation of the condition
     */    
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String
    def expectedSubTypeName: Option[String] = None

final case class NotSearchCondition(condition: SearchCondition) extends SearchCondition:
    require(condition != null, "Condition cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        "NOT (" + condition.toSQL(sqlData, translateFieldPath, bindParameter) + ")"

sealed trait CombinedSearchCondition extends SearchCondition:
    def conditions: List[SearchCondition]
    def ::(condition: SearchCondition): CombinedSearchCondition

final case class AndSearchCondition(conditions: List[SearchCondition]) extends CombinedSearchCondition:
    require(conditions != null, "Conditions cannot be null")
    require(conditions.nonEmpty, "Conditions cannot be empty")
    require(!conditions.contains(null), "Conditions cannot be null")
    def ::(condition: SearchCondition): AndSearchCondition = AndSearchCondition(condition :: conditions)
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        conditions.map("(" + _.toSQL(sqlData, translateFieldPath, bindParameter) + ")").mkString(" AND ")

object AndSearchCondition:
    def apply(conditions: SearchCondition*): AndSearchCondition = AndSearchCondition(conditions.toList)

final case class OrSearchCondition(conditions: List[SearchCondition]) extends CombinedSearchCondition:
    require(conditions != null, "Conditions cannot be null")
    require(conditions.nonEmpty, "Conditions cannot be empty")
    require(!conditions.contains(null), "Conditions cannot be null")
    def ::(condition: SearchCondition): OrSearchCondition = OrSearchCondition(condition :: conditions)
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        conditions.map(_.toSQL(sqlData, translateFieldPath, bindParameter)).mkString(" OR ")

object OrSearchCondition:
    def apply(conditions: SearchCondition*): OrSearchCondition = OrSearchCondition(conditions.toList)

sealed trait SingleFieldSearchCondition extends SearchCondition:
    def field: FieldPathChainCell

final case class EqSearchCondition(field: FieldPathChainCell, value: Any) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + (if (value == null) " IS NULL" else " = " + bindParameter(value))
final case class GtSearchCondition(field: FieldPathChainCell, value: Any) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + " > " + bindParameter(value)
final case class GeSearchCondition(field: FieldPathChainCell, value: Any) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + " >= " + bindParameter(value)
final case class LtSearchCondition(field: FieldPathChainCell, value: Any) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + " < " + bindParameter(value)
final case class LeSearchCondition(field: FieldPathChainCell, value: Any) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + " <= " + bindParameter(value)
final case class BetweenSearchCondition(field: FieldPathChainCell, value: Range[Any]) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        if (sqlData.isBetweenSupported)
            translateFieldPath(field) + " BETWEEN " + bindParameter(value.from) + " AND " + bindParameter(value.to)
        else
            val qualifiedFieldName = translateFieldPath(field)
            qualifiedFieldName + " >= " + bindParameter(value.from) + " AND " + qualifiedFieldName + " < " + bindParameter(value.to)

final case class LikeSearchCondition(field: FieldPathChainCell, value: String, fieldTypeIsString: Boolean) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + " LIKE " + bindParameter(value.replaceAll(GlobalConstants.searchWildcard, sqlData.likeWildcard))
        
final case class InSearchCondition(field: FieldPathChainCell, value: List[Any]) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String =
        translateFieldPath(field) + " IN (" + bindParameter(value) + ")"
        
final case class IsOfTypeSearchCondition(field: FieldPathChainCell, entityType: AbstractEntityType[_, _, _]) extends SingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(entityType != null, "EntityType cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindType: Any => String): String =
        val idPath = field.subPath(GlobalConstants.entityIdFieldNameForDsc)
        translateFieldPath(idPath) + " IS NOT NULL"
    def expectedSubTypeName: Option[String] = Some(entityType.name)
    
//Array search conditions
sealed trait ArraySingleFieldSearchCondition extends SingleFieldSearchCondition

final case class ArrayEqSearchCondition(
                                           field: FieldPathChainCell,
                                           value: List[(AbstractArrayEntityType[_, _], Any)],
                                           allItems: Boolean
                                       ) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class ArrayGtSearchCondition(field: FieldPathChainCell, value: List[(AbstractArrayEntityType[_, _], Any)], allItems: Boolean) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO
final case class ArrayGeSearchCondition(field: FieldPathChainCell, value: List[(AbstractArrayEntityType[_, _], Any)], allItems: Boolean) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO
final case class ArrayLtSearchCondition(field: FieldPathChainCell, value: List[(AbstractArrayEntityType[_, _], Any)], allItems: Boolean) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO
final case class ArrayLeSearchCondition(field: FieldPathChainCell, value: List[(AbstractArrayEntityType[_, _], Any)], allItems: Boolean) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO
final case class ArrayBetweenSearchCondition(field: FieldPathChainCell, value: List[(AbstractArrayEntityType[_, _], Range[Any])], allItems: Boolean) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class ArrayLikeSearchCondition(field: FieldPathChainCell, value: String, isStringOrFieldTypes: Boolean | Set[AbstractArrayEntityType[_, _]], allItems: Boolean) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(value != null, "Value cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class ArrayIsOfTypeSearchCondition(field: FieldPathChainCell, entityType: AbstractEntityType[_, _, _]) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(entityType != null, "EntityType cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO
    def expectedSubTypeName: Option[String] = Some(entityType.name)

final case class IsSubSetSearchCondition(
                                               field: FieldPathChainCell,
                                               values: Map[AbstractArrayEntityType[_, _], List[Any]]
                                           ) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class IsSuperSetSearchCondition(
                                                 field: FieldPathChainCell,
                                                 values: Map[AbstractArrayEntityType[_, _], List[Any]]
                                             ) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class IsEqualSetSearchCondition(
                                                 field: FieldPathChainCell,
                                                 values: Map[AbstractArrayEntityType[_, _], List[Any]]
                                             ) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class IsIntersectsSearchCondition(
                                                   field: FieldPathChainCell,
                                                   values: Map[AbstractArrayEntityType[_, _], List[Any]]
                                               ) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    require(values != null, "Values cannot be null")
    require(!values.contains(null), "Values items cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO

final case class IsEmptySearchCondition(field: FieldPathChainCell) extends ArraySingleFieldSearchCondition:
    require(field != null, "Field path cannot be null")
    def toSQL(sqlData: SqlData, translateFieldPath: FieldPathChainCell => String, bindParameter: Any => String): String = ??? //TODO