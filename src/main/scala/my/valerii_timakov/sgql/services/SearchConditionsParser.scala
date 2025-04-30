package my.valerii_timakov.sgql.services

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.SearchConditionParseError
import my.valerii_timakov.sgql.entity.domain.types.EntityType
import my.valerii_timakov.sgql.entity.read_modiriers.{AndSearchCondition, BetweenRawSearchCondition, BetweenSearchCondition, CombinedSearchCondition, EqRawSearchCondition, EqSearchCondition, FieldPathChainCell, GeRawSearchCondition, GeSearchCondition, GlobalConstants, GtRawSearchCondition, GtSearchCondition, InRawSearchCondition, InSearchCondition, IsEmptyRawSearchCondition, IsEqualsSetRawSearchCondition, IsIntersectsRawSearchCondition, IsOfTypeRawSearchCondition, IsOfTypeSearchCondition, IsSubSetRawSearchCondition, IsSuperSetRawSearchCondition, LeRawSearchCondition, LeSearchCondition, LikeRawSearchCondition, LikeSearchCondition, LtRawSearchCondition, LtSearchCondition, NotSearchCondition, OrSearchCondition, Range, RawSearchCondition, SearchCondition}

import java.util.regex.Pattern
import scala.jdk.StreamConverters.*


class SearchConditionsParser(
                                conf: Config,
                                typesDefinitionProvider: TypesDefinitionProvider,
                            ):

    private final val FieldsDelimiterInChain = conf.getString("fields-delimiter")
    private final val EqConditionSign = conf.getString("eq")
    private final val GraterThanConditionSign = conf.getString("gt")
    private final val LessThanConditionSign = conf.getString("lt")
    private final val GraterOrEqualConditionSign = conf.getString("ge")
    private final val LessOrEqualConditionSign = conf.getString("le")
    private final val LikeConditionSign = conf.getString("like")
    private final val InConditionSign = conf.getString("in")
    private final val IsConditionSign = conf.getString("is")
    private final val AllConditionSign = conf.getString("all")
    private final val IsSubSetConditionSign = conf.getString("subset")
    private final val IsSuperSetConditionSign = conf.getString("superset")
    private final val IsIntersectsConditionSign = conf.getString("intersects")
    private final val IsEqualsSetConditionSign = conf.getString("equalsset")
    private final val IsEmptyConditionSign = conf.getString("empty")
    private final val AndOperatorSign = conf.getString("and")
    private final val OrOperatorSign = conf.getString("or")
    private final val NotOperatorSign = conf.getString("not")
    private final val Wildcard = conf.getString("wildcard")
    private final val AllUniConditionsLine = List(EqConditionSign, GraterThanConditionSign, LessThanConditionSign,
        GraterOrEqualConditionSign, LessOrEqualConditionSign, LikeConditionSign, InConditionSign, IsConditionSign)
        .map(Pattern.quote)
        .map(sign => Pattern.quote(AllConditionSign) + "?" + sign)
        .mkString("|")
    private final val AllSetConditionsLine = List(IsSubSetConditionSign, IsSuperSetConditionSign, IsIntersectsConditionSign, 
        IsEqualsSetConditionSign, IsEmptyConditionSign)
        .map(Pattern.quote)
        .mkString("|")
    private final val ConditionStartRE = s"""^([\\w_]+(\\.[\\w_]+)*)($AllUniConditionsLine|$AllSetConditionsLine)""".r
    private final val ValueEndRE = """\+|\*""".r
    private final val ValueEndInsideParenthesisRE = """(?<!\\)[+*)]""".r
    private final val IntervalMiddleMark = ".."
    private final val ValueEndInsideListRE = """(?<!\\),""".r

    private final val uniConditionConstructors: Map[String, (FieldPathChainCell, String, Boolean) => RawSearchCondition] = Map(
        EqConditionSign -> EqRawSearchCondition.apply,
        GraterThanConditionSign -> GtRawSearchCondition.apply,
        LessThanConditionSign -> LtRawSearchCondition.apply,
        GraterOrEqualConditionSign -> GeRawSearchCondition.apply,
        LessOrEqualConditionSign -> LeRawSearchCondition.apply,
        LikeConditionSign -> LikeRawSearchCondition.apply,
        IsConditionSign -> IsOfTypeRawSearchCondition.apply,
    )
    
    private final val setConditionConstructors: Map[String, (FieldPathChainCell, List[String]) => RawSearchCondition] = Map(
        IsSubSetConditionSign -> IsSubSetRawSearchCondition.apply,
        IsSuperSetConditionSign -> IsSuperSetRawSearchCondition.apply, 
        IsIntersectsConditionSign -> IsIntersectsRawSearchCondition.apply,
        IsEqualsSetConditionSign -> IsEqualsSetRawSearchCondition.apply, 
    )

    def parse(input: String, entityType: EntityType[_, _, _]): Either[SearchConditionParseError, SearchCondition] =
        parseExpression(input, None, false, entityType) match
            case Right((condition, "")) =>
                Right(condition)
            case Right((_, rest)) =>
                Left(SearchConditionParseError(s"Unexpected end of expression in ...$rest!"))
            case Left(error) =>
                Left(error)


    private def parseExpression(
                                   input: String,
                                   prevCondition: Option[CombinedSearchCondition],
                                   expectParenthesis: Boolean,
                                   entityType: EntityType[_, _, _]
                               ): Either[SearchConditionParseError, (SearchCondition, String)] =
        val (isNot, inputRestAfterNot) =
            if (input.startsWith(NotOperatorSign))
                (true, input.substring(NotOperatorSign.length))
            else
                (false, input)
        val currConditionRes: Either[SearchConditionParseError, (SearchCondition, String)] =
            if (inputRestAfterNot.startsWith("("))
                parseExpression(inputRestAfterNot.substring(1), None, true, entityType) match
                    case Right((experrsion, rest)) =>
                        if (rest.isEmpty || rest(0) != ')')
                            Left(SearchConditionParseError(s"Closing parenthesis not found in ...$rest!"))
                        else
                            Right((experrsion, rest.substring(1)))
                    case Left(error) =>
                        Left(error)
            else
                parseCondition(inputRestAfterNot, expectParenthesis, entityType)

        currConditionRes match
            case Right((condition, inputRestAfter1Condition)) =>
                val conditionWithNot = if (isNot) NotSearchCondition(condition) else condition
                if (inputRestAfter1Condition.isEmpty)
                    val resCondition = prevCondition match
                        case Some(condition: CombinedSearchCondition) =>
                            conditionWithNot :: condition
                        case None =>
                            conditionWithNot
                    Right((resCondition, ""))
                else
                    if (inputRestAfter1Condition.startsWith(AndOperatorSign))
                        val inputAfterCombineOperator = inputRestAfter1Condition.substring(AndOperatorSign.length)
                        prevCondition match
                            case Some(prevCondition: AndSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(conditionWithNot :: prevCondition), expectParenthesis, entityType)
                            case Some(prevCondition: OrSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(AndSearchCondition(conditionWithNot)), expectParenthesis, entityType)
                                    .map((nextCondition, inputAfterNextCondition) => (nextCondition :: prevCondition, inputAfterNextCondition))
                            case None =>
                                parseExpression(inputAfterCombineOperator, Some(AndSearchCondition(conditionWithNot)), expectParenthesis, entityType)
                    else if (inputRestAfter1Condition.startsWith(OrOperatorSign))
                        val inputAfterCombineOperator = inputRestAfter1Condition.substring(OrOperatorSign.length)
                        prevCondition match
                            case Some(prevCondition: OrSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(conditionWithNot :: prevCondition), expectParenthesis, entityType)
                            case Some(prevCondition: AndSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(prevCondition :: OrSearchCondition(conditionWithNot)), expectParenthesis, entityType)
                            case None =>
                                parseExpression(inputAfterCombineOperator, Some(OrSearchCondition(conditionWithNot)), expectParenthesis, entityType)
                        Left(SearchConditionParseError(""))
                    else if (expectParenthesis && inputRestAfter1Condition.startsWith(")"))
                        Right((conditionWithNot, inputRestAfter1Condition))
                    else
                        Left(SearchConditionParseError(s"Unexpected end of expression in ...$inputRestAfter1Condition! " +
                            s"'$OrOperatorSign', '$AndOperatorSign', ')' or end of input expected!"))
            case Left(error) =>
                Left(error)

    private def parseCondition(
                                  input: String,
                                  insideParenthesis: Boolean,
                                  entityType: EntityType[_, _, _]
                              ): Either[SearchConditionParseError, (SearchCondition, String)] =

        def parseFieldsChain(input: String): FieldPathChainCell =
            val firstFieldEnd = input.indexOf(FieldsDelimiterInChain)
            if (firstFieldEnd == -1)
                FieldPathChainCell(input, None)
            else
                val currFieldName = input.substring(0, firstFieldEnd)
                val nextChain = Some(parseFieldsChain(input.substring(firstFieldEnd + FieldsDelimiterInChain.length)))
                FieldPathChainCell(currFieldName, nextChain)
        def parseValue(input: String, insideParenthesis: Boolean): (String, String) =
            val valueEndRE = if (insideParenthesis) ValueEndInsideParenthesisRE else ValueEndRE
            valueEndRE.findFirstMatchIn(input).map(endMatch =>
                (input.substring(0, endMatch.start), input.substring(endMatch.start))
            ).getOrElse((input, ""))
        def parseValues(input: String, insideParenthesis: Boolean): (List[String], String) =
            val (listValues, inputAfterList) = parseValue(input, insideParenthesis)
            val result = ValueEndInsideListRE.pattern.splitAsStream(inputAfterList).toScala(List)
            (result, inputAfterList)
        def parseInterval(input: String, insideParenthesis: Boolean): Either[SearchConditionParseError, (Range[String], String)] =
            val intervalMiddle = input.indexOf(IntervalMiddleMark)
            if (intervalMiddle == -1)
                Left(SearchConditionParseError(s"Interval end not found in $input!"))
            else
                val startOfIntervalValue = input.substring(0, intervalMiddle)
                val inputRest = input.substring(intervalMiddle + IntervalMiddleMark.length)
                val (endOfIntervalValue, afterIntervalInputRest) = parseValue(inputRest, insideParenthesis)
                Right(Range(startOfIntervalValue, endOfIntervalValue), afterIntervalInputRest)
        ConditionStartRE.findFirstMatchIn(input).map(match1 =>
                val fieldName = match1.group(1)
                if (fieldName.isEmpty)
                    Left(SearchConditionParseError(s"Empty field name in search condition $input!"))
                else
                    val fieldsChain = parseFieldsChain(fieldName)
                    val operatorRaw = match1.group(3)
                    val inputRest = input.substring(match1.end(0))
                    val (operator, all) = 
                        if (operatorRaw.startsWith(AllConditionSign)) {
                            val operatorWithoutAll = operatorRaw.substring(AllConditionSign.length)
                            (operatorWithoutAll, true)
                        } else {
                            (operatorRaw, false)
                        }
                    
                    val searchCondition = operator match
                        case `EqConditionSign` | `GraterThanConditionSign` | `LessThanConditionSign` |
                             `GraterOrEqualConditionSign` | `LessOrEqualConditionSign` | `LikeConditionSign` | `IsConditionSign` =>
                            val constructor = uniConditionConstructors(operator)
                            var (value, rest) = parseValue(inputRest, insideParenthesis)
                            if (operator == LikeConditionSign) {
                                value = value.replace(Wildcard, GlobalConstants.searchWildcard)
                            }
                            (constructor(fieldsChain, value, all), rest)
                        case `InConditionSign` =>
                            parseInterval(inputRest, insideParenthesis) match
                                case Right((value, rest)) =>
                                    (BetweenRawSearchCondition(fieldsChain, value, all), rest)
                                case Left(_) =>
                                    val (value, rest) = parseValues(inputRest, insideParenthesis)
                                    (InRawSearchCondition(fieldsChain, value, all), rest)
                        case `IsSubSetConditionSign` | `IsSuperSetConditionSign` | `IsIntersectsConditionSign` | `IsEqualsSetConditionSign` =>
                            val constructor = setConditionConstructors(operator)
                            val (value, rest) = parseValues(inputRest, insideParenthesis)
                            (constructor(fieldsChain, value), rest)
                        case `IsEmptyConditionSign` =>
                            (IsEmptyRawSearchCondition(fieldsChain), inputRest)
                    Right(searchCondition)
            )
            .getOrElse(Left(SearchConditionParseError(s"Search condition not found! Invalid input: $input!")))
            .flatMap(conditionRawRes => typesDefinitionProvider
                .validateAndParseSearchCondition(conditionRawRes._1, entityType)
                .map(condition => (condition, conditionRawRes._2))
            )

    
