package my.valerii_timakov.sgql.services

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.SearchConditionParseError
import my.valerii_timakov.sgql.entity.read_modiriers.{AndSearchCondition, BetweenSearchCondition, CombinedSearchCondition, EqSearchCondition, GeSearchCondition, GtSearchCondition, InSearchCondition, LeSearchCondition, LikeSearchCondition, LtSearchCondition, NotSearchCondition, OrSearchCondition, Range, SearchCondition, SearchFieldChainCell}

import java.util.regex.Pattern

class SearchConditionsParser(conf: Config):

    private final val FieldsDelimiterInChain = conf.getString("fields-delimiter")
    private final val EqConditionSign = conf.getString("eq")
    private final val GraterThanConditionSign = conf.getString("gt")
    private final val LessThanConditionSign = conf.getString("lt")
    private final val GraterOrEqualConditionSign = conf.getString("ge")
    private final val LessOrEqualConditionSign = conf.getString("le")
    private final val LikeConditionSign = conf.getString("like")
    private final val InConditionSign = conf.getString("in")
    private final val AndOperatorSign = conf.getString("and")
    private final val OrOperatorSign = conf.getString("or")
    private final val NotOperatorSign = conf.getString("not")
    private final val ReferencedSubtypeSpecifierStartMark = conf.getString("ref-subtype-specifier-start")
    private final val ReferencedSubtypeNamespacesDelimiterMark = conf.getString("ref-subtype-namespaces-delimiter")
    private final val RefSbtpSpcStMrk = Pattern.quote(ReferencedSubtypeSpecifierStartMark)
    private final val RefSbtpNmspDlmMrk = Pattern.quote(ReferencedSubtypeNamespacesDelimiterMark)
    private final val AllConditionsLine = List(EqConditionSign, GraterThanConditionSign, LessThanConditionSign,
        GraterOrEqualConditionSign, LessOrEqualConditionSign, LikeConditionSign, InConditionSign)
        .map(Pattern.quote)
        .mkString("|")
    private final val ConditionStartRE = s"""^([\\w_]+(($RefSbtpSpcStMrk[\\w_$RefSbtpNmspDlmMrk]+)?\\.[\\w_]+)*)($AllConditionsLine)""".r
    private final val ValueEndRE = """\+|\*""".r
    private final val ValueEndInsideParenthesisRE = """(?<!\\)[+*)]""".r
    private final val IntervalMiddleMark = ".."
    private final val ValueEndInsideListRE = """(?<!\\),""".r
    private final val NAMESPACES_DELIMITER = "" + TypesDefinitionsParser.NAMESPACES_DELIMITER

    private final val conditionConstructors: Map[String, (SearchFieldChainCell, String) => SearchCondition] = Map(
        EqConditionSign -> EqSearchCondition.apply,
        GraterThanConditionSign -> GtSearchCondition.apply,
        LessThanConditionSign -> LtSearchCondition.apply,
        GraterOrEqualConditionSign -> GeSearchCondition.apply,
        LessOrEqualConditionSign -> LeSearchCondition.apply,
        LikeConditionSign -> LikeSearchCondition.apply,
    )

    def parse(input: String): Either[SearchConditionParseError, SearchCondition] =
        parseExpression(input, None, false) match
            case Right((condition, "")) =>
                Right(condition)
            case Right((_, rest)) =>
                Left(SearchConditionParseError(s"Unexpected end of expression in ...$rest!"))
            case Left(error) =>
                Left(error)


    private def parseExpression(
                                   input: String,
                                   prevCondition: Option[CombinedSearchCondition],
                                   expectParenthesis: Boolean
                               ): Either[SearchConditionParseError, (SearchCondition, String)] =
        val (isNot, inputRestAfterNot) =
            if (input.startsWith(NotOperatorSign))
                (true, input.substring(NotOperatorSign.length))
            else
                (false, input)
        val currConditionRes: Either[SearchConditionParseError, (SearchCondition, String)] =
            if (inputRestAfterNot.startsWith("("))
                parseExpression(inputRestAfterNot.substring(1), None, true) match
                    case Right((experrsion, rest)) =>
                        if (rest.isEmpty || rest(0) != ')')
                            Left(SearchConditionParseError(s"Closing parenthesis not found in ...$rest!"))
                        else
                            Right((experrsion, rest.substring(1)))
                    case Left(error) =>
                        Left(error)
            else
                parseCondition(inputRestAfterNot, expectParenthesis)

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
                                parseExpression(inputAfterCombineOperator, Some(conditionWithNot :: prevCondition), expectParenthesis)
                            case Some(prevCondition: OrSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(AndSearchCondition(conditionWithNot)), expectParenthesis)
                                    .map((nextCondition, inputAfterNextCondition) => (nextCondition :: prevCondition, inputAfterNextCondition))
                            case None =>
                                parseExpression(inputAfterCombineOperator, Some(AndSearchCondition(conditionWithNot)), expectParenthesis)
                    else if (inputRestAfter1Condition.startsWith(OrOperatorSign))
                        val inputAfterCombineOperator = inputRestAfter1Condition.substring(OrOperatorSign.length)
                        prevCondition match
                            case Some(prevCondition: OrSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(conditionWithNot :: prevCondition), expectParenthesis)
                            case Some(prevCondition: AndSearchCondition) =>
                                parseExpression(inputAfterCombineOperator, Some(prevCondition :: OrSearchCondition(conditionWithNot)), expectParenthesis)
                            case None =>
                                parseExpression(inputAfterCombineOperator, Some(OrSearchCondition(conditionWithNot)), expectParenthesis)
                        Left(SearchConditionParseError(""))
                    else if (expectParenthesis && inputRestAfter1Condition.startsWith(")"))
                        Right((conditionWithNot, inputRestAfter1Condition))
                    else
                        Left(SearchConditionParseError(s"Unexpected end of expression in ...$inputRestAfter1Condition! " +
                            s"'$OrOperatorSign', '$AndOperatorSign', ')' or end of input expected!"))
            case Left(error) =>
                Left(error)

    private def parseCondition(input: String, insideParenthesis: Boolean): Either[SearchConditionParseError, (SearchCondition, String)] =

        def parseFieldsChain(input: String): SearchFieldChainCell =
            val firstFieldEnd = input.indexOf(FieldsDelimiterInChain)
            if (firstFieldEnd == -1)
                SearchFieldChainCell(input, None, None)
            else
                val currFieldName = input.substring(0, firstFieldEnd)
                val nextChain = Some(parseFieldsChain(input.substring(firstFieldEnd + FieldsDelimiterInChain.length)))
                val rsssmIndex = currFieldName.indexOf(ReferencedSubtypeSpecifierStartMark)
                if (rsssmIndex != -1)
                    val subTypeRef = currFieldName.substring(rsssmIndex + ReferencedSubtypeSpecifierStartMark.length)
                        .replaceAll(ReferencedSubtypeNamespacesDelimiterMark, NAMESPACES_DELIMITER)
                    SearchFieldChainCell(currFieldName.substring(0, rsssmIndex), Some(subTypeRef), nextChain)
                else
                    SearchFieldChainCell(currFieldName, None, nextChain)
        def parseValue(input: String, insideParenthesis: Boolean): (String, String) =
            val valueEndRE = if (insideParenthesis) ValueEndInsideParenthesisRE else ValueEndRE
            valueEndRE.findFirstMatchIn(input).map(endMatch =>
                (input.substring(0, endMatch.start), input.substring(endMatch.start))
            ).getOrElse((input, ""))
        def parseValues(input: String, insideParenthesis: Boolean): (Array[String], String) =
            val (listValues, inputAfterList) = parseValue(input, insideParenthesis)
            (listValues.split(ValueEndInsideListRE.regex), inputAfterList)
        def parseInterval(input: String, insideParenthesis: Boolean): Either[SearchConditionParseError, (Range, String)] =
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
                    val operator = match1.group(4)
                    val inputRest = input.substring(match1.end(0))
                    val searchCondition = operator match
                        case `EqConditionSign` | `GraterThanConditionSign` | `LessThanConditionSign` |
                             `GraterOrEqualConditionSign` | `LessOrEqualConditionSign` | `LikeConditionSign` =>
                            val constructor = conditionConstructors(operator)
                            val (value, rest) = parseValue(inputRest, insideParenthesis)
                            (EqSearchCondition(fieldsChain, value), rest)
                        case `InConditionSign` =>
                            parseInterval(inputRest, insideParenthesis) match
                                case Right((value, rest)) =>
                                    (BetweenSearchCondition(fieldsChain, value), rest)
                                case Left(_) =>
                                    val (value, rest) = parseValues(inputRest, insideParenthesis)
                                    (InSearchCondition(fieldsChain, value), rest)
                    Right(searchCondition)
            )
            .getOrElse(Left(SearchConditionParseError(s"Search condition not found! Invalid input: $input!")))

    
