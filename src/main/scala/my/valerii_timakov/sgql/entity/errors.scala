package my.valerii_timakov.sgql.entity

sealed class Error(val mnemo: String)

sealed class ParseError(mnemo: String) extends Error(mnemo)
sealed class ValidationError(mnemo: String) extends Error(mnemo)
sealed class NotFountError(mnemo: String) extends Error(mnemo)
final class GetFieldsParseError(sumMnemo: String) extends ParseError("{Error.parsing.get.fields} " + sumMnemo)
final class SearchConditionParseError(subMnemo: String) extends ParseError("{Error.parsing.search.condition} " + subMnemo)
final class IdParseError(typeName: String, value: String) extends ParseError("{Error.parsing.entity.id}" + typeName + " from " + value)
final class TypeNotFountError(typeName: String) extends NotFountError("{Error.parsing.type.not.found}" + typeName)


