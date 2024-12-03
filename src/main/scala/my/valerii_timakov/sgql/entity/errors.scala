package my.valerii_timakov.sgql.entity

sealed class Error(val message: String)

sealed class ParseError(message: String) extends Error(message)
final class GetFieldsParseError(subMessage: String) extends ParseError("{Error.parsing.get.fields} " + subMessage)
final class SearchConditionParseError(subMessage: String) extends ParseError("{Error.parsing.search.condition} " + subMessage)
final class ValueParseError(typeName: String, value: String, cause: String = "") 
    extends ParseError("{Error.parsing.entity.value}" + typeName + " from " + value + cause)
sealed class ValidationError(message: String) extends Error(message)
sealed class NotFountError(message: String) extends Error(message)
final class TypeNotFountError(typeName: String) extends NotFountError("{Error.parsing.type.not.found}" + typeName)
final class AbstractTypeError(typeName: String) extends Error("{Error.type.abstract}" + typeName)
final class TypesDefinitionsParseError(message: String) extends Error("{Error.parsing.type_definition}" + message)
final class TypesConsistencyError(message: String) extends Error("{Error.types.consistency}" + message)
