package my.valerii_timakov.sgql.entity

sealed trait Error:
    def message: String
sealed class SingleMessageError(val message: String) extends Error

sealed class ParseError(message: String) extends SingleMessageError(message)
final class GetFieldsParseError(subMessage: String) extends ParseError("{Error.parsing.get.fields} " + subMessage)
final class GetFieldsFieldValidateError(subMessage: String) extends ParseError("{Error.validating.get.fields} " + subMessage)
final class GetFieldsFieldsValidateError(errors: Seq[Error]) extends Error:
    def message: String = errors.mkString(", ")
final class SearchConditionParseError(subMessage: String) extends ParseError("{Error.parsing.search.condition} " + subMessage)
final class ValueParseError(typeName: String, value: String, cause: String = "") 
    extends ParseError("{Error.parsing.entity.value}" + typeName + " from " + value + cause)
sealed class ValidationError(message: String) extends SingleMessageError(message)
sealed class NotFountError(message: String) extends SingleMessageError(message)
final class TypeNotFountError(typeName: String) extends NotFountError("{Error.parsing.type.not.found}" + typeName)
final class AbstractTypeError(typeName: String) extends SingleMessageError("{Error.type.abstract}" + typeName)
final class TypesDefinitionsParseError(message: String) extends SingleMessageError("{Error.parsing.type_definition}" + message)
final class TypesConsistencyError(message: String) extends SingleMessageError("{Error.types.consistency}" + message)
