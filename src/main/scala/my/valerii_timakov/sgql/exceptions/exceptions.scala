package my.valerii_timakov.sgql.exceptions

import my.valerii_timakov.sgql.entity.TypesDefinitionsParseError

class TypesLoadExceptionException(error: TypesDefinitionsParseError) extends Exception(error.message)
class TypeReinitializationException extends Exception("Type reinitialization!")
class NotAbstractParentTypeException(typeName: String) extends Exception(s"Type $typeName is not abstract! Could not be used as parent type.")
class NotPrimitiveAbstractTypeException(typeName: String) extends Exception(s"Type $typeName is not primitive!")
class NotArrayAbstractTypeException(typeName: String) extends Exception(s"Type $typeName is not primitive!")
class NotObjectAbstractTypeException(typeName: String) extends Exception(s"Type $typeName is not primitive!")
class NoTypeFound(typeName: String) extends Exception(s"No type found for name $typeName!")
class NoIdTypeFound(typeName: String) extends Exception(s"No ID type found for name $typeName!")
class WrongTypeNameException(name: String) extends Exception(s"Wrong type name '$name'!")
class AbstractTypeException(typeName: String, reason: String) extends Exception(s"Type $typeName is abstract! " + reason):
    def this(typeName: String) = this(typeName, "")