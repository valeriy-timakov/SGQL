package my.valerii_timakov.sgql.exceptions

import my.valerii_timakov.sgql.entity.TypesDefinitionsParseError

class TypesLoadExceptionException(error: TypesDefinitionsParseError) extends Exception(error.message):
    def this(message: String) = this(TypesDefinitionsParseError(message))

class ConsistencyException(msg: String) extends RuntimeException(msg)
    
class AbstractTypeException(typeName: String, reason: String) extends Exception(s"Type \"$typeName\" is abstract! " + reason):
    def this(typeName: String) = this(typeName, "")
    
class TypeReinitializationException extends Exception("Type reinitialization!")
class NotAbstractParentTypeException(typeName: String) extends Exception(s"Type \"$typeName\" is not abstract! Could not be used as parent type.")
class NotPrimitiveAbstractTypeException(typeName: String) extends Exception(s"Type \"$typeName\" is not primitive!")
class NotArrayAbstractTypeException(typeName: String) extends Exception(s"Type \"$typeName\" is not primitive!")
class NotObjectAbstractTypeException(typeName: String) extends Exception(s"Type \"$typeName\" is not primitive!")
class NoTypeFound(typeName: String) extends Exception(s"No type found for name \"$typeName\"!")
class NoIdTypeFound(typeName: String) extends Exception(s"No ID type found for name \"$typeName\"!")
class WrongTypeNameException(name: String) extends Exception(s"Wrong type name '$name'!")
class NoIdentityForRootSupetType(typeName: String) extends Exception(s"No identity for root super type! Type: \"$typeName\"")
class IdentitySetForNonRootType(typeName: String) extends Exception(s"Identity set for type with non root parent type! Type: \"$typeName\"")

class DbTableMigrationException(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
object DbTableMigrationException:
    def apply(tableName: String, cause: Throwable) = new DbTableMigrationException(s"Error migrating table \"$tableName\"! " +
        s"Table migration failed with cause ${cause.getMessage}! Use migration tools or try migrate manually.", cause)
    def apply(tableName: String) = new DbTableMigrationException(s"Error migrating table \"$tableName\"! " +
        s"Table could not be migrated automatically - use migration tools or try migrate manually.", null)
class PersistenceRepositoryTypeNotFoundException extends RuntimeException(s"Repository type not found!")