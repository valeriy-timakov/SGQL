package my.valerii_timakov.sgql.services


import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.types.EntityType
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId, EntityValue, ValueTypes}
import my.valerii_timakov.sgql.entity.read_modiriers.{GetFieldsDescriptor, SearchCondition}
import my.valerii_timakov.sgql.exceptions.PersistenceRepositoryTypeNotFoundException
import my.valerii_timakov.sgql.services.repositories.postres.{PostgresCrudRepository, Version}

import scala.util.Try

object CrudRepositoriesFactory:
    def createRopository(conf: Config, typeNameMaxLength: Short, fieldMaxLength: Short): CrudRepository =
        if conf.hasPath("postgres") then
            new PostgresCrudRepository(conf.getConfig("postgres"), conf, typeNameMaxLength, fieldMaxLength)
        else
            throw new PersistenceRepositoryTypeNotFoundException


trait CrudRepository:
    def create(entityType: EntityType[_, _, _], data: ValueTypes): Try[Entity[_, _, _]]

    def update(entityType: EntityType[_, _, _], entity: Entity[_, _, _]): Try[Option[Unit]]

    def delete(entityType: EntityType[_, _, _], id: EntityId[_, _]): Try[Option[Unit]]

    def get(entityType: EntityType[_, _, _], id: EntityId[_, _], getFields: GetFieldsDescriptor): Try[Option[Entity[_, _, _]]]

    def find(entityType: EntityType[_, _, _], query: SearchCondition, getFields: GetFieldsDescriptor): Try[Seq[Entity[_, _, _]]]

    def init(typesDefinitionsProvider: TypesDefinitionProviderInitializer): Version

    def setTypesDefinitionsProvider(typesDefinitionsProvider: TypesDefinitionProvider): Unit



