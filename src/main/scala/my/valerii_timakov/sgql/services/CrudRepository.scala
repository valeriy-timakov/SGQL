package my.valerii_timakov.sgql.services


import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.domain.type_definitions.EntityType
import my.valerii_timakov.sgql.entity.domain.type_values.{Entity, EntityId, EntityValue, ValueTypes}
import my.valerii_timakov.sgql.entity.read_modiriers.{GetFieldsDescriptor, SearchCondition}
import my.valerii_timakov.sgql.exceptions.PersistenceRepositoryTypeNotFoundException
import my.valerii_timakov.sgql.services.repositories.postres.PostgresCrudRepository

import scala.util.Try

object CrudRepositoriesFactory:
    def createRopository(conf: Config, typeNameMaxLength: Short, fieldMaxLength: Short): CrudRepository =
        if conf.hasPath("postgres") then
            new PostgresCrudRepository(conf.getConfig("postgres"), conf, typeNameMaxLength, fieldMaxLength)
        else
            throw new PersistenceRepositoryTypeNotFoundException


trait CrudRepository:
    def create(entityType: EntityType[?, ?], data: ValueTypes): Try[Entity[?, ?]]

    def update(entityType: EntityType[?, ?], entity: Entity[?, ?]): Try[Option[Unit]]

    def delete(entityType: EntityType[?, ?], id: EntityId[?, ?]): Try[Option[Unit]]

    def get(entityType: EntityType[?, ?], id: EntityId[?, ?], getFields: GetFieldsDescriptor): Try[Option[Entity[?, ?]]]

    def find(entityType: EntityType[?, ?], query: SearchCondition, getFields: GetFieldsDescriptor): Try[Seq[Entity[?, ?]]]

    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit



