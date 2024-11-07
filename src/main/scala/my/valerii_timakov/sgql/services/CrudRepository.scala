package my.valerii_timakov.sgql.services


import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.PersistenceRepositoryTypeNotFoundException
import my.valerii_timakov.sgql.services.repositories.postres.PostgresCrudRepository

import scala.util.Try

object CrudRepositoriesFactory:
    def createRopository(conf: Config): CrudRepository =
        if conf.hasPath("postgres") then
            new PostgresCrudRepository(conf.getConfig("postgres"), conf)
        else
            throw new PersistenceRepositoryTypeNotFoundException


trait CrudRepository:
    def create(entityType: EntityType, data: EntityFieldType): Try[Entity]

    def update(entityType: EntityType, entity: Entity): Try[Option[Entity]]

    def delete(entityType: EntityType, id: EntityId): Try[Option[EntityId]]

    def get(entityType: EntityType, id: EntityId, getFields: GetFieldsDescriptor): Try[Option[Entity]]

    def find(entityType: EntityType, query: SearchCondition, getFields: GetFieldsDescriptor): Try[Seq[Entity]]

    def init(typesDefinitionsProvider: TypesDefinitionProvider): Unit



