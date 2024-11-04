package my.valerii_timakov.sgql.services.repositories.postres

import com.typesafe.config.Config
import my.valerii_timakov.sgql.entity.*
import my.valerii_timakov.sgql.exceptions.{DbTableMigrationException, PersistenceRepositoryTypeNotFoundException}
import my.valerii_timakov.sgql.services.*
import java.sql.{DatabaseMetaData, ResultSet}
import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.immutable.Set
import scala.collection.mutable.Builder
import scala.util.{Success, Try, Using}
import scalikejdbc._


case class ColumnData(
    name: String,
    columnType: String,
    isPrimaryKey: Boolean,
    isNullable: Boolean,
    defaultValue: Option[String]
)

case class PrimaryKeyData(
    keyName: String,
    tableName: String,
    columnNames: Set[String]
)

case class ForeignKeyData(
    keyName: String,
    tableName: String,
    columnName: String,
    refTableName: String,
    refColumnName: String
)

class MetadataUtils:

    def getTableNames: Set[String] =
        DB.readOnly { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(metaData.getTables(null, null, "%", Array("TABLE"))) { rs =>
                var tables = Set[String]()

                while (rs.next()) 
                    tables += rs.getString("TABLE_NAME")
                

                tables
            }
        }

    def getTableColumnsDataMap(tableName: String): Map[String, ColumnData] =
        val primaryKeyColumns = getTablePrimaryKeys(tableName).map(_.columnNames).getOrElse(Set())
        DB.autoCommit { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(metaData.getColumns(null, null, tableName, "%")) { rs =>
                val columns = Map.newBuilder[String, ColumnData]

                while (rs.next()) {
                    val columnName = rs.getString("COLUMN_NAME")
                    val columnType = rs.getString("TYPE_NAME")
                    val isNullable = rs.getString("IS_NULLABLE") == "YES"
                    val defaultValue = Option(rs.getString("COLUMN_DEF"))
                    columns += columnName -> ColumnData(columnName, columnType, primaryKeyColumns.contains(columnName),
                        isNullable, defaultValue)
                }

                columns.result()
            }
        }

    def getTablePrimaryKeys(tableName: String): Option[PrimaryKeyData] =
        val res = getMetadata(
            metaData => metaData.getPrimaryKeys(null, null, tableName),
            rs =>(rs.getString("PK_NAME"), rs.getString("COLUMN_NAME")),
            Set.newBuilder[(String, String)])
        if res.nonEmpty
            then Some(PrimaryKeyData(res.head._1, tableName, res.map(_._2)))
            else None

    def getForeignKeys(tableName: String): Set[ForeignKeyData] =
        getMetadata(
            metaData => metaData.getImportedKeys(null, null, tableName),
            rs => ForeignKeyData(rs.getString("FK_NAME"), rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
                rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")),
            Set.newBuilder[ForeignKeyData])

    private def getMetadata[R, CC](
        query: DatabaseMetaData => ResultSet,
        extractor: ResultSet => R,
        builder: mutable.Builder[R, CC]
    ): CC =
        DB.readOnly { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(query(metaData)) { rs =>
                builder += extractor(rs)
            }

            builder.result()
        }


class PostgresDBInitUtils(conf: Config):
    def init(): Unit =
        val utilsSchemaName = conf.getString("utils-schema")
        val trc = conf.getConfig("tables-altering")
        DB.autoCommit { implicit session =>
            SQL(
                s"""CREATE SCHEMA IF NOT EXISTS $utilsSchemaName"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString("renaming-columns-table-name")} (
                    ${trc.getString("id-column")} SERIAL PRIMARY KEY,
                    ${trc.getString("altering-table-column")} ${trc.getString("db-names-column-type")} NOT NULL,
                    ${trc.getString("previous-name-column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("new-name-column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("operation-date-column")} ${trc.getString("operation-date-column-type")} NOT NULL,
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString("primary-key-altering-table-name")} (
                    ${trc.getString("id-column")} SERIAL PRIMARY KEY,
                    ${trc.getString("altering-table-column")} ${trc.getString("db-names-column-type")} NOT NULL,
                    ${trc.getString("previous_primary_key_column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("new_primary_key_column")} ${trc.getString("db-names-column-type")},
                    ${trc.getString("operation-date-column")} ${trc.getString("operation-date-column-type")} NOT NULL,
                )"""
            ).execute.apply()
        }

    def addTableRenamingData(alteringTableName: String, prevColumnName: String, newColumnName: String): Unit =
        addItemChangeData(alteringTableName, prevColumnName, newColumnName, "previous-name-column", "new-name-column")

    def addPrimaryKeyAlteringData(alteringTableName: String, prevColumnName: String, newColumnName: String): Unit =
        addItemChangeData(alteringTableName, prevColumnName, newColumnName, "previous_primary_key_column", "new_primary_key_column")

    private def addItemChangeData(
        alteringTableName: String,
        prevValue: String,
        newValue: String,
        prevValueColConfigName: String,
        newValueColConfigName: String
    ): Unit =
        val utilsSchemaName = conf.getString("utils-schema")
        val trc = conf.getConfig("tables-altering")
        DB.autoCommit { implicit session =>
            SQL(
                s"""INSERT INTO $utilsSchemaName.${trc.getString("renaming-columns-table-name")} (
                        ${trc.getString("altering-table-column")},
                        ${trc.getString(prevValueColConfigName)},
                        ${trc.getString(newValueColConfigName)},
                        ${trc.getString("operation-date-column")}
                    ) VALUES ($$1, $$2, $$3, $$4)"""
            )
                .bind(alteringTableName, prevValue, newValue, LocalDateTime.now())
                .update.apply()
        }



