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

case class FKLinkPair(
    columnName: String,
    refColumnName: String
)

case class ForeignKeyData(
    keyName: String,
    tableName: String,
    refTableName: String,
    links: Set[FKLinkPair]
)

case class Version (
    id: Long,
    version: String,
    creationDate: LocalDateTime
)

class MetadataUtils:

    def getTableNames(schemaPattern: String): Set[String] =
        DB.readOnly { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(metaData.getTables(null, schemaPattern, "%", Array("TABLE"))) { rs =>
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
        val resMap = mutable.Map[(String, String, String), mutable.Set[FKLinkPair]]()
        readDB(
            metaData => metaData.getImportedKeys(null, null, tableName),
            rs => {
                val fkName = rs.getString("FK_NAME")
                val fkTableName = rs.getString("FKTABLE_NAME")
                val fkColumnName = rs.getString("FKCOLUMN_NAME")
                val pkTableName = rs.getString("PKTABLE_NAME")
                val pkColumnName = rs.getString("PKCOLUMN_NAME")
                val key = (fkName, fkTableName, pkTableName)
                val links = resMap.getOrElseUpdate(key, mutable.Set())
                links += FKLinkPair(fkColumnName, pkColumnName)
            }
        )
        resMap.map { case ((fkName, fkTableName, pkTableName), links) =>
            ForeignKeyData(fkName, fkTableName, pkTableName, links.toSet)
        }.toSet


    private def getMetadata[R, CC](
        query: DatabaseMetaData => ResultSet,
        extractor: ResultSet => R,
        builder: mutable.Builder[R, CC]
    ): CC =
        readDB(query, rs => builder += extractor(rs))
        builder.result()

    private def readDB[R](
        query: DatabaseMetaData => ResultSet,
        itemProcessor: ResultSet => Unit
    ): Unit =
        DB.readOnly { implicit session =>
            val conn = session.connection
            val metaData = conn.getMetaData

            Using.resource(query(metaData)) { rs =>
                while (rs.next())
                    itemProcessor(rs)
            }
        }


class PostgresDBInitUtils(persistenceConf: Config, typeNameMaxLength: Int):
    private final val ID_COLUMN_NAME = "id-column"
    private final val RENAMING_TABLES_TABLE_NAME = "renaming-tables-table-name"
    private final val RENAMING_COLUMNS_TABLE_NAME = "renaming-columns-table-name"
    private final val PRIMARY_KEY_ALTERING_TABLE_NAME = "primary-key-altering-table-name"
    private final val VERSIONS_TABLE_NAME = "versions-table-name"
    private final val TYPES_TO_TABLES_MAP_TABLE_NAME = "types-to-tables-map-table-name"
    private final val ALTERING_TABLE_COLUMN = "altering-table-column"
    private final val PREVIOUS_NAME_COLUMN = "previous-name-column"
    private final val NEW_NAME_COLUMN = "new-name-column"
    private final val VERSION_REF_COLUMN = "version-ref-column"
    private final val VERSION_NAME_COLUMN = "version-column"
    private final val CREATION_DATE_COLUMN = "creation-date-column"
    private final val TYPE_NAME_COLUMN = "type-name-column"
    private final val TABLE_NAME_COLUMN = "table-name-column"
    private final val TABLE_COLUNS_TABLE_NAME = "table-columns-table-name"
    private final val COLUMN_NAME_COLUMN = "column-name-column"
    private final val COLUMN_TYPE_COLUMN = "column-type-column"
    private final val TABLE_REF_COLUMN = "table-ref-column"

    private val nameLength = persistenceConf.getInt("db-name-length")
    private val dbTypeNameMaxLength = persistenceConf.getInt("db-type-name-max-length")
    private val versionNameMaxLength = persistenceConf.getInt("version-name-max-length")
    private val utilsConf = persistenceConf.getConfig("utils")
    private val utilsSchemaName = utilsConf.getString("schema")
    private val trc = utilsConf.getConfig("versioning")
    private val idColumnName = trc.getString(ID_COLUMN_NAME)
    def init(): Unit =
        DB.autoCommit { implicit session =>
            SQL(
                s"""CREATE SCHEMA IF NOT EXISTS $utilsSchemaName"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)} (
                    $idColumnName BIGSERIAL PRIMARY KEY,
                    ${trc.getString(VERSION_NAME_COLUMN)} VARCHAR($versionNameMaxLength) NOT NULL,
                    ${trc.getString(CREATION_DATE_COLUMN)} TIMESTAMP NOT NULL
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString(RENAMING_TABLES_TABLE_NAME)} (
                    $idColumnName BIGSERIAL PRIMARY KEY,
                    ${trc.getString(PREVIOUS_NAME_COLUMN)} VARCHAR($nameLength),
                    ${trc.getString(NEW_NAME_COLUMN)} VARCHAR($nameLength) NOT NULL,
                    ${trc.getString(VERSION_REF_COLUMN)} BIGINT NOT NULL,
                    CONSTRAINT ${trc.getString(RENAMING_TABLES_TABLE_NAME)}_${trc.getString(VERSIONS_TABLE_NAME)}_fk FOREIGN KEY
                        (${trc.getString(VERSION_REF_COLUMN)}) REFERENCES $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)}($idColumnName)
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString(RENAMING_COLUMNS_TABLE_NAME)} (
                    $idColumnName BIGSERIAL PRIMARY KEY,
                    ${trc.getString(ALTERING_TABLE_COLUMN)} VARCHAR($nameLength) NOT NULL,
                    ${trc.getString(PREVIOUS_NAME_COLUMN)} VARCHAR($nameLength),
                    ${trc.getString(NEW_NAME_COLUMN)} VARCHAR($nameLength) NOT NULL,
                    ${trc.getString(VERSION_REF_COLUMN)} BIGINT NOT NULL,
                    CONSTRAINT ${trc.getString(RENAMING_COLUMNS_TABLE_NAME)}_${trc.getString(VERSIONS_TABLE_NAME)}_fk FOREIGN KEY
                        (${trc.getString(VERSION_REF_COLUMN)}) REFERENCES $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)}($idColumnName)
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString(PRIMARY_KEY_ALTERING_TABLE_NAME)} (
                    $idColumnName BIGSERIAL PRIMARY KEY,
                    ${trc.getString(ALTERING_TABLE_COLUMN)} VARCHAR($nameLength) NOT NULL,
                    ${trc.getString(PREVIOUS_NAME_COLUMN)} VARCHAR($nameLength),
                    ${trc.getString(NEW_NAME_COLUMN)} VARCHAR($nameLength),
                    ${trc.getString(VERSION_REF_COLUMN)} BIGINT NOT NULL,
                    CONSTRAINT ${trc.getString(PRIMARY_KEY_ALTERING_TABLE_NAME)}_${trc.getString(VERSIONS_TABLE_NAME)}_fk FOREIGN KEY
                        (${trc.getString(VERSION_REF_COLUMN)}) REFERENCES $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)}($idColumnName)
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)} (
                    $idColumnName BIGSERIAL PRIMARY KEY,
                    ${trc.getString(TYPE_NAME_COLUMN)} VARCHAR($typeNameMaxLength) NOT NULL,
                    ${trc.getString(TABLE_NAME_COLUMN)} VARCHAR($nameLength) NOT NULL,
                    ${trc.getString(VERSION_REF_COLUMN)} BIGINT NOT NULL,
                    CONSTRAINT ${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)}_${trc.getString(VERSIONS_TABLE_NAME)}_fk FOREIGN KEY
                        (${trc.getString(VERSION_REF_COLUMN)}) REFERENCES $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)}($idColumnName)
                )"""
            ).execute.apply()

            SQL(
                s"""CREATE TABLE IF NOT EXISTS $utilsSchemaName.${trc.getString(TABLE_COLUNS_TABLE_NAME)} (
                    ${trc.getString(TABLE_REF_COLUMN)} BIGINT NOT NULL,
                    ${trc.getString(COLUMN_NAME_COLUMN)} VARCHAR($nameLength) NOT NULL,
                    ${trc.getString(COLUMN_TYPE_COLUMN)} VARCHAR($dbTypeNameMaxLength) NOT NULL,
                    CONSTRAINT ${trc.getString(TABLE_COLUNS_TABLE_NAME)}_pk PRIMARY KEY
                        (${trc.getString(TABLE_REF_COLUMN)}, ${trc.getString(COLUMN_NAME_COLUMN)}),
                    CONSTRAINT ${trc.getString(TABLE_COLUNS_TABLE_NAME)}_${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)}_fk FOREIGN KEY
                        (${trc.getString(TABLE_REF_COLUMN)}) REFERENCES $utilsSchemaName.${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)}($idColumnName)
                )"""
            ).execute.apply()

        }

    def addTableRenamingData(
        prevName: String,
        newName: String,
        versionId: Long,
    ): Unit =
        DB.autoCommit { implicit session =>
            SQL(s"""
                INSERT INTO $utilsSchemaName.${trc.getString(RENAMING_TABLES_TABLE_NAME)} (
                    ${trc.getString(PREVIOUS_NAME_COLUMN)},
                    ${trc.getString(NEW_NAME_COLUMN)},
                    ${trc.getString(VERSION_REF_COLUMN)}
                ) VALUES (?, ?, ?)
            """)
                .bind(prevName, newName, versionId)
                .update.apply()
        }

    def addTableRenamingData(alteringTableName: String, prevColumnName: String, newColumnName: String, versionId: Long): Unit =
        addItemChangeData(alteringTableName, prevColumnName, newColumnName, versionId)

    def addPrimaryKeyAlteringData(alteringTableName: String, prevColumnName: String, newColumnName: String, versionId: Long): Unit =
        addItemChangeData(alteringTableName, prevColumnName, newColumnName, versionId)

    def addVersion(version: String): Version =
        val creationDate = LocalDateTime.now()
        val id = DB.autoCommit { implicit session =>
            SQL(
                s"""INSERT INTO $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)} (
                    ${trc.getString(VERSION_NAME_COLUMN)},
                    ${trc.getString(CREATION_DATE_COLUMN)}
                ) VALUES (?, ?)"""
            )
            .bind(version, creationDate)
            .updateAndReturnGeneratedKey(trc.getString(ID_COLUMN_NAME)).apply()
        }
        Version(id, version, creationDate)
        
    def addTypeToTableMapEntry(typeName: String, tableName: String, versionId: Long): Unit =
        DB.autoCommit { implicit session =>
            SQL(
                s"""INSERT INTO $utilsSchemaName.${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)} (
                    ${trc.getString(TYPE_NAME_COLUMN)},
                    ${trc.getString(TABLE_NAME_COLUMN)},
                    ${trc.getString(VERSION_REF_COLUMN)}
                ) VALUES (?, ?, ?)"""
            )
            .bind(typeName, tableName, versionId)
            .update.apply()
        }

    def getTypesToTablesMap(versionId: Long): Map[String, String] =
        DB.readOnly { implicit session =>
            SQL(
                s"""SELECT * FROM $utilsSchemaName.${trc.getString(TYPES_TO_TABLES_MAP_TABLE_NAME)} WHERE ${trc.getString(VERSION_REF_COLUMN)} = ?"""
            )
            .bind(versionId)
            .map(rs => (rs.string(trc.getString(TYPE_NAME_COLUMN)), rs.string(trc.getString(TABLE_NAME_COLUMN))))
            .list.apply()
            .toMap
        }

    def getLatestVersion: Option[Version] =
        DB.readOnly { implicit session =>
            SQL(
                s"""SELECT * FROM $utilsSchemaName.${trc.getString(VERSIONS_TABLE_NAME)} ORDER BY $idColumnName DESC LIMIT 1"""
            )
            .map(rs => Version(rs.long(trc.getString(ID_COLUMN_NAME)), rs.string(trc.getString(VERSION_NAME_COLUMN)), rs.localDateTime(trc.getString(CREATION_DATE_COLUMN))))
            .single.apply()
        }

    private def addItemChangeData(
        alteringTableName: String,
        prevValue: String,
        newValue: String,
        versionId: Long,
    ): Unit =
        DB.autoCommit { implicit session =>
            SQL(
                s"""INSERT INTO $utilsSchemaName.${trc.getString(RENAMING_COLUMNS_TABLE_NAME)} (
                        ${trc.getString(ALTERING_TABLE_COLUMN)},
                        ${trc.getString(PREVIOUS_NAME_COLUMN)},
                        ${trc.getString(NEW_NAME_COLUMN)},
                        ${trc.getString(VERSION_REF_COLUMN)}
                    ) VALUES (?, ?, ?, ?)"""
            )
                .bind(alteringTableName, prevValue, newValue, versionId)
                .update.apply()
        }
        
        
        





