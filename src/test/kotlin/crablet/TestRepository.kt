package crablet

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet
import org.slf4j.LoggerFactory

class TestRepository(
    private val pool: Pool,
) {
    suspend fun cleanDatabase() {
        logger.info("Cleaning database -------------")
        pool
            .query("TRUNCATE TABLE events")
            .execute()
            .compose {
                pool.query("ALTER SEQUENCE events_sequence_id_seq RESTART WITH 1").execute()
            }.compose {
                pool.query("UPDATE subscriptions SET sequence_id = 0").execute()
            }.coAwait()
    }

    fun dumpSubscriptions(): Future<RowSet<Row>>? =
        pool
            .query("select * from subscriptions order by name")
            .execute()
            .onSuccess { rs ->
                logger.info("Subscriptions -------------")
                rs.forEach {
                    println(it.toJson())
                }
            }

    fun dumpEvents() {
        pool
            .query("select * from events order by sequence_id")
            .execute()
            .onSuccess { rs ->
                logger.info("Events -------------")
                rs.forEach {
                    println(it.toJson())
                }
            }.compose { dumpSubscriptions() }
            .await()
    }

    fun getSequences(): Future<List<Triple<Long, Long, Long>>> =
        pool
            .query("select sequence_id, causation_id, correlation_id from events")
            .execute()
            .map {
                it.map { row: Row ->
                    Triple(
                        row.getLong("sequence_id"),
                        row.getLong("causation_id"),
                        row.getLong("correlation_id"),
                    )
                }
            }

    fun getAllAccountView(): Future<List<JsonObject>> =
        pool
            .query("select * from accounts_view")
            .execute()
            .map {
                it.map { row: Row ->
                    row.toJson()
                }
            }

    fun getAllSubscriptions(): Future<List<JsonObject>> =
        pool
            .query("select * from subscriptions")
            .execute()
            .map {
                it.map { row: Row ->
                    row.toJson()
                }
            }

    companion object {
        private val logger = LoggerFactory.getLogger(TestRepository::class.java)
    }
}
