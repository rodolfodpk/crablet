package crablet

import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row

class TestRepository(
    private val client: Pool,
) {
    fun getSequences(): List<Triple<Long, Long, Long>>? =
        client
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
            }.await()

    fun getAllAccountView(): List<JsonObject> =
        client
            .query("select * from accounts_view")
            .execute()
            .map {
                it.map { row: Row ->
                    row.toJson()
                }
            }.await()

    fun getAllSubscriptions(): List<JsonObject> =
        client
            .query("select * from subscriptions")
            .execute()
            .map {
                it.map { row: Row ->
                    row.toJson()
                }
            }.await()
}
