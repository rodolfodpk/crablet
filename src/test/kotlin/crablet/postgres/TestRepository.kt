package crablet.postgres

import io.vertx.core.Future
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row

class TestRepository(
    private val client: Pool,
) {
    suspend fun getSequences(): List<Triple<Long, Long, Long>>? =
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
            }.coAwait()
}
