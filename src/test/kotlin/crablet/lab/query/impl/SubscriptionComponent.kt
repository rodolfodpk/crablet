package crablet.lab.query.impl

import crablet.lab.query.SubscriptionConfig
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class SubscriptionComponent(
    private val client: Pool,
) {
    suspend fun handlePendingEvents(subscriptionConfig: SubscriptionConfig) {
        fun updateOffset(
            sqlConnection: SqlConnection,
            newSequenceId: Long,
        ): Future<Void> =
            sqlConnection
                .preparedQuery(SQL_UPDATE_OFFSET)
                .execute(Tuple.of(subscriptionConfig.source.name, newSequenceId))
                .mapEmpty()

        client
            .withTransaction { tx ->
                tx
                    .preparedQuery(SQL_EVENTS_QUERY)
                    .execute(Tuple.of(subscriptionConfig.source.name))
                    .map { rowSet -> rowSet.map { row -> row.toJson() } }
                    .map { jsonList: List<JsonObject> ->
                        jsonList.map { eventJson ->
                            Pair(eventJson, subscriptionConfig.eventEffectFunction.invoke(tx, eventJson))
                        }
                    }.compose { pairList: List<Pair<JsonObject, JsonObject>> ->
                        pairList.last().first.let { updateOffset(tx, it.getLong("sequence_id")) }
                    }
            }.coAwait()
    }

    companion object {
        private const val SQL_UPDATE_OFFSET = "UPDATE subscriptions SET sequence_id = $2 where name = $1"
        private const val SQL_EVENTS_QUERY = """
                                SELECT *
                                  FROM events
                                 WHERE sequence > (SELECT sequence_id FROM subscriptions WHERE name = $1)"""
    }
}
