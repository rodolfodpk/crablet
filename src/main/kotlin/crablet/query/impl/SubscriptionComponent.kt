package crablet.query.impl

import crablet.query.SubscriptionConfig
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class SubscriptionComponent(
    private val client: Pool,
) {
    fun handlePendingEvents(subscriptionConfig: SubscriptionConfig): Future<Pair<Long, Int>> {
        fun updateOffset(
            sqlConnection: SqlConnection,
            newSequenceId: Long,
        ): Future<Long> =
            sqlConnection
                .preparedQuery(SQL_UPDATE_OFFSET)
                .execute(Tuple.of(subscriptionConfig.source.name, newSequenceId))
                .map { newSequenceId }

        return client
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
                        if (pairList.isNotEmpty()) {
                            pairList
                                .last()
                                .first
                                .let { updateOffset(tx, it.getLong("sequence_id")) }
                                .map { Pair(it, pairList.size) }
                        } else {
                            Future.succeededFuture(Pair(0L, 0))
                        }
                    }
            }
    }

    companion object {
        private const val SQL_UPDATE_OFFSET = "UPDATE subscriptions SET sequence_id = $2 where name = $1"
        private const val SQL_EVENTS_QUERY = """
                                SELECT *
                                  FROM events
                                 WHERE sequence > (SELECT sequence_id FROM subscriptions WHERE name = $1)"""
    }
}
