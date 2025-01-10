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
                    .map { rowSet ->
                        println("Found ${rowSet.size()} events")
                        rowSet.map { row -> row.toJson() }
                    }.flatMap { jsonList: List<JsonObject> ->
                        println("----- jsonlist " + jsonList)
                        jsonList
                            .fold(Future.succeededFuture<Void>()) { future, eventJson ->
                                future.compose { subscriptionConfig.eventViewProjector.project(tx, eventJson) }
                            }.map { jsonList }
                    }.compose { jsonList: List<JsonObject> ->
                        println("----- jsonlist " + jsonList)
                        if (jsonList.isNotEmpty()) {
                            jsonList
                                .last()
                                .let { updateOffset(tx, it.getLong("sequence_id")) }
                                .map { Pair(it, jsonList.size) }
                        } else {
                            Future.succeededFuture(Pair(0L, 0))
                        }
                    }.map {
                        if (subscriptionConfig.callback != null) {
                            subscriptionConfig.callback.invoke()
                        }
                        it
                    }
            }
    }

    companion object {
        private const val SQL_UPDATE_OFFSET = "UPDATE subscriptions SET sequence_id = $2 where name = $1"
        private const val SQL_EVENTS_QUERY = """
                                SELECT *
                                  FROM events
                                 WHERE sequence_id > (SELECT sequence_id FROM subscriptions WHERE name = $1)"""
    }
}
