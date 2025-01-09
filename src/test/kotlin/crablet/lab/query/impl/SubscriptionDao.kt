package crablet.lab.query.impl

import crablet.lab.query.SubscriptionSource
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class SubscriptionDao(
    private val subscriptionSource: SubscriptionSource,
) {
    suspend fun getOffsets(sqlConnection: SqlConnection): SubscriptionOffsets =
        sqlConnection
            .preparedQuery(SQL_SELECT_OFFSETS)
            .execute(Tuple.of(subscriptionSource.name))
            .map {
                val row = it.first()
                SubscriptionOffsets(
                    subscriptionOffset = row.getLong("subscription_offset"),
                    globalOffset = row.getLong("global_offset") ?: 0,
                )
            }.coAwait()

    suspend fun updateOffset(sqlConnection: SqlConnection) {
        sqlConnection
            .preparedQuery(SQL_UPDATE_OFFSET)
            .execute(Tuple.of(subscriptionSource.name, subscriptionSource.maxNumberOfRowsToPull))
            .coAwait()
    }

    fun scanPendingEvents(sqlConnection: SqlConnection): List<JsonObject> {
        TODO()
    }

    companion object {
        private const val SQL_LOCK =
            """ SELECT pg_try_advisory_xact_lock($1, $2) as locked
      """
        private const val SQL_SELECT_OFFSETS = """
      WITH subscription_offset AS (
        SELECT sequence_id as subscription_offset FROM subscriptions WHERE name = $1
      ), global_offset AS (
        SELECT max(sequence_id) AS global_offset FROM events
      )
      SELECT subscription_offset.subscription_offset, global_offset.global_offset
        FROM subscription_offset, global_offset
    """
        private const val SQL_UPDATE_OFFSET = "UPDATE subscriptions SET sequence_id = $2 where name = $1"

        data class SubscriptionOffsets(
            val subscriptionOffset: Long,
            val globalOffset: Long,
        )
    }
}
