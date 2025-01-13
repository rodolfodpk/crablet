package crablet.query.impl

import crablet.query.EventSink
import crablet.query.SubscriptionConfig
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple
import org.slf4j.LoggerFactory

internal class SubscriptionComponent(
    private val vertx: Vertx,
    private val pool: Pool,
) {
    fun handlePendingEvents(subscriptionConfig: SubscriptionConfig): Future<Pair<Long, Int>> {
        fun handleEventSink(
            jsonList: List<JsonObject>,
            tx: SqlConnection,
        ): Future<List<JsonObject>> =
            when (val eventSync = subscriptionConfig.eventSink) {
                is EventSink.SingleEventSink ->
                    jsonList
                        .fold(successFuture) { future, eventJson ->
                            future.compose {
                                eventSync.handle(eventJson)
                            }
                        }
                is EventSink.BatchEventSink -> eventSync.handle(jsonList)
                is EventSink.PostgresSingleEventSink ->
                    jsonList
                        .fold(successFuture) { future, eventJson ->
                            future.compose {
                                eventSync.handle(tx, eventJson)
                            }
                        }
                is EventSink.PostgresBatchEventSink ->
                    eventSync.handle(tx, jsonList)
            }.map { jsonList }

        fun updateSequenceId(
            jsonList: List<JsonObject>,
            tx: SqlConnection,
        ): Future<Long> =
            if (jsonList.isNotEmpty()) {
                val last: JsonObject = jsonList.last()
                val newSequenceId: Long = last.getLong("sequence_id")
                logger.info(
                    "Subscription {} found {} events. Will update sequenceId to {}",
                    subscriptionConfig.source.name,
                    jsonList.size,
                    newSequenceId,
                )
                tx
                    .preparedQuery(SQL_UPDATE_OFFSET)
                    .execute(Tuple.of(subscriptionConfig.source.name, newSequenceId))
                    .map { newSequenceId }
            } else {
                logger.debug("View {} found zero events", subscriptionConfig.source.name)
                Future.succeededFuture(0L)
            }

        fun handleCallback(
            sequenceId: Long,
            jsonList: List<JsonObject>,
        ): Pair<Long, Int> =
            if (subscriptionConfig.callback != null) {
                vertx.executeBlocking {
                    try {
                        logger.info(
                            "Subscription {} found {} events. Will invoke callback function",
                            subscriptionConfig.source.name,
                            jsonList.size,
                        )
                        subscriptionConfig.callback.invoke(subscriptionConfig.source.name, jsonList)
                    } catch (exception: RuntimeException) {
                        logger.info(
                            "Error on callback for {} with {} events",
                            subscriptionConfig.source.name,
                            jsonList.size,
                        )
                    }
                }
                Pair(sequenceId, jsonList.size)
            } else {
                Pair(sequenceId, jsonList.size)
            }

        return pool
            .withTransaction { tx: SqlConnection ->
                tx
                    .preparedQuery(SQL_EVENTS_QUERY)
                    .execute(Tuple.of(subscriptionConfig.source.name))
                    .map { rowSet ->
                        logger.info("Subscription {} found {} events", subscriptionConfig.source.name, rowSet.size())
                        rowSet.map { row -> row.toJson() }
                    }.compose { jsonList: List<JsonObject> ->
                        handleEventSink(jsonList, tx)
                    }.compose { jsonList: List<JsonObject> ->
                        updateSequenceId(jsonList, tx)
                            .map { Pair(jsonList, it) }
                    }
            }.map { (jsonList, sequenceId) ->
                if (jsonList.isNotEmpty()) {
                    handleCallback(sequenceId, jsonList)
                } else {
                    Pair(0L, 0)
                }
            }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SubscriptionComponent::class.java)
        private val successFuture = Future.succeededFuture<Void>()
        private const val SQL_UPDATE_OFFSET = "UPDATE subscriptions SET sequence_id = $2 where name = $1"
        private const val SQL_EVENTS_QUERY = """
                                SELECT *
                                  FROM events
                                 WHERE sequence_id > (SELECT sequence_id FROM subscriptions WHERE name = $1)"""
    }
}
