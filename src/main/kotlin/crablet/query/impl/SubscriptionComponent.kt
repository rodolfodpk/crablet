package crablet.query.impl

import crablet.query.EventSink
import crablet.query.SubscriptionConfig
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple
import org.slf4j.LoggerFactory

class SubscriptionComponent(
    private val pool: Pool,
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

        return pool
            .withTransaction { tx: SqlConnection ->
                tx
                    .preparedQuery(SQL_EVENTS_QUERY)
                    .execute(Tuple.of(subscriptionConfig.source.name))
                    .map { rowSet ->
                        rowSet.map { row -> row.toJson() }
                    }.flatMap { jsonList: List<JsonObject> ->
                        when (val eventSync = subscriptionConfig.eventSink) {
                            is EventSink.SingleEventSink -> {
                                jsonList
                                    .fold(successFuture) { future, eventJson ->
                                        future.compose {
                                            eventSync.handle(eventJson)
                                        }
                                    }
                            }

                            is EventSink.BatchEventSink -> eventSync.handle(jsonList)
                            is EventSink.PostgresSingleEventSink -> {
                                jsonList
                                    .fold(successFuture) { future, eventJson ->
                                        future.compose {
                                            eventSync.handle(tx, eventJson)
                                        }
                                    }.onSuccess {
                                        logger.info(
                                            "View {} projected {} events.",
                                            subscriptionConfig.source.name,
                                            jsonList.size,
                                        )
                                    }
                            }

                            is EventSink.PostgresBatchEventSink ->
                                eventSync.handle(tx, jsonList).onSuccess {
                                    logger.info(
                                        "View {} projected {} events in batch.",
                                        subscriptionConfig.source.name,
                                        jsonList.size,
                                    )
                                }
                        }.map { jsonList }
                    }.compose { jsonList: List<JsonObject> ->
                        if (jsonList.isNotEmpty()) {
                            jsonList
                                .last()
                                .let { updateOffset(tx, it.getLong("sequence_id")) }
                                .map { Pair(it, jsonList) }
                                .onSuccess {
                                    logger.info(
                                        "View {} found {} events. Offset is now {}",
                                        subscriptionConfig.source.name,
                                        jsonList.size,
                                        it.first,
                                    )
                                }
                        } else {
                            logger.info("View {} found zero events", subscriptionConfig.source.name)
                            Future.succeededFuture(Pair(0L, emptyList()))
                        }
                    }.map {
                        if (subscriptionConfig.callback != null) {
                            subscriptionConfig.callback.invoke(subscriptionConfig.source.name, it.second)
                        }
                        Pair(it.first, it.second.size)
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
