package crablet.query

import crablet.EventName
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlConnection

data class SubscriptionSource(
    val name: String,
    val eventTypes: List<EventName>,
    val maxNumberOfRowsToPull: Int = 250,
)

sealed interface EventSink {
    interface DefaultEventSink : EventSink {
        fun handle(eventAsJson: JsonObject): Future<Void>
    }

    interface PostgresEventSync : EventSink {
        fun handle(
            sqlConnection: SqlConnection,
            eventAsJson: JsonObject,
        ): Future<Void>
    }
}

class SubscriptionConfig(
    val source: SubscriptionSource,
    val eventSink: EventSink,
    val callback: ((name: String, List<JsonObject>) -> Unit)? = null,
)

data class IntervalConfig(
    val initialInterval: Long = DEFAULT_INITIAL_INTERVAL,
    val interval: Long = DEFAULT_INTERVAL,
    val maxInterval: Long = DEFAULT_MAX_INTERVAL,
    val metricsInterval: Long = DEFAULT_MAX_INTERVAL,
    val jitterFunction: () -> Int = { ((0..10).random() * 1000) },
) {
    companion object {
        private val DEFAULT_INITIAL_INTERVAL = ((1..10).random() * 1000L)
        private const val DEFAULT_INTERVAL = 5_000L
        private const val DEFAULT_MAX_INTERVAL = 60_000L
    }
}

interface SubscriptionsContainer {
    suspend fun addSubscription(
        subscriptionConfig: SubscriptionConfig,
        intervalConfig: IntervalConfig = IntervalConfig(),
    )

    suspend fun deployAll()
}
