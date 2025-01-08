package crablet.lab.query

import crablet.EventName
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlConnection

data class SubscriptionConfig(
    val name: String,
    val initialInterval: Long = DEFAULT_INITIAL_INTERVAL,
    val interval: Long = DEFAULT_INTERVAL,
    val maxNumberOfRows: Int = DEFAULT_NUMBER_ROWS,
    val maxInterval: Long = DEFAULT_MAX_INTERVAL,
    val metricsInterval: Long = DEFAULT_MAX_INTERVAL,
    val jitterFunction: () -> Int = { ((0..10).random() * 1000) },
) {
    companion object {
        private val DEFAULT_INITIAL_INTERVAL = ((1..10).random() * 1000L)
        private const val DEFAULT_INTERVAL = 5_000L
        private const val DEFAULT_NUMBER_ROWS = 250
        private const val DEFAULT_MAX_INTERVAL = 60_000L
    }
}

interface SubscriptionsRegistry {
    suspend fun addSubscription(
        config: SubscriptionConfig,
        eventTypes: List<EventName>,
        eventEffectFunction: (sqlConnection: SqlConnection, eventAsJson: JsonObject) -> JsonObject,
        viewEffectFunction: ((viewAsJson: JsonObject) -> Void)? = null,
    )
}
