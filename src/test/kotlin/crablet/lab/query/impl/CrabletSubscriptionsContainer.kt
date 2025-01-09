package crablet.lab.query.impl

import crablet.lab.query.IntervalConfig
import crablet.lab.query.SubscriptionConfig
import crablet.lab.query.SubscriptionsContainer
import io.vertx.core.Vertx
import io.vertx.sqlclient.Pool

class CrabletSubscriptionsContainer(
    private val vertx: Vertx,
    private val client: Pool,
) : SubscriptionsContainer {
    private val subscriptions: MutableMap<String, Pair<SubscriptionConfig, IntervalConfig>> = mutableMapOf()
    private val deployIds: MutableMap<String, String> = mutableMapOf()

    override suspend fun addSubscription(
        subscriptionConfig: SubscriptionConfig,
        intervalConfig: IntervalConfig,
    ) {
        subscriptions[subscriptionConfig.source.name] = Pair(subscriptionConfig, intervalConfig)
    }

    override suspend fun deployAll() {
        val subscriptionComponent = SubscriptionComponent(client)
        subscriptions.values
            .map { (subscriptionConfig, intervalConfig) ->
                Pair(
                    subscriptionConfig.source.name,
                    SubscriptionVerticle(
                        subscriptionConfig = subscriptionConfig,
                        intervalConfig = intervalConfig,
                        subscriptionComponent = subscriptionComponent,
                    ),
                )
            }.map { (subscriptionName, verticle) ->
                Pair(subscriptionName, vertx.deployVerticle(verticle).await())
            }.map { (subscriptionName, deployId) -> deployIds.put(subscriptionName, deployId) }
    }
}
