package crablet.lab.query.impl

import crablet.lab.query.IntervalConfig
import crablet.lab.query.SubscriptionConfig
import crablet.lab.query.SubscriptionsContainer
import io.vertx.sqlclient.Pool

class CrabletSubscriptionsContainer(
    private val client: Pool,
) : SubscriptionsContainer {
    private val subscriptions: MutableMap<String, SubscriptionConfig> = mutableMapOf()

    override suspend fun addSubscription(
        subscriptionConfig: SubscriptionConfig,
        intervalConfig: IntervalConfig,
    ) {
        subscriptions[subscriptionConfig.source.name] = subscriptionConfig
    }

    override suspend fun start() {
        TODO("Not yet implemented")
    }
}
