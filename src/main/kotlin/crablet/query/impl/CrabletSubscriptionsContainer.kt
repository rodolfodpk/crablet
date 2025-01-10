package crablet.query.impl

import crablet.query.IntervalConfig
import crablet.query.SubscriptionConfig
import crablet.query.SubscriptionsContainer
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.sqlclient.Pool

class CrabletSubscriptionsContainer(
    private val vertx: Vertx,
    private val pool: Pool,
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
        val subscriptionComponent = SubscriptionComponent(pool)
        val deploymentOptions = DeploymentOptions().setInstances(1) // very important!
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
                Pair(subscriptionName, vertx.deployVerticle(verticle, deploymentOptions).await())
            }.map { (subscriptionName, deployId) -> deployIds.put(subscriptionName, deployId) }
    }
}
