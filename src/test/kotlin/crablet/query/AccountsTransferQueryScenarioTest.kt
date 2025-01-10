package crablet.query

import crablet.command.AccountTransferCommandScenarioTest
import crablet.query.impl.CrabletSubscriptionsContainer
import io.vertx.core.Vertx
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test

class AccountsTransferQueryScenario: AccountTransferCommandScenarioTest() {

    @Test
    @Order(100)
    fun `the view model are correct`() = runTest {
        val container = CrabletSubscriptionsContainer(Vertx.vertx(), pool)
        val source = SubscriptionSource(name = "accounts-view", eventTypes = eventTypes)
//        val subscriptionConfig = SubscriptionConfig(source = source,
//            eventEffectFunction = )
//        container.addSubscription()
    }

}

