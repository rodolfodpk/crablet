package crablet

import crablet.query.SubscriptionSource

interface TestAccountsSubscriptionContext : TestAccountsContext {
    val source: SubscriptionSource
        get() = SubscriptionSource(name = "accounts-view", eventTypes = eventTypes)
}
