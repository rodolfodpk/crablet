package crablet

import crablet.command.DomainIdentifier
import crablet.command.StateId
import crablet.command.StateName
import crablet.command.TransactionContext
import crablet.query.SubscriptionSource

interface TestAccountsContext {
    val eventTypes: List<EventName>
        get() = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }

    val transactionContextAcct1: TransactionContext
        get() =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
                eventTypes = eventTypes,
            )

    val transactionContextAcct2: TransactionContext
        get() =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
                eventTypes = eventTypes,
            )

    val transactionContextAcct3: TransactionContext
        get() =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("3"))),
                eventTypes = eventTypes,
            )

    val source: SubscriptionSource
        get() = SubscriptionSource(name = "accounts-view", eventTypes = eventTypes)
}
