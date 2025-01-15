package crablet

import crablet.command.DomainIdentifier
import crablet.command.StateId
import crablet.command.StateName
import crablet.command.TransactionContext

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
}
