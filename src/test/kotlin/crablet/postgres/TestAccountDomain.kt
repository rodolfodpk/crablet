package crablet.postgres

import io.vertx.core.json.JsonObject

object TestAccountDomain {
    data class Account(
        val id: Int? = null,
        val balance: Int = 0,
    )

    val initialStateFunction = { Account() }

    val evolveFunction: (Account, JsonObject) -> Account = { state, event ->
        when (event.getString("type")) {
            "AccountOpened" -> state.copy(id = event.getInteger("id"))
            "AmountDeposited" -> state.copy(balance = state.balance.plus(event.getInteger("amount")))
            "AmountTransferred" -> {
                when {
                    event.getInteger("fromAcct") == state.id ->
                        state.copy(
                            balance = state.balance.minus(event.getInteger("amount")),
                        )

                    event.getInteger("toAcct") == state.id ->
                        state.copy(
                            balance = state.balance.plus(event.getInteger("amount")),
                        )

                    else -> state
                }
            }

            else -> state
        }
    }
}
