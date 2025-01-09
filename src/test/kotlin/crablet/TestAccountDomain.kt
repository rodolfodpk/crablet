package crablet

import io.vertx.core.json.JsonObject

object TestAccountDomain {
    data class Account(
        val id: Int? = null,
        val balance: Int = 0,
    )

    val initialStateFunction = { Account() }

    val evolveFunction: (Account, JsonObject) -> Account = { state, event ->
        println(event.encodePrettily())
        when (event.getString("type")) {
            "AccountOpened" -> state.copy(id = event.getInteger("id"))
            "AmountDeposited" -> state.copy(balance = state.balance.plus(event.getInteger("amount")))
            "AmountTransferred" -> {
                when {
                    event.getInteger("fromAcct") == state.id ->
                        state.copy(
                            balance = event.getInteger("fromAcctBalance"),
                        )

                    event.getInteger("toAcct") == state.id ->
                        state.copy(
                            balance = event.getInteger("toAcctBalance"),
                        )

                    else -> state
                }
            }

            else -> state
        }
    }
}
