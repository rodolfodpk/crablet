package crablet.query

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class AccountsPostgresSingleEventProcessor : EventSink.PostgresSingleEventSync {
    override fun handle(
        sqlConnection: SqlConnection,
        eventAsJson: JsonObject,
    ): Future<Void> {
        val eventPayload = eventAsJson.getJsonObject("event_payload")

        return when (eventPayload.getString("type")) {
            "AccountOpened", "AmountDeposited" -> {
                val id = eventPayload.getInteger("id")
                val balance = eventPayload.getInteger("balance", 0)
                sqlConnection
                    .preparedQuery(upsertQuery)
                    .execute(Tuple.of(id, balance))
                    .mapEmpty()
            }

            "AmountTransferred" -> {
                val fromAcctId = eventPayload.getInteger("fromAcct")
                val fromAcctBalance = eventPayload.getInteger("fromAcctBalance")
                val toAcctId = eventPayload.getInteger("toAcct")
                val toAcctBalance = eventPayload.getInteger("toAcctBalance")
                val tuples = listOf(Tuple.of(fromAcctId, fromAcctBalance), Tuple.of(toAcctId, toAcctBalance))
                sqlConnection
                    .preparedQuery(updateQuery)
                    .executeBatch(tuples)
                    .mapEmpty()
            }

            else -> Future.succeededFuture()
        }
    }

    companion object {
        val upsertQuery =
            "INSERT INTO accounts_view(id, balance) VALUES($1, $2) ON CONFLICT (id) DO UPDATE SET balance = $2"

        val updateQuery =
            "UPDATE accounts_view SET balance = $2 WHERE id = $1"
    }
}
