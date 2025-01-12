package crablet.query.sinks

import crablet.query.EventSink
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class AccountsPostgresBatchEventSink : EventSink.PostgresBatchEventSink {
    override fun handle(
        sqlConnection: SqlConnection,
        eventAsJsonList: List<JsonObject>,
    ): Future<Void> {
        fun collectTuplesAndQueries(eventAsJson: JsonObject): List<Pair<Tuple, String>> {
            val eventPayload = eventAsJson.getJsonObject("event_payload")
            return when (eventPayload.getString("type")) {
                "AccountOpened", "AmountDeposited" -> {
                    val id = eventPayload.getInteger("id")
                    val balance = eventPayload.getInteger("balance", 0)
                    listOf(Pair(Tuple.of(id, balance), upsertQuery))
                }

                "AmountTransferred" -> {
                    val fromAcctId = eventPayload.getInteger("fromAcct")
                    val fromAcctBalance = eventPayload.getInteger("fromAcctBalance")
                    val toAcctId = eventPayload.getInteger("toAcct")
                    val toAcctBalance = eventPayload.getInteger("toAcctBalance")
                    listOf(Tuple.of(fromAcctId, fromAcctBalance), Tuple.of(toAcctId, toAcctBalance))
                        .map { Pair(it, updateQuery) }
                }

                else -> emptyList()
            }
        }

        val futures =
            eventAsJsonList.flatMap(::collectTuplesAndQueries).map { (tuple, query) ->
                sqlConnection.preparedQuery(query).execute(tuple).map { }
            }
        return futures.fold(Future.succeededFuture()) { acc, future ->
            acc
                .compose { future }
                .mapEmpty()
        }
    }

    companion object {
        val upsertQuery =
            "INSERT INTO accounts_view(id, balance) VALUES($1, $2) ON CONFLICT (id) DO UPDATE SET balance = $2"

        val updateQuery =
            "UPDATE accounts_view SET balance = $2 WHERE id = $1"
    }
}
