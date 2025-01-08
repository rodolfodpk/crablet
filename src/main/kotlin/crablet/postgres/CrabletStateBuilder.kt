package crablet.postgres

import crablet.SequenceNumber
import crablet.StateBuilder
import crablet.TransactionContext
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowStream
import io.vertx.sqlclient.Tuple
import org.slf4j.LoggerFactory

class CrabletStateBuilder<S>(
    private val client: Pool,
    private val initialState: S,
    private val evolveFunction: (S, JsonObject) -> S,
    private val pageSize: Int = 1000,
) : StateBuilder<S> {

    override fun buildFor(
        transactionContext: TransactionContext,
    ): Pair<S, SequenceNumber> {

        val promise = Promise.promise<Pair<S, SequenceNumber>>()
        val sql = sqlQuery()
        val domainIds = transactionContext.identifiers.map { it.toStorageFormat() }.sorted().toTypedArray()
        val eventTypes = transactionContext.eventTypes.map { it.value }.toTypedArray()
        val tuple = Tuple.of(domainIds, eventTypes)
        var finalState = initialState
        var lastSequence = 0L
        var error: RuntimeException? = null

        client.withConnection { connection ->
            connection
                .prepare(sql)
                .onFailure { promise.fail(it) }
                .onSuccess { preparedStatement ->
                    // Streams require to run within a transaction
                    connection
                        .begin()
                        .onFailure { promise.fail(it) }
                        .onSuccess { tx ->
                            // Fetch pageSize rows at a time
                            val stream: RowStream<Row> = preparedStatement.createStream(pageSize, tuple)
                            // Use the stream
                            stream.exceptionHandler { err: Throwable ->
                                error = java.lang.RuntimeException(err)
                                logger.error("Stream error", err)
                            }
                            stream.endHandler {
                                if (error != null) {
                                    promise.fail(error)
                                } else {
                                    promise.complete(Pair(finalState, SequenceNumber(lastSequence)))
                                }
                                // Close the stream to release the resources in the database
                                stream
                                    .close()
                                    .compose {
                                        tx.commit()
                                    }
                            }
                            stream.handler { row: Row ->
                                val jsonObject = row.getJsonObject("event_payload")
                                lastSequence = row.getLong("sequence_id")
                                finalState = evolveFunction.invoke(finalState, jsonObject)
                                logger.debug("Event: {} -> {}", lastSequence, jsonObject)
                            }
                        }
                }
        }
        return promise.future().await()
    }

    private fun sqlQuery(): String {
        return """select event_payload, sequence_id
      |         from events
      |        where domain_ids @> $1::text[]
      |          and event_type = ANY($2)
      |        order by sequence_id
      |
    """.trimMargin()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CrabletStateBuilder::class.java)
    }
}