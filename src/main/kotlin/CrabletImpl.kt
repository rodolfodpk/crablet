import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowStream
import io.vertx.sqlclient.Tuple
import jsonvalues.spec.JsObjSpecParser

class JsonObjectEventsValidator(private val parser: JsObjSpecParser) : EventsValidator {

    override fun validate(events: List<JsonObject>) {
        parser.parse(JsonArray(events).toBuffer().bytes)
    }

}

class CrabletEventsAppender(private val client: Pool) : EventsAppender {

    override fun appendIf(events: List<JsonObject>, appendCondition: AppendCondition): Future<SequenceNumber> {
        val promise = Promise.promise<SequenceNumber>()
        client.getConnection { ar ->
            if (ar.succeeded()) {
                val connection = ar.result()
                val jsonArrayDomainIds =
                    appendCondition.query.identifiers.map { it.toStorageFormat() }.sorted().toTypedArray()
                val jsonArrayEventTypes = appendCondition.query.eventTypes.map { it.value }.toTypedArray()
                val jsonArrayEventPayloads = events.map { it.encode() }.toTypedArray()

                // Create a Tuple to pass as parameters
                val params = Tuple.of(
                    jsonArrayDomainIds,
                    appendCondition.maximumEventSequence.value,
                    jsonArrayEventTypes,
                    jsonArrayEventPayloads
                )

                // Use prepared statement for SELECT query
                val functionCall = "SELECT append_events($1, $2, $3, $4) AS last_sequence_id"

                // Execute the prepared query
                connection.preparedQuery(functionCall)
                    .execute(params) { resultQuery ->
                        if (resultQuery.succeeded()) {
                            val resultSet = resultQuery.result()
                            // Extract the result (last_sequence_id) from the first row
                            val latestSequenceId = resultSet.firstOrNull()?.getLong("last_sequence_id")
                            connection.close()
                            if (latestSequenceId != null) {
                                promise.complete(SequenceNumber(latestSequenceId))
                            } else {
                                promise.fail("No last_sequence_id returned from append_events function")
                            }
                        } else {
                            connection.close()
                            promise.fail(resultQuery.cause())
                        }
                    }
            } else {
                promise.fail(ar.cause())
            }
        }
        return promise.future()

    }

}

class CrabletStateBuilder<S>(
    private val client: Pool,
    private val initialState: S,
    private val evolveFunction: (S, JsonObject) -> S,
    private val pageSize: Int = 1000,
) : StateBuilder<S> {

    private fun sqlQuery(): String {
        return """select event_payload, sequence_id
      |         from events
      |        where domain_ids @> $1::text[]
      |          and event_type = ANY($2)
      |        order by sequence_id
      |
    """.trimMargin()
    }

    override fun buildFor(
        query: StreamQuery,
    ): Future<Pair<S, SequenceNumber>> {

        val promise = Promise.promise<Pair<S, SequenceNumber>>()
        val sql = sqlQuery()
        val domainIds = query.identifiers.map { it.toStorageFormat() }.sorted().toTypedArray()
        val eventTypes = query.eventTypes.map { it.value }.toTypedArray()
        val tuple = Tuple.of(domainIds, eventTypes)
        var finalState = initialState
        var lastSequence = 0L
        var error: RuntimeException? = null

        client.withConnection { connection ->
            connection
                .prepare(sql)
                .onFailure { promise.fail(it) }
                .onSuccess { pq ->
                    // Streams require to run within a transaction
                    connection
                        .begin()
                        .onFailure { promise.fail(it) }
                        .onSuccess { tx ->
                            // Fetch pageSize rows at a time
                            val stream: RowStream<Row> = pq.createStream(pageSize, tuple)
                            // Use the stream
                            stream.exceptionHandler { err: Throwable ->
                                error = java.lang.RuntimeException(err)
                                err.printStackTrace()
                            }
                            stream.endHandler { v: Void? ->
                                if (error != null) {
                                    promise.fail(error)
                                } else {
                                    promise.complete(Pair(finalState, SequenceNumber(lastSequence)))
                                }
                                // Close the stream to release the resources in the database
                                stream
                                    .close()
                                    .onComplete { closed: AsyncResult<Void?>? ->
                                        tx.commit()
                                            .onComplete { committed: AsyncResult<Void?>? ->
                                                println("End of stream")
                                            }
                                    }
                            }
                            stream.handler { row: Row ->
                                val jsonObject = row.getJsonObject("event_payload")
                                lastSequence = row.getLong("sequence_id")
                                finalState = evolveFunction.invoke(finalState, jsonObject)
                                println("Event: " + lastSequence + "  " + jsonObject.encodePrettily() + " = " + finalState)
                                // val jsonObj = JsObj.parse(jsonObject.toBuffer().bytes)
                            }
                        }
                }
        }
        return promise.future()
    }
}

