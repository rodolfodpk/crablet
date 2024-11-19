package crablet.postgres

import crablet.AppendCondition
import crablet.EventsAppender
import crablet.SequenceNumber
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Tuple

class CrabletEventsAppender(private val client: Pool) : EventsAppender {

    override fun appendIf(events: List<JsonObject>, appendCondition: AppendCondition): Future<SequenceNumber> {
        val promise = Promise.promise<SequenceNumber>()

        client.withTransaction { connection ->
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

            // Execute the prepared query, it is in transaction due to the use of #withTransaction method.
            connection.preparedQuery(functionCall)
                .execute(params)
        }.onSuccess { rowSet ->
            // Extract the result (last_sequence_id) from the first row
            val latestSequenceId = rowSet.firstOrNull()?.getLong("last_sequence_id")
            if (latestSequenceId != null) {
                promise.complete(SequenceNumber(latestSequenceId))
            } else {
                promise.fail("No last_sequence_id returned from append_events function")
            }
        }.onFailure {
            promise.fail(it)
        }
        return promise.future()
    }

}