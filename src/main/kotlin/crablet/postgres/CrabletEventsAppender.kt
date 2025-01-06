package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.EventsAppender
import crablet.SequenceNumber
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class CrabletEventsAppender(private val client: Pool) : EventsAppender {

    override fun appendIf(events: List<JsonObject>, appendCondition: AppendCondition): Future<SequenceNumber> {
        val promise = Promise.promise<SequenceNumber>()

        client.withTransaction { connection ->
            val params = prepareQueryParams(appendCondition, events)
            executeQuery(connection, params)
        }.onSuccess { rowSet -> processRowSet(rowSet, promise) }
            .onFailure { throwable -> promise.fail(throwable) }

        return promise.future()
    }

    private fun prepareQueryParams(appendCondition: AppendCondition, events: List<JsonObject>) =
        Tuple.of(
            identifiersToSortedArray(appendCondition.transactionContext.identifiers),
            appendCondition.expectedCurrentSequence.value,
            eventTypesToArray(appendCondition.transactionContext.eventTypes),
            eventPayloadsToArray(events)
        )

    private fun identifiersToSortedArray(identifiers: List<DomainIdentifier>) =
        identifiers.map(DomainIdentifier::toStorageFormat).sorted().toTypedArray()

    private fun eventTypesToArray(eventTypes: List<EventName>) = eventTypes.map(EventName::value).toTypedArray()

    private fun eventPayloadsToArray(events: List<JsonObject>) = events.map(JsonObject::encode).toTypedArray()

    private fun executeQuery(connection: SqlConnection, params: Tuple): Future<RowSet<Row>> =
        connection.preparedQuery("SELECT append_events($1, $2, $3, $4) AS $LAST_SEQUENCE_ID")
            .execute(params)

    private fun processRowSet(rowSet: RowSet<Row>, promise: Promise<SequenceNumber>) {
        val firstRowSequenceId = rowSet.first().getLong(LAST_SEQUENCE_ID)

        if (rowSet.rowCount() == 1 && firstRowSequenceId != null) {
            promise.complete(SequenceNumber(firstRowSequenceId))
        } else {
            promise.fail("No last_sequence_id returned from append_events function")
        }
    }

    companion object {
        const val LAST_SEQUENCE_ID = "last_sequence_id"
    }
}