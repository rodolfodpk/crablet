package crablet.command.impl

import crablet.EventName
import crablet.SequenceNumber
import crablet.command.AppendCondition
import crablet.command.DomainIdentifier
import crablet.command.EventsAppender
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple

class CrabletEventsAppender(
    private val pool: Pool,
) : EventsAppender {
    override suspend fun appendIf(
        events: List<JsonObject>,
        appendCondition: AppendCondition,
    ): SequenceNumber =
        pool
            .withTransaction { connection ->
                val params = prepareQueryParams(appendCondition, events)
                executeQuery(connection, params)
            }.compose { rowSet -> processRowSet(rowSet) }
            .coAwait()

    private fun prepareQueryParams(
        appendCondition: AppendCondition,
        events: List<JsonObject>,
    ) = Tuple.of(
        identifiersToSortedArray(appendCondition.transactionContext.identifiers),
        appendCondition.expectedCurrentSequence.value,
        eventTypesToArray(appendCondition.transactionContext.eventTypes),
        eventPayloadsToArray(events),
        appendCondition.lockId(),
    )

    private fun identifiersToSortedArray(identifiers: List<DomainIdentifier>) =
        identifiers.map(DomainIdentifier::toStorageFormat).sorted().toTypedArray()

    private fun eventTypesToArray(eventTypes: List<EventName>) = eventTypes.map(EventName::value).toTypedArray()

    private fun eventPayloadsToArray(events: List<JsonObject>) = events.map(JsonObject::encode).toTypedArray()

    private fun executeQuery(
        connection: SqlConnection,
        params: Tuple,
    ): Future<RowSet<Row>> =
        connection
            .preparedQuery("SELECT append_events($1, $2, $3, $4, $5) AS $LAST_SEQUENCE_ID")
            .execute(params)

    private fun processRowSet(rowSet: RowSet<Row>): Future<SequenceNumber>? {
        val firstRowSequenceId = rowSet.first()?.getLong(LAST_SEQUENCE_ID)
        return if (firstRowSequenceId != null && rowSet.rowCount() == 1) {
            Future.succeededFuture(SequenceNumber(firstRowSequenceId))
        } else {
            Future.failedFuture("No last_sequence_id returned from append_events function")
        }
    }

    companion object {
        const val LAST_SEQUENCE_ID = "last_sequence_id"
    }
}
