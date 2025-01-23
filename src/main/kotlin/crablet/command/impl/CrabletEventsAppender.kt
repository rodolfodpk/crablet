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
    ) {
        pool
            .withConnection { conn ->
                conn.preparedQuery("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute()
                    .compose { conn
                        .preparedQuery("CALL append_events($1, $2, $3, $4, $5);")
                        .execute(prepareQueryParams(appendCondition, events))
                    }
            }
            .mapEmpty<Void>()
            .coAwait()
    }

    private fun prepareQueryParams(
        appendCondition: AppendCondition,
        events: List<JsonObject>,
    ) = Tuple.of(
        identifiersToSortedArray(appendCondition.transactionContext.identifiers),
        appendCondition.expectedCurrentSequence.value,
        eventTypesToArray(appendCondition.transactionContext.eventTypes),
        eventPayloadsToArray(events),
        appendCondition.lockId()
    )

    private fun identifiersToSortedArray(identifiers: List<DomainIdentifier>) =
        identifiers.map(DomainIdentifier::toStorageFormat).sorted().toTypedArray()

    private fun eventTypesToArray(eventTypes: List<EventName>) = eventTypes.map(EventName::value).toTypedArray()

    private fun eventPayloadsToArray(events: List<JsonObject>) = events.map(JsonObject::encode).toTypedArray()

}
