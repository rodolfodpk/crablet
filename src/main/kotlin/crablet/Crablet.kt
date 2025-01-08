package crablet

import io.vertx.core.json.JsonObject

@JvmInline
value class StateName(
    val value: String,
)

@JvmInline
value class StateId(
    val value: String,
)

@JvmInline
value class EventName(
    val value: String,
)

@JvmInline
value class SequenceNumber(
    val value: Long,
)

data class DomainIdentifier(
    val name: StateName,
    val id: StateId,
) {
    fun toStorageFormat(): String =
        this.name.value
            .plus("@")
            .plus(this.id.value)
}

data class TransactionContext(
    val identifiers: List<DomainIdentifier>,
    val eventTypes: List<EventName>,
)

data class AppendCondition(
    val transactionContext: TransactionContext,
    val expectedCurrentSequence: SequenceNumber,
)

interface EventsAppender {
    suspend fun appendIf(
        events: List<JsonObject>,
        appendCondition: AppendCondition,
    ): SequenceNumber
}

interface StateBuilder {
    suspend fun <S> buildFor(
        transactionContext: TransactionContext,
        initialState: () -> S,
        evolveFunction: (S, JsonObject) -> S,
    ): Pair<S, SequenceNumber>
}
