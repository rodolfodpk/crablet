package crablet.command

import crablet.EventName
import crablet.SequenceNumber
import io.vertx.core.json.JsonObject

@JvmInline
value class StateName(
    val value: String,
)

@JvmInline
value class StateId(
    val value: String,
)

data class DomainIdentifier(
    val name: StateName,
    val id: StateId,
) {
    fun toStorageFormat(): String =
        this.id.value
            .plus("@")
            .plus(this.name.value)
}

data class TransactionContext(
    val identifiers: List<DomainIdentifier>,
    val eventTypes: List<EventName>,
)

enum class LockingPolicy(
    val lockId: Int,
) {
    LASTEST_SEQUENCE_ID(1),
    CORRELATION_ID(2),
    DOMAIN_IDS_HASH(3),
}

data class AppendCondition(
    val transactionContext: TransactionContext,
    val expectedCurrentSequence: SequenceNumber,
    val lockingPolicy: LockingPolicy = LockingPolicy.LASTEST_SEQUENCE_ID,
) {
    fun lockId() = lockingPolicy.lockId
}

interface EventsAppender {
    suspend fun appendIf(
        events: List<JsonObject>,
        appendCondition: AppendCondition,
    ): SequenceNumber
}

interface StateBuilder {
    suspend fun <S> buildFor(
        transactionContext: TransactionContext,
        initialStateFunction: () -> S,
        onEventFunction: (S, JsonObject) -> S,
    ): Pair<S, SequenceNumber>
}
