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
    DOMAIN_IDS_HASH(1), // hash of the domain_ids: the most restrictive mode
    LATEST_SEQUENCE_ID(2), // latest sequence_id within the stream that can contain N aggregate roots
    CORRELATION_ID(3), // only for single aggregate transactions
}

data class AppendCondition(
    val transactionContext: TransactionContext,
    val expectedCurrentSequence: SequenceNumber,
    val lockingPolicy: LockingPolicy = LockingPolicy.DOMAIN_IDS_HASH,
) {
    fun lockId() = lockingPolicy.lockId
}

interface EventsAppender {
    suspend fun appendIf(
        events: List<JsonObject>,
        appendCondition: AppendCondition,
    )
}

interface StateBuilder {
    suspend fun <S> buildFor(
        transactionContext: TransactionContext,
        initialStateFunction: () -> S,
        onEventFunction: (S, JsonObject) -> S,
    ): Pair<S, SequenceNumber>
}
