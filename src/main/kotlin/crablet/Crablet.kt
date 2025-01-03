package crablet

import io.vertx.core.Future
import io.vertx.core.json.JsonObject

@JvmInline
value class StateName(val value: String)

@JvmInline
value class StateId(val value: String)

@JvmInline
value class EventName(val value: String)

@JvmInline
value class SequenceNumber(val value: Long)

data class DomainIdentifier(val name: StateName, val id: StateId) {
    fun toStorageFormat(): String = this.name.value.plus("@").plus(this.id.value)
}

data class StreamQuery(val identifiers: List<DomainIdentifier>, val eventTypes: List<EventName>)

data class AppendCondition(val query: StreamQuery, val maximumEventSequence: SequenceNumber)

// write

interface EventsAppender {
    fun appendIf(events: List<JsonObject>, appendCondition: AppendCondition): Future<SequenceNumber>
}

// read

interface StateBuilder<S> {
    fun buildFor(query: StreamQuery): Future<Pair<S, SequenceNumber>>
}
