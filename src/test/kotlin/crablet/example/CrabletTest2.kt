package crablet.example

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.StreamQuery
import crablet.postgres.CrabletEventsAppender
import crablet.postgres.CrabletStateBuilder
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.*

fun main() {

    println("Vertx Pool started")

    val eventsAppender = CrabletEventsAppender(pool)

    val stateBuilder = CrabletStateBuilder(
        client = pool,
        initialState = JsonArray(),
        evolveFunction = { state, event -> state.add(event) })

    val domainIdentifiers = listOf(
        DomainIdentifier(name = StateName("Account"), id = StateId(UUID.randomUUID().toString()))
    )

    val streamQuery = StreamQuery(
        identifiers = domainIdentifiers,
        eventTypes = listOf("AccountOpened", "AmountDeposited").map { EventName(it) }
    )

    val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))

    val eventsToAppend: List<JsonObject> = listOf(
        JsonObject().put("type", "AccountOpened").put("id", 10),
        JsonObject().put("type", "AmountDeposited").put("amount", 100)
    )

    println("Append operation")
    println("--> eventsToAppend: $eventsToAppend")
    println("--> appendCondition: $appendCondition ")

    // append events
    eventsAppender.appendIf(eventsToAppend, appendCondition)
        .compose {
            // print the resulting sequenceId
            println()
            println("New sequence id ---> $it")
            // now project a state given the past events
            stateBuilder.buildFor(streamQuery)
        }
        .onSuccess { stateResult: Pair<JsonArray, SequenceNumber> ->
            println("New state ---> ${stateResult.first}")
        }
        .onFailure { it.printStackTrace() }

}
