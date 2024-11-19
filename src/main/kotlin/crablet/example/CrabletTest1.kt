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
import io.vertx.pgclient.PgConnectOptions
import io.vertx.sqlclient.Pool
import io.vertx.sqlclient.PoolOptions
import java.util.*

val connectOptions: PgConnectOptions = PgConnectOptions()
    .setPort(5432)
    .setHost("127.0.0.1")
    .setDatabase("postgres")
    .setUser("postgres")
    .setPassword("postgres")

// pool options
val poolOptions: PoolOptions = PoolOptions().setMaxSize(5)

// create the pool from the data object
val pool: Pool = Pool.pool(connectOptions, poolOptions)

fun main() {

    println("Vertx Pool started")

    val eventsAppender = CrabletEventsAppender(pool)

    val stateBuilder = CrabletStateBuilder(
        client = pool,
        initialState = JsonArray(),
        evolveFunction = { state, event -> state.add(event) })

    val di = listOf(DomainIdentifier(name = StateName("Account"), id = StateId(UUID.randomUUID().toString())))
    val sq =
        StreamQuery(identifiers = di, eventTypes = listOf("AccountOpened", "AmountDeposited").map { EventName(it) })
    val appendCondition = AppendCondition(query = sq, maximumEventSequence = SequenceNumber(0))

    val eventPayloads: List<JsonObject> = listOf(
        JsonObject().put("type", "AccountOpened").put("id", 10),
        JsonObject().put("type", "AmountDeposited").put("amount", 10)
    )

    // append events
    eventsAppender.appendIf(eventPayloads, appendCondition)
        .compose {
            // print the resulting sequenceId
            println("New sequence id ---> $it")
            // now project a state given the past events
            stateBuilder.buildFor(sq)
        }
        .onSuccess { stateResult: Pair<JsonArray, SequenceNumber> ->
            println("New state ---> ${stateResult.first.encodePrettily()}")
        }
        .onFailure { it.printStackTrace() }

}
