package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.StreamQuery
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.*

@ExtendWith(VertxExtension::class)
class Scenario2TestAbstract : AbstractCrabletTest() {

    data class Account(val id: Int? = null, val balance: Int = 0)

    lateinit var eventsAppender: CrabletEventsAppender
    lateinit var stateBuilder: CrabletStateBuilder<Account>
    lateinit var appendCondition: AppendCondition
    lateinit var eventsToAppend: List<JsonObject>
    lateinit var streamQuery: StreamQuery

    @BeforeEach
    fun setUp(testContext: VertxTestContext) {
        eventsAppender = CrabletEventsAppender(pool)
        stateBuilder = CrabletStateBuilder(
            client = pool,
            initialState = Account(),
            evolveFunction = { state, event ->
                when (event.getString("type")) {
                    "AccountOpened" -> state.copy(id = event.getInteger("id"))
                    "AmountDeposited" -> state.copy(balance = state.balance.plus(event.getInteger("amount")))
                    else -> state
                }
            })

        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId(UUID.randomUUID().toString()))
        )
        streamQuery = StreamQuery(
            identifiers = domainIdentifiers,
            eventTypes = listOf("AccountOpened", "AmountDeposited").map { EventName(it) }
        )
        appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
        eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 10),
            JsonObject().put("type", "AmountDeposited").put("amount", 100)
        )
        testContext.completeNow()
    }

    @Test
    fun testAppendAndBuildState(testContext: VertxTestContext) {
        // Append events and build the state
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .compose {
                stateBuilder.buildFor(streamQuery)
            }
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->

                assertNotNull(state)
                assertNotNull(sequence)

                assertEquals(state.id, 10)
                assertEquals(state.balance, 100)

                // Complete the test context indicating the test passed
                testContext.completeNow()
            }
            .onFailure { it ->
                // Fail the test context indicating the test failed
                testContext.failNow(it)
            }
    }

}