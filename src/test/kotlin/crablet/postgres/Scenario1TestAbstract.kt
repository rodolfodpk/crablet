package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.StreamQuery
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.*

@ExtendWith(VertxExtension::class)
class Scenario1TestAbstract : AbstractCrabletTest() {

    lateinit var eventsAppender: CrabletEventsAppender
    lateinit var stateBuilder: CrabletStateBuilder<JsonArray>
    lateinit var appendCondition: AppendCondition
    lateinit var eventsToAppend: List<JsonObject>
    lateinit var streamQuery: StreamQuery

    @BeforeEach
    fun setUp(testContext: VertxTestContext) {
        eventsAppender = CrabletEventsAppender(pool)
        stateBuilder = CrabletStateBuilder(
            client = pool,
            initialState = JsonArray(),
            evolveFunction = { state, event -> state.add(event) })
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
            .onSuccess { stateResult: Pair<JsonArray, SequenceNumber> ->
                // Verify that the Pair object is not null
                assertNotNull(stateResult)

                // Verify that the JsonArray contains the expected events
                assertNotNull(stateResult.first)
                assertEquals(2, stateResult.first.size())
                assertTrue(stateResult.first.contains(JsonObject().put("type", "AccountOpened").put("id", 10)))
                assertTrue(stateResult.first.contains(JsonObject().put("type", "AmountDeposited").put("amount", 100)))

                // Verify that the SequenceNumber is not null
                assertNotNull(stateResult.second)

                // Complete the test context indicating the test passed
                testContext.completeNow()
            }
            .onFailure { it ->
                // Fail the test context indicating the test failed
                testContext.failNow(it)
            }
    }

}