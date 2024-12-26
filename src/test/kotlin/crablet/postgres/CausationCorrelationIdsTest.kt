package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.StreamQuery
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeExactly
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(VertxExtension::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class CausationCorrelationIdsTest : AbstractCrabletTest() {

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`(testContext: VertxTestContext) {
        val testRepository = TestRepository(pool)

        val streamQuery = StreamQuery(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 1),
            JsonObject().put("type", "AmountDeposited").put("amount", 50),
            JsonObject().put("type", "AmountDeposited").put("amount", 50),
            JsonObject().put("type", "AmountDeposited").put("amount", 50)
        )
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .compose {
                dumpEvents()
            }
            .compose {
                testRepository.getSequences()
            }
            .onSuccess { ids ->
                testContext.verify {
                    val expectedResults = listOf(
                        Triple(1L, 1L, 1L),
                        Triple(2L, 1L, 1L),
                        Triple(3L, 2L, 1L),
                        Triple(4L, 3L, 1L)
                    )
                    expectedResults.size shouldBeExactly ids.size
                    ids.forEachIndexed { index, triple ->
                        triple shouldBeEqual expectedResults[index]
                    }
                }
                testContext.completeNow()
            }
            .onFailure { it ->
                testContext.failNow(it)
            }

    }

    companion object {

        lateinit var eventsAppender: CrabletEventsAppender
        lateinit var testRepository: TestRepository

        val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }

        @BeforeAll
        @JvmStatic
        fun setUp(testContext: VertxTestContext) {
            eventsAppender = CrabletEventsAppender(pool)
            testRepository = TestRepository(pool)
            cleanDatabase().onSuccess { testContext.completeNow() }
        }
    }
}