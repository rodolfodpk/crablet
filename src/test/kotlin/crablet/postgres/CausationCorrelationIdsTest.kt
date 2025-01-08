package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.TransactionContext
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.vertx.core.json.JsonObject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class CausationCorrelationIdsTest : AbstractCrabletTest() {

    @Test
    @Order(1)
    fun `it can open Account 1 with correct IDs`() {
        val testRepository = TestRepository(pool)

        val transactionContext = TransactionContext(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 1),
            JsonObject().put("type", "AmountDeposited").put("amount", 10),
            JsonObject().put("type", "AmountDeposited").put("amount", 20),
            JsonObject().put("type", "AmountDeposited").put("amount", 30)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition).await()
        sequence.value shouldBeExactly 4L
        val ids = testRepository.getSequences()!!.await()
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

    @Test
    @Order(2)
    fun `it can open Account 2  with correct IDs`() {
        val testRepository = TestRepository(pool)

        val transactionContext = TransactionContext(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 2),
            JsonObject().put("type", "AmountDeposited").put("amount", 10),
            JsonObject().put("type", "AmountDeposited").put("amount", 20),
            JsonObject().put("type", "AmountDeposited").put("amount", 30)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition).await()
        sequence.value shouldBeExactly 8L
        val ids = testRepository.getSequences()!!.await()
        val expectedResults = listOf(
            Triple(1L, 1L, 1L),
            Triple(2L, 1L, 1L),
            Triple(3L, 2L, 1L),
            Triple(4L, 3L, 1L),

            Triple(5L, 5L, 5L),
            Triple(6L, 5L, 5L),
            Triple(7L, 6L, 5L),
            Triple(8L, 7L, 5L)
        )
        expectedResults.size shouldBeExactly ids.size
        ids.forEachIndexed { index, triple ->
            triple shouldBeEqual expectedResults[index]
        }
    }

    companion object {

        lateinit var eventsAppender: CrabletEventsAppender
        lateinit var testRepository: TestRepository

        val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }

        @BeforeAll
        @JvmStatic
        fun setUp() {
            eventsAppender = CrabletEventsAppender(pool)
            testRepository = TestRepository(pool)
            cleanDatabase()
        }
    }
}