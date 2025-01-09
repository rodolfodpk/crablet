package crablet.command

import crablet.AbstractCrabletTest
import crablet.EventName
import crablet.SequenceNumber
import crablet.TestAccountDomain.evolveFunction
import crablet.TestAccountDomain.initialStateFunction
import crablet.command.impl.CrabletEventsAppender
import crablet.command.impl.CrabletStateBuilder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class OptimisticLockingErrorTest : AbstractCrabletTest() {
    @AfterEach
    fun log() {
        dumpEvents()
    }

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`() =
        runTest {
            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct1,
                    expectedCurrentSequence = SequenceNumber(0)
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 1),
                    JsonObject().put("type", "AmountDeposited").put("amount", 50),
                    JsonObject().put("type", "AmountDeposited").put("amount", 50),
                )
            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
            sequence.value shouldBeExactly 3L

            val (state, seq) = stateBuilder.buildFor(
                transactionContext = transactionContextAcct1,
                initialStateFunction = initialStateFunction,
                onEventFunction = evolveFunction
            )
            seq.value shouldBeExactly 3L
            state.id shouldBe 1
            state.balance shouldBeExactly 100
        }

    @Test
    @Order(2)
    fun `it will fail if expectedCurrentSequence does not match`() =
        runTest {
            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct1,
                    expectedCurrentSequence = SequenceNumber(2)
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AmountDeposited").put("amount", 60),
                )
            val exception =
                shouldThrow<Exception> {
                    eventsAppender.appendIf(eventsToAppend, appendCondition)
                }
            exception.message shouldContain
                    "Sequence mismatch: the current last sequence 3 from the database does not match the expected sequence: 2."
        }

    @Test
    @Order(3)
    fun `Account 1 state is intact`() =
        runTest {
            val (state, sequence) = stateBuilder.buildFor(
                transactionContext = transactionContextAcct1,
                initialStateFunction = initialStateFunction,
                onEventFunction = evolveFunction
            )
            sequence.value shouldBeExactly 3L
            state.id shouldBe 1
            state.balance shouldBeExactly 100
        }

    companion object {
        private lateinit var eventsAppender: CrabletEventsAppender
        private lateinit var stateBuilder: CrabletStateBuilder
        private val transactionContextAcct1 =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
                eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) },
            )

        @BeforeAll
        @JvmStatic
        fun setUp() {
            eventsAppender = CrabletEventsAppender(pool)
            stateBuilder = CrabletStateBuilder(client = pool)
            cleanDatabase()
        }
    }
}
