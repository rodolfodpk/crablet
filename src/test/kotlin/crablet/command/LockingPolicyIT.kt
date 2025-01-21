package crablet.command

import crablet.AbstractCrabletTest
import crablet.SequenceNumber
import crablet.TestAccountDomain.evolveFunction
import crablet.TestAccountDomain.initialStateFunction
import crablet.TestAccountsContext
import crablet.command.impl.CrabletEventsAppender
import crablet.command.impl.CrabletStateBuilder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LockingPolicyIT :
    AbstractCrabletTest(),
    TestAccountsContext {
    @Test
    @Order(0)
    fun `when setup is done`() =
        runTest {
            eventsAppender = CrabletEventsAppender(pool)
            stateBuilder = CrabletStateBuilder(pool = pool)
            testRepository.cleanDatabase()
        }

    @Test
    @Order(1)
    fun `it can open Account 1 with $100 using default lockingPolicy LAST_SEQUENCE_ID`() =
        runTest {
            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct1,
                    expectedCurrentSequence = SequenceNumber(0),
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 1),
                    JsonObject().put("type", "AmountDeposited").put("amount", 50).put("balance", 50),
                    JsonObject().put("type", "AmountDeposited").put("amount", 50).put("balance", 100),
                )
            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
            sequence.value shouldBeExactly 3L

            val (state, seq) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
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
                    expectedCurrentSequence = SequenceNumber(2),
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
            val (state, sequence) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            sequence.value shouldBeExactly 3L
            state.id shouldBe 1
            state.balance shouldBeExactly 100
        }

    @Test
    @Order(4)
    fun `it can open Account 2 with 50 using DOMAIN_IDS_HASH as Locking Policy`() =
        runTest {
            val expectedSequence = 6L // because 1 sequence was generated but not used:
            // expectedCurrentSequence does not match

            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct2,
                    expectedCurrentSequence = SequenceNumber(0),
                    lockingPolicy = LockingPolicy.DOMAIN_IDS_HASH,
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 2),
                    JsonObject().put("type", "AmountDeposited").put("amount", 50).put("balance", 50),
                )

            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)

            sequence.value shouldBeExactly expectedSequence

            val (state, seq) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            seq.value shouldBeExactly expectedSequence
            state.id shouldBe 2
            state.balance shouldBeExactly 50
        }

    @Test
    @Order(5)
    fun `it can open Account 3 with 0 using CORRELATION_ID as Locking Policy`() =
        runTest {
            val expectedSequence = 7L

            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct3,
                    expectedCurrentSequence = SequenceNumber(0),
                    lockingPolicy = LockingPolicy.CORRELATION_ID,
                )

            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 3),
                )

            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
            sequence.value shouldBeExactly expectedSequence

            val (state, seq) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct3,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            seq.value shouldBeExactly expectedSequence
            state.id shouldBe 3
            state.balance shouldBeExactly 0
        }

    companion object {
        private lateinit var eventsAppender: CrabletEventsAppender
        private lateinit var stateBuilder: CrabletStateBuilder
    }
}
