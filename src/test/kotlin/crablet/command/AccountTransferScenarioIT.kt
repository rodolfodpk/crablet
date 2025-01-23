package crablet.command

import crablet.AbstractCrabletTest
import crablet.SequenceNumber
import crablet.TestAccountDomain.evolveFunction
import crablet.TestAccountDomain.initialStateFunction
import crablet.TestAccountsContext
import crablet.command.impl.CrabletEventsAppender
import crablet.command.impl.CrabletStateBuilder
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AccountTransferScenarioIT :
    AbstractCrabletTest(),
    TestAccountsContext {
    @Test
    @Order(0)
    fun `when setup is done`() =
        runTest {
            eventsAppender = CrabletEventsAppender(pool = pool)
            stateBuilder = CrabletStateBuilder(pool = pool)
            testRepository.cleanDatabase()
        }

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`() =
        runTest {
            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct1,
                    expectedCurrentSequence = SequenceNumber(0),
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 1),
                    JsonObject()
                        .put("type", "AmountDeposited")
                        .put("id", 1)
                        .put("amount", 50)
                        .put("balance", 50),
                    JsonObject()
                        .put("type", "AmountDeposited")
                        .put("id", 1)
                        .put("amount", 50)
                        .put("balance", 100),
                )
            eventsAppender.appendIf(eventsToAppend, appendCondition)

            testRepository.dumpAllState()

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
    fun `it can open Account 2 with $0`() =
        runTest {
            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct2,
                    expectedCurrentSequence = SequenceNumber(0),
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 2),
                )
            eventsAppender.appendIf(eventsToAppend, appendCondition)

            val (state, seq) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            seq.value shouldBeExactly 4L
            state.id shouldBe 2
            state.balance shouldBeExactly 0
        }

    @Test
    @Order(3)
    fun `it can transfer $30 from Account 1 to Account 2 within the same db transaction`() =
        runTest {
            val domainIdentifiers =
                listOf(
                    DomainIdentifier(name = StateName("Account"), id = StateId("1")),
                    DomainIdentifier(name = StateName("Account"), id = StateId("2")),
                )
            val transactionContext =
                TransactionContext(
                    identifiers = domainIdentifiers,
                    eventTypes = eventTypes,
                )
            val appendCondition =
                AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(3))
            val eventsToAppend =
                listOf(
                    JsonObject()
                        .put("type", "AmountTransferred")
                        .put("fromAcct", 1)
                        .put("toAcct", 2)
                        .put("amount", 30)
                        .put("fromAcctBalance", 70)
                        .put("toAcctBalance", 30),
                )
            eventsAppender.appendIf(eventsToAppend, appendCondition)

            val (state1, seq1) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq1.value shouldBeExactly 5L
            state1.id shouldBe 1
            state1.balance shouldBeExactly 70

            val (state2, seq2) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq2.value shouldBeExactly 5L
            state2.id shouldBe 2
            state2.balance shouldBeExactly 30
        }

    @Test
    @Order(4)
    fun `it can transfer $10 from Account 2 to Account 1 within the same db transaction`() =
        runTest {
            val domainIdentifiers =
                listOf(
                    DomainIdentifier(name = StateName("Account"), id = StateId("1")),
                    DomainIdentifier(name = StateName("Account"), id = StateId("2")),
                )
            val transactionContext =
                TransactionContext(
                    identifiers = domainIdentifiers,
                    eventTypes = eventTypes,
                )
            val appendCondition =
                AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(5))
            val eventsToAppend =
                listOf(
                    JsonObject()
                        .put("type", "AmountTransferred")
                        .put("fromAcct", 2)
                        .put("toAcct", 1)
                        .put("amount", 10)
                        .put("fromAcctBalance", 20)
                        .put("toAcctBalance", 80),
                )
            eventsAppender.appendIf(eventsToAppend, appendCondition)

            val (state1, seq1) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq1.value shouldBeExactly 6L
            state1.id shouldBe 1
            state1.balance shouldBeExactly 80

            val (state2, seq2) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq2.value shouldBeExactly 6L
            state2.id shouldBe 2
            state2.balance shouldBeExactly 20
        }

    @Test
    @Order(5)
    fun `it can transfer $1 from Account 2 to Account 1 within the same db transaction`() =
        runTest {
            val domainIdentifiers =
                listOf(
                    DomainIdentifier(name = StateName("Account"), id = StateId("1")),
                    DomainIdentifier(name = StateName("Account"), id = StateId("2")),
                )
            val transactionContext =
                TransactionContext(
                    identifiers = domainIdentifiers,
                    eventTypes = eventTypes,
                )
            val appendCondition =
                AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(6))
            val eventsToAppend =
                listOf(
                    JsonObject()
                        .put("type", "AmountTransferred")
                        .put("fromAcct", 2)
                        .put("toAcct", 1)
                        .put("amount", 1)
                        .put("fromAcctBalance", 19)
                        .put("toAcctBalance", 81),
                )
            eventsAppender.appendIf(eventsToAppend, appendCondition)

            val (state1, seq1) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq1.value shouldBeExactly 7L
            state1.id shouldBe 1
            state1.balance shouldBeExactly 81

            val (state2, seq2) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq2.value shouldBeExactly 7L
            state2.id shouldBe 2
            state2.balance shouldBeExactly 19
        }

    companion object {
        private lateinit var eventsAppender: CrabletEventsAppender
        private lateinit var stateBuilder: CrabletStateBuilder
    }
}
