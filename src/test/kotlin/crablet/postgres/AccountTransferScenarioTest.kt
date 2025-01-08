package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.TransactionContext
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AccountTransferScenarioTest : AbstractCrabletTest() {

    @AfterEach
    fun log() {
        dumpEvents()
    }

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`() = runTest {
        val transactionContext = TransactionContext(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 1),
            JsonObject().put("type", "AmountDeposited").put("amount", 50),
            JsonObject().put("type", "AmountDeposited").put("amount", 50)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
        sequence.value shouldBeExactly 3L

        val (state, seq) = stateBuilder.buildFor(transactionContext)
        seq.value shouldBeExactly 3L
        state.id shouldBe 1
        state.balance shouldBeExactly 100
    }

    @Test
    @Order(2)
    fun `it can open Account 2 with $0`() = runTest {
        val transactionContext = TransactionContext(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 2)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
        sequence.value shouldBeExactly 4L

        val (state, seq) = stateBuilder.buildFor(transactionContext)
        seq.value shouldBeExactly sequence.value
        state.id shouldBe 2
        state.balance shouldBeExactly 0
    }

    @Test
    @Order(3)
    fun `it can transfer $30 from Account 1 to Account 2 within the same db transaction`() = runTest {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1")),
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val transactionContext = TransactionContext(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(3))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 1).put("toAcct", 2).put("amount", 30)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
        sequence.value shouldBeExactly 5L

        val domainIdentifiersAcct1 = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1"))
        )
        val (state1, seq1) = stateBuilder
            .buildFor(TransactionContext(identifiers = domainIdentifiersAcct1, eventTypes = eventTypes))

        seq1.value shouldBeExactly sequence.value
        state1.id shouldBe 1
        state1.balance shouldBeExactly 70

        val domainIdentifiersAcct2 = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val (state2, seq2) = stateBuilder
            .buildFor(TransactionContext(identifiers = domainIdentifiersAcct2, eventTypes = eventTypes))

        seq2.value shouldBeExactly sequence.value
        state2.id shouldBe 2
        state2.balance shouldBeExactly 30
    }

    @Test
    @Order(4)
    fun `it can transfer $10 from Account 2 to Account 1 within the same db transaction`() = runTest {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1")),
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val transactionContext = TransactionContext(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(5))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 2).put("toAcct", 1).put("amount", 10)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
        sequence.value shouldBeExactly 6L

        val domainIdentifiersAcct1 = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1"))
        )
        val (state1, seq1) = stateBuilder
            .buildFor(TransactionContext(identifiers = domainIdentifiersAcct1, eventTypes = eventTypes))

        seq1.value shouldBeExactly sequence.value
        state1.id shouldBe 1
        state1.balance shouldBeExactly 80

        val domainIdentifiersAcct2 = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val (state2, seq2) = stateBuilder
            .buildFor(TransactionContext(identifiers = domainIdentifiersAcct2, eventTypes = eventTypes))

        seq2.value shouldBeExactly sequence.value
        state2.id shouldBe 2
        state2.balance shouldBeExactly 20

    }

    @Test
    @Order(5)
    fun `it can transfer $1 from Account 2 to Account 1 within the same db transaction`() = runTest {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1")),
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val transactionContext = TransactionContext(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(6))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 2).put("toAcct", 1).put("amount", 1)
        )
        val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
        sequence.value shouldBeExactly 7L

        val domainIdentifiersAcct1 = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1"))
        )
        val (state1, seq1) = stateBuilder
            .buildFor(TransactionContext(identifiers = domainIdentifiersAcct1, eventTypes = eventTypes))

        seq1.value shouldBeExactly sequence.value
        state1.id shouldBe 1
        state1.balance shouldBeExactly 81

        val domainIdentifiersAcct2 = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val (state2, seq2) = stateBuilder
            .buildFor(TransactionContext(identifiers = domainIdentifiersAcct2, eventTypes = eventTypes))
        seq2.value shouldBeExactly sequence.value
        state2.id shouldBe 2
        state2.balance shouldBeExactly 19
    }

    companion object {
        lateinit var eventsAppender: CrabletEventsAppender
        lateinit var stateBuilder: CrabletStateBuilder<Account>

        data class Account(val id: Int? = null, val balance: Int = 0)

        val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }

        private val evolveFunction: (Account, JsonObject) -> Account = { state, event ->
            when (event.getString("type")) {
                "AccountOpened" -> state.copy(id = event.getInteger("id"))
                "AmountDeposited" -> state.copy(balance = state.balance.plus(event.getInteger("amount")))
                "AmountTransferred" -> {
                    when {
                        event.getInteger("fromAcct") == state.id -> state.copy(
                            balance = state.balance.minus(event.getInteger("amount"))
                        )

                        event.getInteger("toAcct") == state.id -> state.copy(
                            balance = state.balance.plus(event.getInteger("amount"))
                        )

                        else -> state
                    }
                }

                else -> state
            }
        }

        @BeforeAll
        @JvmStatic
        fun setUp() {
            eventsAppender = CrabletEventsAppender(pool)
            stateBuilder = CrabletStateBuilder(
                client = pool,
                initialState = { Account() },
                evolveFunction = evolveFunction
            )
            cleanDatabase()
        }
    }

}