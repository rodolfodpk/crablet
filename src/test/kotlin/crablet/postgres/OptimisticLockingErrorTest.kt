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
import io.kotest.matchers.string.shouldContain
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
class OptimisticLockingErrorTest : AbstractCrabletTest() {

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`(testContext: VertxTestContext) {
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
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .compose {
                dumpEvents()
            }
            .compose {
                stateBuilder.buildFor(transactionContext)
            }
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
                testContext.verify {
                    sequence.value shouldBeExactly 3L
                    state.id shouldBe 1
                    state.balance shouldBeExactly 100
                }
                testContext.completeNow()
            }
            .onFailure { it ->
                testContext.failNow(it)
            }

    }

    @Test
    @Order(2)
    fun `it will fail if expectedCurrentSequence does not match`(testContext: VertxTestContext) {
        val transactionContext = TransactionContext(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        val appendCondition =
            AppendCondition(transactionContext = transactionContext, expectedCurrentSequence = SequenceNumber(2))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountDeposited").put("amount", 60)
        )
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .onSuccess {
                testContext.failNow("It should fail")
            }
            .onFailure {
                testContext.verify {
                    it.message shouldContain
                            "Sequence mismatch: the current last sequence 3 from the database does not match the expected sequence: 2."
                }
                testContext.completeNow()
            }
    }

    @Test
    @Order(8)
    fun `Account 1 state is intact`(testContext: VertxTestContext) {
        val transactionContext = TransactionContext(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        stateBuilder.buildFor(transactionContext)
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
                testContext.verify {
                    sequence.value shouldBeExactly 3L
                    state.id shouldBe 1
                    state.balance shouldBeExactly 100
                }
                testContext.completeNow()
            }
            .onFailure { it ->
                testContext.failNow(it)
            }
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
        fun setUp(testContext: VertxTestContext) {
            eventsAppender = CrabletEventsAppender(pool)
            stateBuilder = CrabletStateBuilder(
                client = pool,
                initialState = Account(),
                evolveFunction = evolveFunction
            )
            cleanDatabase().onSuccess { testContext.completeNow() }
        }
    }

}