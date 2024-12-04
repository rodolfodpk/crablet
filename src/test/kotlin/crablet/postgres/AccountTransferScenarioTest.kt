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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(VertxExtension::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AccountTransferScenarioTest : AbstractCrabletTest() {

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`(testContext: VertxTestContext) {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1"))
        )
        val streamQuery = StreamQuery(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", "1"),
            JsonObject().put("type", "AmountDeposited").put("amount", 100)
        )
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .compose {
                dumpEvents()
            }
            .compose {
                stateBuilder.buildFor(streamQuery)
            }
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
                testContext.verify {
                    assertEquals(2, sequence.value)
                    assertEquals("1", state.id)
                    assertEquals(100, state.balance)
                }
                testContext.completeNow()
            }
            .onFailure { it ->
                testContext.failNow(it)
            }

    }

    @Test
    @Order(2)
    fun `it can open Account 2  with $0`(testContext: VertxTestContext) {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val streamQuery = StreamQuery(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", "2"),
            JsonObject().put("type", "AmountDeposited").put("amount", 0)
        )
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .compose {
                dumpEvents()
            }
            .compose {
                stateBuilder.buildFor(streamQuery)
            }
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
                testContext.verify {
                    assertEquals(4, sequence.value)
                    assertEquals("2", state.id)
                    assertEquals(0, state.balance)
                }
                testContext.completeNow()
            }
            .onFailure { it ->
                testContext.failNow(it)
            }
    }

    @Test
    @Order(3)
    fun `it can transfer $30 from Account 1 to Account 2 within the same db transaction`(testContext: VertxTestContext) {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1")),
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val streamQuery = StreamQuery(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(4))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 1).put("toAcct", 2).put("amount", 30)
        )
        eventsAppender.appendIf(eventsToAppend, appendCondition)
            .compose {
                dumpEvents()
            }
            .compose {
                // assert acct1 state
                val domainIdentifiersAcct1 = listOf(
                    DomainIdentifier(name = StateName("Account"), id = StateId("1"))
                )
                val streamQueryAcct1 = StreamQuery(identifiers = domainIdentifiersAcct1, eventTypes = eventTypes)
                stateBuilder.buildFor(streamQueryAcct1)
                    .onSuccess { (state, sequence) ->
                        testContext.verify {
                            assertEquals(5, sequence.value)
                            assertEquals("1", state.id)
                            assertEquals(70, state.balance)
                        }
                    }
            }
            .compose {
                // assert acct2 state
                val domainIdentifiersAcct2 = listOf(
                    DomainIdentifier(name = StateName("Account"), id = StateId("2"))
                )
                val streamQueryAcct2 = StreamQuery(identifiers = domainIdentifiersAcct2, eventTypes = eventTypes)
                stateBuilder.buildFor(streamQueryAcct2)
                    .onSuccess { (state, sequence) ->
                        testContext.verify {
                            assertEquals(5, sequence.value)
                            assertEquals("2", state.id)
                            assertEquals(30, state.balance)
                        }
                    }
            }
            .onSuccess {
                testContext.completeNow()
            }
            .onFailure {
                testContext.failNow(it)
            }
    }

    companion object {
        lateinit var eventsAppender: CrabletEventsAppender
        lateinit var stateBuilder: CrabletStateBuilder<Account>
        data class Account(val id: String? = null, val balance: Int = 0)
        val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }
        @BeforeAll
        @JvmStatic
        fun setUp(testContext: VertxTestContext) {
            eventsAppender = CrabletEventsAppender(pool)
            stateBuilder = CrabletStateBuilder(
                client = pool,
                initialState = Account(),
                evolveFunction = { state, event ->
                    when (event.getString("type")) {
                        "AccountOpened" -> state.copy(id = event.getString("id"))
                        "AmountDeposited" -> state.copy(balance = state.balance.plus(event.getInteger("amount")))
                        "AmountTransferred" -> {
                            when {
                                event.getString("fromAcct") == state.id -> state.copy(
                                    balance = state.balance.minus(
                                        event.getInteger("amount")
                                    )
                                )
                                event.getString("toAcct") == state.id -> state.copy(
                                    balance = state.balance.plus(
                                        event.getInteger(
                                            "amount"
                                        )
                                    )
                                )
                                else -> state
                            }
                        }
                        else -> state
                    }
                })
            cleanDatabase().onSuccess { testContext.completeNow() }
        }
    }

}