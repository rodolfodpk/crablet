package crablet.postgres

import crablet.AppendCondition
import crablet.DomainIdentifier
import crablet.EventName
import crablet.SequenceNumber
import crablet.StateId
import crablet.StateName
import crablet.StreamQuery
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
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
class AccountTransferScenarioTest : AbstractCrabletTest() {

    @Test
    @Order(1)
    fun `it can open Account 1 with $100`(testContext: VertxTestContext) {
        val streamQuery = StreamQuery(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 1),
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
                    sequence.value shouldBeExactly 2L
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
    fun `it can open Account 2  with $0`(testContext: VertxTestContext) {
        val streamQuery = StreamQuery(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AccountOpened").put("id", 2)
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
                    sequence.value shouldBeExactly 3L
                    state.id shouldBe 2
                    state.balance shouldBeExactly 0
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
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(3))
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
                            sequence.value shouldBeExactly 4L
                            state.id shouldBe 1
                            state.balance shouldBeExactly 70
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
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
                            sequence.value shouldBeExactly 4L
                            state.id shouldBe 2
                            state.balance shouldBeExactly 30
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
                    }
            }
            .onSuccess {
                testContext.completeNow()
            }
            .onFailure {
                testContext.failNow(it)
            }
    }

    @Test
    @Order(4)
    fun `it can transfer $10 from Account 2 to Account 1 within the same db transaction`(testContext: VertxTestContext) {
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
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 2).put("toAcct", 1).put("amount", 10)
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
                            sequence.value shouldBeExactly 5L
                            state.id shouldBe 1
                            state.balance shouldBeExactly 80
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
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
                            sequence.value shouldBeExactly 5L
                            state.id shouldBe 2
                            state.balance shouldBeExactly 20
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
                    }
            }
            .onSuccess {
                testContext.completeNow()
            }
            .onFailure {
                testContext.failNow(it)
            }
    }

    @Test
    @Order(5)
    fun `it can transfer $1 from Account 2 to Account 1 within the same db transaction`(testContext: VertxTestContext) {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1")),
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val streamQuery = StreamQuery(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(5))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 2).put("toAcct", 1).put("amount", 1)
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
                            sequence.value shouldBeExactly 6L
                            state.id shouldBe 1
                            state.balance shouldBeExactly 81
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
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
                            sequence.value shouldBeExactly 6L
                            state.id shouldBe 2
                            state.balance shouldBeExactly 19
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
                    }
            }
            .onSuccess {
                testContext.completeNow()
            }
            .onFailure {
                testContext.failNow(it)
            }
    }

    @Test
    @Order(6)
    fun `it can transfer $1 from Account 1 to Account 2 within the same db transaction`(testContext: VertxTestContext) {
        val domainIdentifiers = listOf(
            DomainIdentifier(name = StateName("Account"), id = StateId("1")),
            DomainIdentifier(name = StateName("Account"), id = StateId("2"))
        )
        val streamQuery = StreamQuery(
            identifiers = domainIdentifiers,
            eventTypes = eventTypes
        )
        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(6))
        val eventsToAppend = listOf(
            JsonObject().put("type", "AmountTransferred").put("fromAcct", 1).put("toAcct", 2).put("amount", 1)
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
                            sequence.value shouldBeExactly 7L
                            state.id shouldBe 1
                            state.balance shouldBeExactly 80
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
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
                            sequence.value shouldBeExactly 7L
                            state.id shouldBe 2
                            state.balance shouldBeExactly 20
                        }
                    }
                    .onFailure {
                        testContext.failNow(it)
                    }
            }
            .onSuccess {
                testContext.completeNow()
            }
            .onFailure {
                testContext.failNow(it)
            }
    }

    @Test
    @Order(7)
    fun `Account 1 state is correct`(testContext: VertxTestContext) {
        val streamQuery = StreamQuery(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
            eventTypes = eventTypes
        )
        stateBuilder.buildFor(streamQuery)
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
                testContext.verify {
                    sequence.value shouldBeExactly 7L
                    state.id shouldBe 1
                    state.balance shouldBeExactly 80
                }
                testContext.completeNow()
            }
            .onFailure { it ->
                testContext.failNow(it)
            }
    }

    @Test
    @Order(8)
    fun `Account 2 state is correct`(testContext: VertxTestContext) {
        val streamQuery = StreamQuery(
            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
            eventTypes = eventTypes
        )
        stateBuilder.buildFor(streamQuery)
            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
                testContext.verify {
                    sequence.value shouldBeExactly 7L
                    state.id shouldBe 2
                    state.balance shouldBeExactly 20
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