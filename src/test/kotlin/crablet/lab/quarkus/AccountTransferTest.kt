//package crablet.quarkus
//
//import crablet.AppendCondition
//import crablet.DomainIdentifier
//import crablet.EventName
//import crablet.EventsAppender
//import crablet.SequenceNumber
//import crablet.StateId
//import crablet.StateName
//import crablet.TransactionContext
//import crablet.postgres.AbstractCrabletTest
//import crablet.postgres.AccountTransferScenarioTest
//import crablet.postgres.AccountTransferScenarioTest.Companion
//import crablet.postgres.AccountTransferScenarioTest.Companion.Account
//import crablet.postgres.AccountTransferScenarioTest.Companion.eventsAppender
//import crablet.postgres.AccountTransferScenarioTest.Companion.stateBuilder
//import crablet.postgres.CrabletEventsAppender
//import crablet.postgres.CrabletStateBuilder
//import io.kotest.matchers.ints.shouldBeExactly
//import io.kotest.matchers.longs.shouldBeExactly
//import io.kotest.matchers.shouldBe
//import io.micrometer.core.instrument.MeterRegistry
//import io.quarkus.test.junit.QuarkusTest
//import io.smallrye.mutiny.Uni
//import io.smallrye.mutiny.vertx.AsyncResultUni
//import io.smallrye.mutiny.vertx.UniHelper.toUni
//import io.vertx.core.Future
//import io.vertx.core.json.JsonObject
//import io.vertx.junit5.VertxTestContext
//import jakarta.inject.Inject
//import org.junit.jupiter.api.Assertions.assertEquals
//import org.junit.jupiter.api.BeforeAll
//import org.junit.jupiter.api.BeforeEach
//import org.junit.jupiter.api.MethodOrderer
//import org.junit.jupiter.api.Order
//import org.junit.jupiter.api.Test
//import org.junit.jupiter.api.TestMethodOrder
//
//@QuarkusTest
//@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
//class AccountTransferTest : AbstractCrabletTest() {
//
//    @Inject
//    lateinit var registry: MeterRegistry
//
//    @BeforeEach
//    fun setUp() {
//        eventsAppender = CrabletDecoratedEventsAppender(CrabletEventsAppender(pool), meterRegistry = registry)
//        stateBuilder = CrabletStateBuilder(
//            client = pool,
//            initialState = Account(),
//            evolveFunction = evolveFunction
//        )
//    }
//
//    @Test
//    @Order(1)
//    fun `it can open Account 1 with $100`() {
//        val streamQuery = TransactionContext(
//            identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
//            eventTypes = eventTypes
//        )
//        val appendCondition = AppendCondition(query = streamQuery, maximumEventSequence = SequenceNumber(0))
//        val eventsToAppend = listOf(
//            JsonObject().put("type", "AccountOpened").put("id", 1),
//            JsonObject().put("type", "AmountDeposited").put("amount", 100)
//        )
//        eventsAppender.appendIf(eventsToAppend, appendCondition)
//            .compose {
//                dumpEvents()
//            }
//            .compose {
//                stateBuilder.buildFor(streamQuery)
//            }
//            .onSuccess { (state, sequence): Pair<Account, SequenceNumber> ->
//                    sequence.value shouldBeExactly 2888L
//                    state.id shouldBe 1
//                    state.balance shouldBeExactly 100
//                }
//            }
//
//    companion object {
//        lateinit var eventsAppender: EventsAppender
//        lateinit var stateBuilder: CrabletStateBuilder<Account>
//
//        data class Account(val id: Int? = null, val balance: Int = 0)
//
//        val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }
//
//        private val evolveFunction: (Account, JsonObject) -> Account = { state, event ->
//            when (event.getString("type")) {
//                "AccountOpened" -> state.copy(id = event.getInteger("id"))
//                "AmountDeposited" -> state.copy(balance = state.balance.plus(event.getInteger("amount")))
//                "AmountTransferred" -> {
//                    when {
//                        event.getInteger("fromAcct") == state.id -> state.copy(
//                            balance = state.balance.minus(event.getInteger("amount"))
//                        )
//
//                        event.getInteger("toAcct") == state.id -> state.copy(
//                            balance = state.balance.plus(event.getInteger("amount"))
//                        )
//
//                        else -> state
//                    }
//                }
//
//                else -> state
//            }
//        }
//
//        @BeforeAll
//        @JvmStatic
//        fun setUpAll() {
//            toUni(cleanDatabase()).await()
//        }
//    }
//
//}