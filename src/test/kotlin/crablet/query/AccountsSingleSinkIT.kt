package crablet.query

import crablet.AbstractCrabletTest
import crablet.EventName
import crablet.SequenceNumber
import crablet.TestAccountDomain.evolveFunction
import crablet.TestAccountDomain.initialStateFunction
import crablet.TestRepository
import crablet.command.AppendCondition
import crablet.command.DomainIdentifier
import crablet.command.StateId
import crablet.command.StateName
import crablet.command.TransactionContext
import crablet.command.impl.CrabletEventsAppender
import crablet.command.impl.CrabletStateBuilder
import crablet.query.impl.CrabletSubscriptionsContainer
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AccountsSingleSinkIT : AbstractCrabletTest() {
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
            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
            sequence.value shouldBeExactly 4L

            val (state, seq) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            seq.value shouldBeExactly sequence.value
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
            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
            sequence.value shouldBeExactly 5L

            val (state1, seq1) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq1.value shouldBeExactly sequence.value
            state1.id shouldBe 1
            state1.balance shouldBeExactly 70

            val (state2, seq2) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )

            seq2.value shouldBeExactly sequence.value
            state2.id shouldBe 2
            state2.balance shouldBeExactly 30
        }

    @Test
    @Order(4)
    fun `event sink was called`() {
        latch.await(3, TimeUnit.SECONDS)
        verify(exactly = 5) { mockSingleEventSink.handle(any<JsonObject>()) }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AccountsSingleSinkIT::class.java)

        private lateinit var container: CrabletSubscriptionsContainer
        private lateinit var eventsAppender: CrabletEventsAppender
        private lateinit var stateBuilder: CrabletStateBuilder
        private lateinit var latch: CountDownLatch
        private lateinit var testRepository: TestRepository
        private lateinit var mockSingleEventSink: EventSink.SingleEventSink

        private val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }

        private val transactionContextAcct1 =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
                eventTypes = eventTypes,
            )

        private val transactionContextAcct2 =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
                eventTypes = eventTypes,
            )

        @BeforeAll
        @JvmStatic
        fun setUp() =
            runTest {
                container = CrabletSubscriptionsContainer(vertx = vertx, pool = pool)
                eventsAppender = CrabletEventsAppender(pool = pool)
                stateBuilder = CrabletStateBuilder(pool = pool)
                latch = CountDownLatch(1)
                testRepository = TestRepository(client = pool)
                mockSingleEventSink = mockk<EventSink.SingleEventSink>()
                every { mockSingleEventSink.handle(any<JsonObject>()) } returns Future.succeededFuture()
                cleanDatabase()

                val source = SubscriptionSource(name = "accounts-view", eventTypes = eventTypes)

                val callback: (name: String, list: List<JsonObject>) -> Unit = { name, list ->
                    logger.info("Call back called for {} with {} events", name, list.size)
                    latch.countDown()
                }

                val subscriptionConfig =
                    SubscriptionConfig(
                        source = source,
                        eventSink = mockSingleEventSink,
                        callback = callback,
                    )

                container.addSubscription(
                    subscriptionConfig = subscriptionConfig,
                    intervalConfig = IntervalConfig(initialInterval = 500, interval = 500),
                )
                container.deployAll()
            }
    }
}
