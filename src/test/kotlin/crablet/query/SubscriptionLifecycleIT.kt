package crablet.query

import crablet.AbstractCrabletTest
import crablet.SequenceNumber
import crablet.TestAccountDomain.evolveFunction
import crablet.TestAccountDomain.initialStateFunction
import crablet.TestAccountsContext
import crablet.command.AppendCondition
import crablet.command.impl.CrabletEventsAppender
import crablet.command.impl.CrabletStateBuilder
import crablet.query.impl.CrabletSubscriptionsContainer
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SubscriptionLifecycleIT :
    AbstractCrabletTest(),
    TestAccountsContext {
    @Test
    @Order(0)
    fun `when starting subscription`() =
        runTest {
            testRepository.cleanDatabase()
            testRepository.dumpEvents()

            container = CrabletSubscriptionsContainer(vertx = vertx, pool = pool)
            eventsAppender = CrabletEventsAppender(pool = pool)
            stateBuilder = CrabletStateBuilder(pool = pool)
            latch = CountDownLatch(1)
            mockSingleEventSink = mockk<EventSink.SingleEventSink>()
            every { mockSingleEventSink.handle(any<JsonObject>()) } returns Future.succeededFuture()

            val callback: (name: String, list: List<JsonObject>) -> Unit = { name, list ->
                logger.info("Call back called for {} with {} events", name, list.size)
                if (list.isNotEmpty()) {
                    latch.countDown()
                }
            }

            val subscriptionConfig =
                SubscriptionConfig(
                    source = source,
                    eventSink = mockSingleEventSink,
                    callback = callback,
                )

            container.addSubscription(
                subscriptionConfig = subscriptionConfig,
                intervalConfig = IntervalConfig(initialInterval = 10_000, interval = 10_000),
            )
            container.deployAll()
        }

    @Test
    @Order(1)
    fun `the initial status is correct`() =
        runTest {
            val actualStatus =
                container.submitSubscriptionCommand(
                    subscriptionName = source.name,
                    command = SubscriptionCommand.SHOW_STATUS,
                )
            actualStatus.getString("subscriptionName") shouldBe "accounts-view"
            actualStatus.getBoolean("paused") shouldBe false
            actualStatus.getBoolean("busy").shouldNotBeNull()
            actualStatus.getBoolean("greedy") shouldBe false
            actualStatus.getInteger("failures") shouldBe 0
            actualStatus.getInteger("backOff") shouldBe 0
            actualStatus.getInteger("currentOffset") shouldBe 0
        }

    @Test
    @Order(2)
    fun `when pausing it, the status is paused`() =
        runTest {
            val actualStatus = container.submitSubscriptionCommand(subscriptionName = source.name, command = SubscriptionCommand.PAUSE)
            actualStatus.getString("subscriptionName") shouldBe "accounts-view"
            actualStatus.getBoolean("paused") shouldBe true
            actualStatus.getBoolean("busy").shouldNotBeNull()
            actualStatus.getBoolean("greedy") shouldBe false
            actualStatus.getInteger("failures") shouldBe 0
            actualStatus.getInteger("backOff") shouldBe 0
            actualStatus.getInteger("currentOffset") shouldBe 0
        }

    @Test
    @Order(3)
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
            sequence.value shouldBeExactly 1L

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
    @Order(4)
    fun `when trying to perform`() =
        runTest {
            container.submitSubscriptionCommand(subscriptionName = source.name, command = SubscriptionCommand.TRY_PERFORM_NOW)
        }

    @Test
    @Order(5)
    fun `then the view model and subscriptions are correct`() {
        vertx.executeBlocking {
            val accountsViewList = testRepository.getAllAccountView().await()
            accountsViewList.size shouldBeExactly 0

            val subscriptionsList = testRepository.getAllSubscriptions().await()
            subscriptionsList.size shouldBeExactly 1
            subscriptionsList[0].toString() shouldBe """{"name":"accounts-view","sequence_id":0}"""
        }
    }

    @Test
    @Order(6)
    fun `when resuming it, the status is NOT paused`() =
        runTest {
            val actualStatus = container.submitSubscriptionCommand(subscriptionName = source.name, command = SubscriptionCommand.RESUME)
            actualStatus.getString("subscriptionName") shouldBe "accounts-view"
            actualStatus.getBoolean("paused") shouldBe false
            actualStatus.getBoolean("busy").shouldNotBeNull()
            actualStatus.getBoolean("greedy") shouldBe false
            actualStatus.getInteger("failures") shouldBe 0
            actualStatus.getInteger("backOff").shouldNotBeNull()
            actualStatus.getInteger("currentOffset") shouldBe 0
        }

    @Test
    @Order(7)
    fun `when trying to perform again`() =
        runTest {
            container.submitSubscriptionCommand(subscriptionName = source.name, command = SubscriptionCommand.TRY_PERFORM_NOW)
        }

    @Test
    @Order(8)
    fun `the the status is correct`() =
        runTest {
            Thread.sleep(100)
            val actualStatus =
                container.submitSubscriptionCommand(
                    subscriptionName = source.name,
                    command = SubscriptionCommand.SHOW_STATUS,
                )
            actualStatus.getString("subscriptionName") shouldBe "accounts-view"
            actualStatus.getBoolean("paused") shouldBe false
            actualStatus.getBoolean("busy").shouldNotBeNull()
            actualStatus.getBoolean("greedy") shouldBe false
            actualStatus.getInteger("failures") shouldBe 0
            actualStatus.getInteger("backOff").shouldNotBeNull()
            actualStatus.getInteger("currentOffset") shouldBe 1
        }

    @Test
    @Order(9)
    fun `then the view model and subscriptions are updated`() {
        vertx.executeBlocking {
            latch.await(7, TimeUnit.SECONDS)

            val accountsViewList = testRepository.getAllAccountView().await()
            accountsViewList.size shouldBeExactly 1
            accountsViewList[0].toString() shouldBe """{"id":1,"balance":0}"""

            val subscriptionsList = testRepository.getAllSubscriptions().await()
            subscriptionsList.size shouldBeExactly 1
            subscriptionsList[0].toString() shouldBe """{"name":"accounts-view","sequence_id":1}"""
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SubscriptionLifecycleIT::class.java)

        private lateinit var container: CrabletSubscriptionsContainer
        private lateinit var eventsAppender: CrabletEventsAppender
        private lateinit var stateBuilder: CrabletStateBuilder
        private lateinit var latch: CountDownLatch
        private lateinit var mockSingleEventSink: EventSink.SingleEventSink
    }
}
