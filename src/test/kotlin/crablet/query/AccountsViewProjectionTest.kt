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
import io.kotest.common.runBlocking
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AccountsViewProjectionTest : AbstractCrabletTest() {

    @AfterEach
    fun log(): Unit = runBlocking {
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
                    JsonObject().put("type", "AmountDeposited").put("id", 1).put("amount", 50).put("balance", 50),
                    JsonObject().put("type", "AmountDeposited").put("id", 1).put("amount", 50).put("balance", 100),
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
    fun `it can open Account 2 with $0`() =
        runTest {
            val appendCondition =
                AppendCondition(
                    transactionContext = transactionContextAcct2,
                    expectedCurrentSequence = SequenceNumber(0)
                )
            val eventsToAppend =
                listOf(
                    JsonObject().put("type", "AccountOpened").put("id", 2),
                )
            val sequence = eventsAppender.appendIf(eventsToAppend, appendCondition)
            sequence.value shouldBeExactly 4L

            val (state, seq) = stateBuilder.buildFor(
                transactionContext = transactionContextAcct2,
                initialStateFunction = initialStateFunction,
                onEventFunction = evolveFunction
            )
            seq.value shouldBeExactly sequence.value
            state.id shouldBe 2
            state.balance shouldBeExactly 0

            latch.await(10, TimeUnit.SECONDS)

            val accountsViewList = testRepository.getAllAccountView()

            accountsViewList.size shouldBeExactly 2
            accountsViewList[0].toString() shouldBe """{"id":1,"balance":100}"""
            accountsViewList[1].toString() shouldBe """{"id":2,"balance":0}"""

            val subscriptionsList = testRepository.getAllSubscriptions()
            subscriptionsList.size shouldBeExactly 1
            subscriptionsList[0].toString() shouldBe """{"name":"accounts-view","sequence_id":4}"""
        }


    companion object {
        private lateinit var container: CrabletSubscriptionsContainer
        private lateinit var eventsAppender: CrabletEventsAppender
        private lateinit var stateBuilder: CrabletStateBuilder
        private lateinit var latch: CountDownLatch
        private lateinit var testRepository: TestRepository

        private val eventTypes = listOf("AccountOpened", "AmountDeposited", "AmountTransferred").map { EventName(it) }

        private val transactionContextAcct1 =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("1"))),
                eventTypes = crablet.command.AccountTransferScenarioTest.eventTypes,
            )

        private val transactionContextAcct2 =
            TransactionContext(
                identifiers = listOf(DomainIdentifier(name = StateName("Account"), id = StateId("2"))),
                eventTypes = eventTypes,
            )

        @BeforeAll
        @JvmStatic
        fun setUp() = runBlocking {
            container = CrabletSubscriptionsContainer(vertx = Vertx.vertx(), client = pool)
            eventsAppender = CrabletEventsAppender(client = pool)
            stateBuilder = CrabletStateBuilder(client = pool)
            latch = CountDownLatch(1)
            testRepository = TestRepository(client = pool)

            cleanDatabase()

            val source = SubscriptionSource(name = "accounts-view", eventTypes = eventTypes)

            class AccountsViewProjector : EventViewProjector {
                override fun project(sqlConnection: SqlConnection, eventAsJson: JsonObject): Future<Void> {
                    val eventPayload = eventAsJson.getJsonObject("event_payload")
                    return when (eventPayload.getString("type")) {
                        "AccountOpened", "AmountDeposited" -> {
                            val id = eventPayload.getInteger("id")
                            val balance = eventPayload.getInteger("balance", 0)
                            val upsertQuery =
                                "INSERT INTO accounts_view(id, balance) VALUES($1, $2) ON CONFLICT (id) DO UPDATE SET balance = $2"
                            sqlConnection.preparedQuery(upsertQuery)
                                .execute(Tuple.of(id, balance))
                                .mapEmpty()
                        }

                        "AmountTransferred" -> TODO()
                        else -> Future.succeededFuture()
                    }
                }

            }

            val subscriptionConfig = SubscriptionConfig(
                source = source,
                eventViewProjector = AccountsViewProjector(),
                callback = { latch.countDown() })
            container.addSubscription(
                subscriptionConfig = subscriptionConfig,
                intervalConfig = IntervalConfig(initialInterval = 1000, interval = 100)
            )
            container.deployAll()
        }
    }
}

