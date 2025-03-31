package crablet.command

import crablet.AbstractCrabletTest
import crablet.SequenceNumber
import crablet.TestAccountDomain.Account
import crablet.TestAccountDomain.evolveFunction
import crablet.TestAccountDomain.initialStateFunction
import crablet.TestAccountsContext
import crablet.command.impl.CrabletEventsAppender
import crablet.command.impl.CrabletStateBuilder
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.MethodOrderer

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class AccountTransferErrorsScenarioIT :
    AbstractCrabletTest(),
    TestAccountsContext {

    private val commandHandler = AccountTransferCommandHandler()
    private lateinit var account1: Account
    private lateinit var account2: Account
    private lateinit var eventsAppender: CrabletEventsAppender
    private lateinit var stateBuilder: CrabletStateBuilder

    @BeforeEach
    fun setup() = runTest {
        eventsAppender = CrabletEventsAppender(pool = pool)
        stateBuilder = CrabletStateBuilder(pool = pool)
        testRepository.cleanDatabase()

        // Setup Account 1 with $100
        val appendCondition1 =
            AppendCondition(
                transactionContext = transactionContextAcct1,
                expectedCurrentSequence = SequenceNumber(0),
            )
        val eventsToAppend1 =
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
        eventsAppender.appendIf(eventsToAppend1, appendCondition1)

        // Setup Account 2 with $0
        val appendCondition2 =
            AppendCondition(
                transactionContext = transactionContextAcct2,
                expectedCurrentSequence = SequenceNumber(0),
            )
        val eventsToAppend2 =
            listOf(
                JsonObject().put("type", "AccountOpened").put("id", 2),
            )
        eventsAppender.appendIf(eventsToAppend2, appendCondition2)

        // Get account states for later use
        val (state1, _) =
            stateBuilder.buildFor(
                transactionContext = transactionContextAcct1,
                initialStateFunction = initialStateFunction,
                onEventFunction = evolveFunction,
            )
        account1 = state1
        state1.balance shouldBeExactly 100

        val (state2, _) =
            stateBuilder.buildFor(
                transactionContext = transactionContextAcct2,
                initialStateFunction = initialStateFunction,
                onEventFunction = evolveFunction,
            )
        account2 = state2
        state2.balance shouldBeExactly 0
    }

    @Test
    @Order(1)
    fun `it should fail when trying to transfer more than available balance`() =
        runTest {
            val command = AccountTransferCommandHandler.TransferCommand(
                fromAccountId = 2,
                toAccountId = 1,
                amount = 100  // Account 2 only has 0
            )

            val result = commandHandler.handle(command, account2, account1)
            result.isFailure shouldBe true
            assertThrows(IllegalStateException::class.java) {
                result.getOrThrow()
            }.apply {
                message shouldBe AccountTransferCommandHandler.TransferError.InsufficientFunds.toString()
            }

            // Verify account balances remain unchanged
            val (state1, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state1.balance shouldBeExactly 100

            val (state2, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state2.balance shouldBeExactly 0
        }

    @Test
    @Order(2)
    fun `it should fail when trying to transfer to non-existent account`() =
        runTest {
            val command = AccountTransferCommandHandler.TransferCommand(
                fromAccountId = 1,
                toAccountId = 999,
                amount = 10
            )

            val result = commandHandler.handle(command, account1, null)
            result.isFailure shouldBe true
            assertThrows(IllegalStateException::class.java) {
                result.getOrThrow()
            }.apply {
                message shouldBe AccountTransferCommandHandler.TransferError.AccountNotFound.toString()
            }

            // Verify source account balance remains unchanged
            val (state1, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state1.balance shouldBeExactly 100
        }

    @Test
    @Order(3)
    fun `it should fail when trying to transfer zero amount`() =
        runTest {
            val command = AccountTransferCommandHandler.TransferCommand(
                fromAccountId = 1,
                toAccountId = 2,
                amount = 0
            )

            val result = commandHandler.handle(command, account1, account2)
            result.isFailure shouldBe true
            assertThrows(IllegalArgumentException::class.java) {
                result.getOrThrow()
            }.apply {
                message shouldBe AccountTransferCommandHandler.TransferError.NonPositiveAmount.toString()
            }

            // Verify balances remain unchanged
            val (state1, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state1.balance shouldBeExactly 100

            val (state2, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state2.balance shouldBeExactly 0
        }

    @Test
    @Order(4)
    fun `it should fail when trying to transfer to same account`() =
        runTest {
            val command = AccountTransferCommandHandler.TransferCommand(
                fromAccountId = 1,
                toAccountId = 1,
                amount = 10
            )

            val result = commandHandler.handle(command, account1, account1)
            result.isFailure shouldBe true
            assertThrows(IllegalArgumentException::class.java) {
                result.getOrThrow()
            }.apply {
                message shouldBe AccountTransferCommandHandler.TransferError.SameAccountTransfer.toString()
            }

            // Verify balance remains unchanged
            val (state1, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state1.balance shouldBeExactly 100
        }

    @Test
    @Order(5)
    fun `it should fail when trying to transfer negative amount`() =
        runTest {
            val command = AccountTransferCommandHandler.TransferCommand(
                fromAccountId = 1,
                toAccountId = 2,
                amount = -10
            )

            val result = commandHandler.handle(command, account1, account2)
            result.isFailure shouldBe true
            assertThrows(IllegalArgumentException::class.java) {
                result.getOrThrow()
            }.apply {
                message shouldBe AccountTransferCommandHandler.TransferError.NonPositiveAmount.toString()
            }

            // Verify balances remain unchanged
            val (state1, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct1,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state1.balance shouldBeExactly 100

            val (state2, _) =
                stateBuilder.buildFor(
                    transactionContext = transactionContextAcct2,
                    initialStateFunction = initialStateFunction,
                    onEventFunction = evolveFunction,
                )
            state2.balance shouldBeExactly 0
        }
}