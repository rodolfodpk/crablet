package crablet.command

import crablet.TestAccountDomain.Account
import io.vertx.core.json.JsonObject

class AccountTransferCommandHandler {
    data class TransferCommand(
        val fromAccountId: Int,
        val toAccountId: Int,
        val amount: Int
    )

    sealed class TransferError {
        data object SameAccountTransfer : TransferError()
        data object NonPositiveAmount : TransferError()
        data object InsufficientFunds : TransferError()
        data object AccountNotFound : TransferError()
    }

    fun handle(command: TransferCommand, fromAccount: Account?, toAccount: Account?): Result<JsonObject> {
        // Validate accounts exist
        if (fromAccount?.id == null || toAccount?.id == null) {
            return Result.failure(IllegalStateException(TransferError.AccountNotFound.toString()))
        }

        // Validate not same account
        if (command.fromAccountId == command.toAccountId) {
            return Result.failure(IllegalArgumentException(TransferError.SameAccountTransfer.toString()))
        }

        // Validate amount is positive
        if (command.amount <= 0) {
            return Result.failure(IllegalArgumentException(TransferError.NonPositiveAmount.toString()))
        }

        // Validate sufficient funds
        if (fromAccount.balance < command.amount) {
            return Result.failure(IllegalStateException(TransferError.InsufficientFunds.toString()))
        }

        // Calculate new balances
        val newFromBalance = fromAccount.balance - command.amount
        val newToBalance = toAccount.balance + command.amount

        // Create transfer event
        val transferEvent = JsonObject()
            .put("type", "AmountTransferred")
            .put("fromAcct", command.fromAccountId)
            .put("toAcct", command.toAccountId)
            .put("amount", command.amount)
            .put("fromAcctBalance", newFromBalance)
            .put("toAcctBalance", newToBalance)

        return Result.success(transferEvent)
    }
} 