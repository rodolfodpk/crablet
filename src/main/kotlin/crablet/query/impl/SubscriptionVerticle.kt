package crablet.query.impl

import crablet.query.IntervalConfig
import crablet.query.SubscriptionCommand
import crablet.query.SubscriptionCommand.PAUSE
import crablet.query.SubscriptionCommand.RESUME
import crablet.query.SubscriptionCommand.SHOW_STATUS
import crablet.query.SubscriptionCommand.TRY_PERFORM_NOW
import crablet.query.SubscriptionConfig
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

internal class SubscriptionVerticle(
    private val subscriptionConfig: SubscriptionConfig,
    private val subscriptionComponent: SubscriptionComponent,
    private val intervalConfig: IntervalConfig,
) : AbstractVerticle() {
    private val greedy = AtomicBoolean(false)
    private val failures = AtomicLong(0L)
    private val backOff = AtomicLong(0L)
    private val isPaused = AtomicBoolean(false)
    private val isBusy = AtomicBoolean(false)
    private var lastSequenceId = AtomicLong(0L)

    override fun start() {
        logger.info("Starting subscription for {}", subscriptionConfig.name())
        vertx.setTimer(intervalConfig.initialInterval, handler)
        val eventBus = vertx.eventBus()

        val endpointAddress = "${subscriptionConfig.name()}@subscriptions"
        val consumer = eventBus.localConsumer<SubscriptionCommand>(endpointAddress)
        consumer.handler { message: Message<SubscriptionCommand> ->
            val command = message.body()
            logger.info("Endpoint {} received command {}", endpointAddress, command)
            try {
                commandHandler(command)
                message.reply(currentStatus())
            } catch (e: Exception) {
                message.fail(0, "Error on command ${command.name}")
            }
        }
    }

    private fun commandHandler(command: SubscriptionCommand) {
        when (command) {
            TRY_PERFORM_NOW -> handler.invoke(0)
            PAUSE -> isPaused.set(true)
            RESUME -> isPaused.set(false)
            SHOW_STATUS -> { /* just return status */ }
        }
    }

    private val handler: (Long) -> Unit = { timerId ->
        if (logger.isTraceEnabled) {
            logger.trace("Timer $timerId has been fired!")
        }
        if (isBusy.get() || isPaused.get()) {
            justReschedule()
        } else {
            proceed()
        }
    }

    private fun justReschedule() {
        vertx.setTimer(intervalConfig.interval, handler)
        if (logger.isTraceEnabled) {
            logger.trace("justReschedule - Rescheduled to next {} milliseconds", intervalConfig.interval)
        }
    }

    private fun proceed(): Future<Void> {
        isBusy.set(true)
        logger.info(
            "Scanning for new events for subscription {}. Last sequence {}",
            subscriptionConfig.name(),
            lastSequenceId.get(),
        )
        return subscriptionComponent
            .handlePendingEvents(subscriptionConfig)
            .onSuccess { (sequenceId, howManyNewEvents) ->
                if (howManyNewEvents == 0) {
                    registerNoNewEvents()
                } else {
                    registerSuccess(sequenceId)
                }
            }.onFailure {
                registerFailure(it)
            }.mapEmpty()
    }

    private fun registerNoNewEvents() {
        greedy.set(false)
        val jitter = intervalConfig.jitterFunction.invoke()
        val nextInterval = min(intervalConfig.maxInterval, intervalConfig.interval * backOff.incrementAndGet() + jitter)
        vertx.setTimer(nextInterval, handler)
        if (logger.isTraceEnabled) {
            logger.trace(
                "registerNoNewEvents - Rescheduled to next {} milliseconds",
                nextInterval,
            )
        }
    }

    private fun registerSuccess(eventSequence: Long) {
        logger.info("Will update lastSequenceId to {}", eventSequence)
        lastSequenceId.set(eventSequence)
        failures.set(0)
        backOff.set(0)
        val nextInterval = if (greedy.get()) greedInterval().invoke() else intervalConfig.interval
        vertx.setTimer(nextInterval, handler)
        if (logger.isTraceEnabled) logger.trace("registerSuccess - Rescheduled to next {} milliseconds", nextInterval)
    }

    private fun registerFailure(throwable: Throwable) {
        greedy.set(false)
        val jitter = intervalConfig.jitterFunction.invoke()
        val nextInterval =
            min(intervalConfig.maxInterval, (intervalConfig.interval * failures.incrementAndGet()) + jitter)
        vertx.setTimer(nextInterval, handler)
        logger.error("registerFailure - Rescheduled to next {} milliseconds", nextInterval, throwable)
    }

    private fun currentStatus(): JsonObject =
        JsonObject()
            .put("subscriptionName", subscriptionConfig.name())
            .put("paused", isPaused.get())
            .put("busy", isBusy.get())
            .put("greedy", greedy.get())
            .put("failures", failures.get())
            .put("backOff", backOff.get())
            .put("currentOffset", lastSequenceId.get())

    companion object {
        private val logger = LoggerFactory.getLogger(SubscriptionVerticle::class.java)

        private fun greedInterval(): () -> Long = { (1..7).random() * 100L }
    }
}
