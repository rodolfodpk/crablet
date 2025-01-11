package crablet.query.impl

import crablet.query.IntervalConfig
import crablet.query.SubscriptionConfig
import io.vertx.core.Future
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

internal class SubscriptionVerticle(
    private val subscriptionConfig: SubscriptionConfig,
    private val subscriptionComponent: SubscriptionComponent,
    private val intervalConfig: IntervalConfig,
) : CoroutineVerticle() {
    private val name = "${SubscriptionVerticle::class.java.simpleName}-${subscriptionConfig.source.name}"
    private val logger = LoggerFactory.getLogger(name)

    private val greedy = AtomicBoolean(false)
    private val failures = AtomicLong(0L)
    private val backOff = AtomicLong(0L)
    private val isPaused = AtomicBoolean(false)
    private val isBusy = AtomicBoolean(false)
    private var lastSequenceId = 0L

    override suspend fun start() {
        logger.info("Starting subscription for {}", subscriptionConfig.source.name)
        vertx.setTimer(intervalConfig.initialInterval, handler)
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
            logger.trace("It is busy=$isBusy or paused=$isPaused. Will just reschedule.")
        }
        if (logger.isTraceEnabled) {
            logger.trace("justReschedule - Rescheduled to next {} milliseconds", intervalConfig.interval)
        }
    }

    private fun proceed(): Future<Void> {
        isBusy.set(true)
        if (logger.isTraceEnabled) {
            logger.trace(
                "Scanning for new events for subscription {}. Last sequence {}",
                subscriptionConfig.source.name,
                lastSequenceId,
            )
        }
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
        lastSequenceId = eventSequence
        failures.set(0)
        backOff.set(0)
        val nextInterval = if (greedy.get()) GREED_INTERVAL else intervalConfig.interval
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

    companion object {
        private val jmxBeanName: String = ManagementFactory.getRuntimeMXBean().name
        private const val GREED_INTERVAL = 100L
    }
}
