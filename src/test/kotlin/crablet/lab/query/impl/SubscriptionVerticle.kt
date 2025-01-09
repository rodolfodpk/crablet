package crablet.lab.query.impl

import crablet.lab.query.IntervalConfig
import crablet.lab.query.SubscriptionConfig
import io.vertx.core.Handler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class SubscriptionVerticle(
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
    private var currentOffset = 0L

    override suspend fun start() {
        super.start()
    }

    private fun run() {
        if (isBusy.get() || isPaused.get()) {
            justReschedule()
            return
        }
        isBusy.set(true)
        // TODO
    }

    private fun justReschedule() {
        vertx.setTimer(intervalConfig.interval, handler())
        if (logger.isTraceEnabled) {
            logger.trace("It is busy=$isBusy or paused=$isPaused. Will just reschedule.")
        }
        if (logger.isTraceEnabled) {
            logger.trace("justReschedule - Rescheduled to next {} milliseconds", intervalConfig.interval)
        }
    }

    private fun handler(): Handler<Long> =
        Handler<Long> {
            action()
        }

    private fun action() {
        if (logger.isTraceEnabled) {
            logger.trace("Scanning for new events for subscription {}", subscriptionConfig.source.name)
        }
//        try {
//            subscriptionComponent.scanPendingEvents(1000)
//        }
    }

    companion object {
        private val jmxBeanName: String = ManagementFactory.getRuntimeMXBean().name
        private const val GREED_INTERVAL = 100L
    }
}
