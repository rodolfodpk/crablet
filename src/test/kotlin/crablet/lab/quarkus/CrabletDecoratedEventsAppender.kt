//package crablet.quarkus;

//import crablet.AppendCondition
//import crablet.EventsAppender
//import crablet.SequenceNumber
//import io.micrometer.core.instrument.MeterRegistry
//import io.vertx.core.Future
//import io.vertx.core.json.JsonObject
//import org.eclipse.microprofile.faulttolerance.CircuitBreaker
//import org.eclipse.microprofile.faulttolerance.Retry
//import io.micrometer.core.instrument.Timer
//import io.smallrye.mutiny.Uni

//class CrabletDecoratedEventsAppender(
//    private val delegate: EventsAppender,
//    private val meterRegistry: MeterRegistry
//) : EventsAppender {
//    private val appendTimer: Timer = Timer
//        .builder("append.timer")
//        .description("The time taken by the appendIf function")
//        .register(this.meterRegistry)
//
//    @Retry(maxRetries = 2, jitter = 200L)
//    @CircuitBreaker(requestVolumeThreshold = 10, failureRatio = 0.5, delay = 500L, successThreshold = 5)
//    override fun appendIf(events: List<JsonObject>, appendCondition: AppendCondition): Future<SequenceNumber> {
//        return appendTimer.recordCallable {
////            val x =  Uni.createFrom().item({ delegate.appendIf(events, appendCondition) })
////            return x
//        }
//    }
//}