package crablet.jsonvalues

import crablet.EventsValidator
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import jsonvalues.spec.JsObjSpecParser

class CrabletEventsValidator(private val parser: JsObjSpecParser) : EventsValidator {

    override fun validate(events: List<JsonObject>) {
        parser.parse(JsonArray(events).toBuffer().bytes)
    }

}

