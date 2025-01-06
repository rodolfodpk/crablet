package crablet.lab.jsonvalues

import crablet.lab.jsonvalues.CustomerEvents.CUSTOMER_ACTIVATED
import crablet.lab.jsonvalues.CustomerEvents.CUSTOMER_DEACTIVATED
import crablet.lab.jsonvalues.CustomerEvents.CUSTOMER_REGISTERED
import crablet.lab.jsonvalues.CustomerEvents.CUSTOMER_RENAMED
import crablet.lab.jsonvalues.CustomerEventsFields.ID
import crablet.lab.jsonvalues.CustomerEventsFields.NAME
import crablet.lab.jsonvalues.CustomerEventsFields.REASON
import crablet.lab.jsonvalues.CustomerEventsFields.TYPE
import `fun`.gen.Gen

import io.vertx.core.json.JsonObject
import jsonvalues.JsObj
import jsonvalues.JsPath
import jsonvalues.JsStr
import jsonvalues.JsValue
import jsonvalues.spec.JsObjSpec
import jsonvalues.spec.JsSpec
import jsonvalues.spec.JsSpecs
import jsonvalues.spec.JsSpecs.cons
import jsonvalues.spec.JsSpecs.str
import jsonvalues.spec.SpecToGen
import jsonvalues.spec.SpecToJsonSchema

object CustomerEvents {
    const val CUSTOMER_REGISTERED = "CustomerRegistered"
    const val CUSTOMER_ACTIVATED = "CustomerActivated"
    const val CUSTOMER_DEACTIVATED = "CustomerDeactivated"
    const val CUSTOMER_RENAMED = "CustomerRenamed"
}

object CustomerEventsFields {
    const val TYPE = "type"
    const val ID = "id"
    const val NAME = "name"
    const val REASON = "reason"
}

fun getCustomerSpec(): JsSpec {
    val registeredEventSpec =
        JsObjSpec.of(
            TYPE, cons(JsStr.of(CUSTOMER_REGISTERED)),
            ID, str(),
            NAME, str()
        )

    val activatedEventSpec =
        JsObjSpec.of(
            TYPE, cons(JsStr.of(CUSTOMER_ACTIVATED)),
            REASON, str(),
        )

    val deactivatedEventSpec =
        JsObjSpec.of(
            TYPE, cons(JsStr.of(CUSTOMER_DEACTIVATED)),
            REASON, str(),
        )

    val renamedEventSpec =
        JsObjSpec.of(
            TYPE, cons(JsStr.of(CUSTOMER_RENAMED)),
            NAME, str(),
        )

    val spec: JsSpec = JsSpecs.oneSpecOf(
        JsSpecs.ofNamedSpec(CUSTOMER_REGISTERED, registeredEventSpec),
        JsSpecs.ofNamedSpec(CUSTOMER_ACTIVATED, activatedEventSpec),
        JsSpecs.ofNamedSpec(CUSTOMER_DEACTIVATED, deactivatedEventSpec),
        JsSpecs.ofNamedSpec(CUSTOMER_RENAMED, renamedEventSpec),
    )

    return spec
}

class CustomerRegistered(val map: Map<String, Any?>) {
    val id: String by map
    val name: String by map
}

fun main() {

    fun test1() {

        val event1 = CustomerRegistered(mapOf(ID to "1", NAME to "John Doe"))

        val event2 = CustomerRegistered(mapOf())

        println(event1.map)

        val js1 = JsonObject(event1.map)

        println(js1.encodePrettily())

    }

    fun test2() {

        val customerEventSpec = getCustomerSpec()

        val jsonSchema: JsObj = SpecToJsonSchema.convert(customerEventSpec)

        println("---------------")
        println(jsonSchema.toPrettyString())

        val customerGen: Gen<JsValue> = SpecToGen.DEFAULT.convert(customerEventSpec)

        val sample: MutableList<JsValue> = customerGen.sample(10).toList()

        println("---------------")
        sample.forEach {
            println(it.toJson().toPrettyString())
        }

        val event = sample.random()

        println("Customer event type " + event.toJson().getStr(JsPath.path("/type")))
        println("Customer event type " + event.toJson().getStr(JsPath.path("/type")))
        println("Customer event name " + event.toJson().getStr(JsPath.path("/name"))) // can be null

    }

    test2()

}
