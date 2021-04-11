package org.chicago.cta.producers.models

class Train constructor(val trainId: String, var status: Status = Status.OUT_OF_SERVICE) {
    enum class Status(val status: String) {
        OUT_OF_SERVICE("out_of_service"),
        IN_SERVICE("in_service"),
        BROKEN_DOWN("broken_down")
    }

    override fun toString(): String {
        return "Train ID $trainId is ${status.status.replace('_', ' ')}"
    }

    fun broken() : Boolean {
        return status == Status.BROKEN_DOWN
    }


}