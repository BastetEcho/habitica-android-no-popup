package com.habitrpg.android.habitica.data.sync

import com.google.gson.JsonParser

/** Projects local ordering without changing the queryable collection consumed by task filters. */
object TodoTaskOrdering {
    const val LOCAL_ORDER = "_localOrder"

    /** Moves one identity in a complete order and gives every remaining task a unique position. */
    fun move(order: List<String>, taskId: String, position: Int): List<String> =
        order.distinct().filterNot { it == taskId }.toMutableList().apply {
            add(position.coerceIn(0, size), taskId)
        }

    /** Reapplies pending operations to active Todo order, matching server score/undo membership. */
    fun project(
        currentOrder: List<String>,
        operations: List<TodoOperation>,
        resolve: (String) -> String = { it },
    ): List<String> {
        var order = currentOrder.map(resolve).distinct()
        val available = order.toMutableSet()
        operations.filter { it.type == TodoOperationType.CREATE }.forEach { available.add(resolve(it.taskId)) }
        operations.sortedBy { it.sequence }.forEach { operation ->
            val taskId = resolve(operation.taskId)
            when (operation.type) {
                TodoOperationType.CREATE -> order = move(order, taskId, 0)
                TodoOperationType.MOVE -> {
                    val payload = JsonParser.parseString(operation.payload).asJsonObject
                    val saved = payload.getAsJsonArray(LOCAL_ORDER)?.map { resolve(it.asString) }
                        ?.distinct()?.filter { it in available }
                    order = if (saved == null) {
                        if (taskId in available) move(order, taskId, payload.get("position").asInt) else order
                    } else {
                        // Server-side arrivals absent from the saved order remain visible once.
                        saved + order.filterNot { it in saved }
                    }
                }
                TodoOperationType.DELETE -> {
                    order = order.filterNot { it == taskId }
                    available.remove(taskId)
                }
                TodoOperationType.SCORE -> {
                    val up = JsonParser.parseString(operation.payload).asJsonObject.get("up").asBoolean
                    if (up) {
                        order = order.filterNot { it == taskId }
                        available.remove(taskId)
                    } else {
                        // Habitica appends an undone Todo to tasksOrder.todos, rather than restoring its old index.
                        available.add(taskId)
                        order = move(order, taskId, order.size)
                    }
                }
                else -> Unit
            }
        }
        return order
    }
}
