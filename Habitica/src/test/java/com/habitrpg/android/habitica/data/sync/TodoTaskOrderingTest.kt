package com.habitrpg.android.habitica.data.sync

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe

/** Tests durable ordering independently of Realm, network requests, and task-list filters. */
class TodoTaskOrderingTest : WordSpec({
    "personal Todo ordering" should {
        "shift every adjacent task when moving the first task to the bottom" {
            TodoTaskOrdering.move(listOf("a", "b", "c"), "a", 2) shouldBe listOf("b", "c", "a")
        }

        "restore the saved full order after refresh returns the previous server order" {
            val move = orderOperation(1, "a", TodoOperationType.MOVE, 2, listOf("b", "c", "a"))

            TodoTaskOrdering.project(listOf("a", "b", "c"), listOf(move)) shouldBe listOf("b", "c", "a")
            TodoTaskOrdering.project(listOf("b", "a", "c"), listOf(move)) shouldBe listOf("b", "c", "a")
        }

        "preserve multiple moves in sequence and remain stable when projections are reapplied" {
            val moves = listOf(
                orderOperation(1, "a", TodoOperationType.MOVE, 2, listOf("b", "c", "a")),
                orderOperation(2, "c", TodoOperationType.MOVE, 0, listOf("c", "b", "a")),
            )
            val projected = TodoTaskOrdering.project(listOf("a", "b", "c"), moves)

            projected shouldBe listOf("c", "b", "a")
            TodoTaskOrdering.project(projected, moves) shouldBe projected
            TodoTaskOrdering.project(listOf("b", "c", "a"), moves.drop(1)) shouldBe projected
        }

        "retain newly created tasks at the top in creation order through refresh" {
            val creates = listOf(
                orderOperation(1, "first", TodoOperationType.CREATE),
                orderOperation(2, "second", TodoOperationType.CREATE),
            )

            TodoTaskOrdering.project(listOf("a", "b", "first", "second"), creates) shouldBe
                listOf("second", "first", "a", "b")
            TodoTaskOrdering.project(listOf("second", "first", "a", "b"), creates) shouldBe
                listOf("second", "first", "a", "b")
        }

        "preserve a pending create followed by a move and a later create" {
            val operations = listOf(
                orderOperation(1, "first", TodoOperationType.CREATE),
                orderOperation(2, "first", TodoOperationType.MOVE, 1, listOf("a", "first", "b")),
                orderOperation(3, "second", TodoOperationType.CREATE),
            )

            TodoTaskOrdering.project(listOf("a", "b", "first", "second"), operations) shouldBe
                listOf("second", "a", "first", "b")
        }

        "resolve placeholders and collapse duplicate identities before assigning positions" {
            val operations = listOf(
                orderOperation(1, "local", TodoOperationType.CREATE),
                orderOperation(2, "local", TodoOperationType.MOVE, 1, listOf("a", "local", "b")),
            )

            TodoTaskOrdering.project(listOf("a", "local", "server", "b"), operations) {
                if (it == "local") "server" else it
            } shouldBe listOf("a", "server", "b")
        }

        "remove deletion tombstones and never resurrect them from a later saved order" {
            val operations = listOf(
                orderOperation(1, "a", TodoOperationType.DELETE),
                orderOperation(2, "c", TodoOperationType.MOVE, 0, listOf("c", "a", "b")),
            )

            TodoTaskOrdering.project(listOf("a", "b", "c"), operations) shouldBe listOf("c", "b")
        }

        "retain new server arrivals once without inventing rows absent from current personal tasks" {
            val move = orderOperation(1, "b", TodoOperationType.MOVE, 0, listOf("b", "a", "absent"))

            TodoTaskOrdering.project(listOf("a", "b", "new", "new"), listOf(move)) shouldBe listOf("b", "a", "new")
        }

        "clamp local insertion positions and support previously stored position-only moves" {
            TodoTaskOrdering.move(listOf("a", "b", "c"), "c", -1) shouldBe listOf("c", "a", "b")
            TodoTaskOrdering.move(listOf("a", "b", "c"), "a", 99) shouldBe listOf("b", "c", "a")
            val move = orderOperation(1, "a", TodoOperationType.MOVE, 2)

            TodoTaskOrdering.project(listOf("a", "b", "c"), listOf(move)) shouldBe listOf("b", "c", "a")
        }

        "remove completed tasks before applying active-list move indices" {
            val operations = listOf(
                scoreOrderOperation(1, "completed", up = true),
                orderOperation(2, "a", TodoOperationType.MOVE, 1),
            )

            TodoTaskOrdering.project(listOf("completed", "a", "b"), operations) shouldBe listOf("b", "a")
        }

        "never reinsert a queued create into active order after it has also been completed" {
            val operations = listOf(
                orderOperation(1, "created", TodoOperationType.CREATE),
                scoreOrderOperation(2, "created", up = true),
            )

            TodoTaskOrdering.project(listOf("a", "b"), operations) shouldBe listOf("a", "b")
        }

        "append an undone Todo to active order as the server does instead of reusing an old position" {
            val undo = scoreOrderOperation(1, "undone", up = false)

            TodoTaskOrdering.project(listOf("undone", "a", "b"), listOf(undo)) shouldBe listOf("a", "b", "undone")
            TodoTaskOrdering.project(listOf("a", "b"), listOf(undo)) shouldBe listOf("a", "b", "undone")
        }

        "preserve completion then undo ordering through repeated refresh projection" {
            val operations = listOf(
                scoreOrderOperation(1, "a", up = true),
                scoreOrderOperation(2, "a", up = false),
            )
            val expected = listOf("b", "c", "a")

            TodoTaskOrdering.project(listOf("a", "b", "c"), operations) shouldBe expected
            TodoTaskOrdering.project(expected, operations) shouldBe expected
        }
    }
})

/** Builds an opaque operation with only the local ordering fields used by these tests. */
private fun orderOperation(
    sequence: Long,
    taskId: String,
    type: TodoOperationType,
    position: Int = 0,
    order: List<String>? = null,
): TodoOperation {
    val payload = JsonObject().apply {
        addProperty("position", position)
        order?.let { add(TodoTaskOrdering.LOCAL_ORDER, JsonArray().apply { it.forEach { id -> add(id) } }) }
    }
    return TodoOperation(sequence, TodoScope("https://example.com", "test-user"), taskId, type, payload.toString())
}

/** Builds a desired completion operation whose server behavior changes active Todo membership. */
private fun scoreOrderOperation(sequence: Long, taskId: String, up: Boolean): TodoOperation =
    TodoOperation(
        sequence, TodoScope("https://example.com", "test-user"), taskId, TodoOperationType.SCORE,
        JsonObject().apply { addProperty("up", up) }.toString(),
    )
