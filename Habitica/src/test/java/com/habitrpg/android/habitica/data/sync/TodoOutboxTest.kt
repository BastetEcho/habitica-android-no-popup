package com.habitrpg.android.habitica.data.sync

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.mockk
import io.realm.RealmList
import kotlinx.coroutines.CancellationException
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.ResponseBody.Companion.toResponseBody
import retrofit2.HttpException
import retrofit2.Response
import java.io.IOException

/** Exercises replay boundaries with a retained store; SQLite durability has separate tests. */
class TodoOutboxTest : WordSpec({
    "durable Todo replay" should {
        "keep offline create and completion ordered through transport failure and coordinator restart" {
            val fixture = OutboxFixture()
            val task = testTodo()
            fixture.outbox.enqueue(task, TodoOperationType.CREATE)
            task.completed = true
            fixture.outbox.enqueue(task, TodoOperationType.SCORE, completionPayload(true))
            coEvery { fixture.remote.create(any()) } throws IOException("offline fixture")

            fixture.outbox.replay { _, _ -> error("Offline work must not be applied") } shouldBe false
            fixture.store.operations(TEST_SCOPE).map { it.type } shouldBe listOf(TodoOperationType.CREATE, TodoOperationType.SCORE)
            fixture.store.operations(TEST_SCOPE).first().attempted shouldBe true
            fixture.outbox.projections().single().second?.completed shouldBe true
            fixture.restart()
            coEvery { fixture.remote.find(TEST_TASK_ID) } returnsMany listOf(null, testTodo())
            coEvery { fixture.remote.create(any()) } returns testTodo()
            val score = TaskDirectionData()
            coEvery { fixture.remote.score(TEST_TASK_ID, true) } returns score
            val applied = mutableListOf<TodoOperationType>()

            fixture.outbox.replay { operation, reply ->
                applied.add(operation.type)
                fixture.outbox.projections().single().second?.completed shouldBe true
                if (operation.type == TodoOperationType.SCORE) {
                    reply.score shouldBe score
                    reply.refreshUser shouldBe true
                }
            } shouldBe true

            applied shouldBe listOf(TodoOperationType.CREATE, TodoOperationType.SCORE)
            fixture.store.hasPending(TEST_SCOPE) shouldBe false
            fixture.outbox.projections() shouldBe emptyList()
            coVerify(exactly = 1) { fixture.remote.score(TEST_TASK_ID, true) }
        }

        "reconcile a lost create response by its original UUID without creating another task" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            val originalPayload = fixture.store.operations(TEST_SCOPE).single().payload
            coEvery { fixture.remote.create(any()) } throws IOException("response lost fixture")
            fixture.outbox.replay { _, _ -> } shouldBe false
            fixture.restart()
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo()

            fixture.outbox.replay { operation, reply ->
                operation.payload shouldBe originalPayload
                reply.serverId shouldBe TEST_TASK_ID
            } shouldBe true

            coVerify(exactly = 1) { fixture.remote.create(match { it.get("_id").asString == TEST_TASK_ID }) }
            coVerify(exactly = 1) { fixture.remote.find(TEST_TASK_ID) }
        }

        "apply a persisted response receipt without another remote call" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            val operation = fixture.store.operations(TEST_SCOPE).single()
            val reply = TodoReply(TEST_TASK_ID, fixture.outbox.codec.snapshot(testTodo()))
            fixture.store.recordResponse(operation.sequence, Gson().toJson(reply), TEST_TASK_ID)
            fixture.restart()
            var applied = 0

            fixture.outbox.replay { _, actual -> actual shouldBe reply; applied++ } shouldBe true

            applied shouldBe 1
            coVerify(exactly = 0) { fixture.remote.create(any()); fixture.remote.find(any()) }
        }

        "retain a successful response when local application fails and retry only application" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } returns testTodo()

            shouldThrow<IllegalStateException> {
                fixture.outbox.replay { _, _ -> error("local application fixture failure") }
            }
            fixture.store.operations(TEST_SCOPE).single().response.isNullOrBlank() shouldBe false
            fixture.restart()
            fixture.outbox.replay { _, _ -> } shouldBe true

            coVerify(exactly = 1) { fixture.remote.create(any()) }
            coVerify(exactly = 0) { fixture.remote.find(any()) }
        }

        "retain a score receipt when refreshing user state fails and never rescore during application retry" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo().apply { completed = true }, TodoOperationType.SCORE, completionPayload(true))
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo().apply { value = 7.5 }
            coEvery { fixture.remote.score(TEST_TASK_ID, true) } returns TaskDirectionData().apply { delta = 1.25f }

            fixture.outbox.replay { _, reply ->
                reply.refreshUser shouldBe true
                throw IOException("user refresh fixture failure")
            } shouldBe false
            fixture.store.operations(TEST_SCOPE).single().response.isNullOrBlank() shouldBe false
            fixture.restart()
            fixture.outbox.replay { operation, reply ->
                reply.refreshUser shouldBe true
                reply.score?.delta shouldBe 1.25f
                fixture.outbox.codec.restore(requireNotNull(reply.snapshot)).value shouldBe 8.75
                fixture.outbox.projections(operation.sequence) shouldBe emptyList()
            } shouldBe true

            coVerify(exactly = 1) { fixture.remote.score(TEST_TASK_ID, true); fixture.remote.find(TEST_TASK_ID) }
        }

        "exclude only the final acknowledged task projection so canonical server values survive application" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            fixture.outbox.enqueue(testTodo().apply { text = "latest fixture" }, TodoOperationType.UPDATE)
            fixture.outbox.enqueue(testTodo(OTHER_TASK_ID), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } answers {
                testTodo(firstArg<JsonObject>().get("_id").asString).apply { value = 11.0 }
            }
            coEvery { fixture.remote.update(TEST_TASK_ID, any()) } returns testTodo().apply { value = 12.0 }

            fixture.outbox.replay { operation, reply ->
                val projected = fixture.outbox.projections(operation.sequence)
                if (operation.taskId == TEST_TASK_ID && operation.type == TodoOperationType.CREATE) {
                    projected.map { it.first }.toSet() shouldBe setOf(TEST_TASK_ID, OTHER_TASK_ID)
                    projected.first { it.first == TEST_TASK_ID }.second?.text shouldBe "latest fixture"
                } else if (operation.taskId == TEST_TASK_ID) {
                    projected.map { it.first } shouldBe listOf(OTHER_TASK_ID)
                    fixture.outbox.codec.restore(requireNotNull(reply.snapshot)).value shouldBe 12.0
                } else {
                    projected shouldBe emptyList()
                }
                // Reading the application overlay must not itself delete durable records.
                fixture.store.operations(TEST_SCOPE).any { it.sequence == operation.sequence } shouldBe true
            } shouldBe true

            fixture.outbox.projections() shouldBe emptyList()
        }

        "remap subsequent requests and projected identities when creation returns another server ID" {
            val fixture = OutboxFixture()
            val task = testTodo()
            fixture.outbox.enqueue(task, TodoOperationType.CREATE)
            task.completed = true
            fixture.outbox.enqueue(task, TodoOperationType.SCORE, completionPayload(true))
            coEvery { fixture.remote.create(any()) } returns testTodo(OTHER_TASK_ID)
            coEvery { fixture.remote.find(OTHER_TASK_ID) } returns testTodo(OTHER_TASK_ID)
            coEvery { fixture.remote.score(OTHER_TASK_ID, true) } returns TaskDirectionData()

            fixture.outbox.replay { operation, reply ->
                operation.taskId shouldBe TEST_TASK_ID
                reply.serverId shouldBe OTHER_TASK_ID
                fixture.outbox.resolve(TEST_TASK_ID) shouldBe OTHER_TASK_ID
                fixture.outbox.projections().single().second?.id shouldBe OTHER_TASK_ID
                fixture.outbox.projections().single().second?.completed shouldBe true
            } shouldBe true

            fixture.outbox.projections() shouldBe emptyList()
            coVerify(exactly = 0) { fixture.remote.find(TEST_TASK_ID); fixture.remote.score(TEST_TASK_ID, any()) }
            coVerify(exactly = 1) { fixture.remote.score(OTHER_TASK_ID, true) }
        }

        "retain the old account receipt without applying it after an account switch" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } answers {
                fixture.selectedScope = TEST_SCOPE.copy(userId = "other-user")
                testTodo()
            }

            fixture.outbox.replay { _, _ -> error("Must not apply to another account") } shouldBe false
            fixture.store.operations(TEST_SCOPE).single().response.isNullOrBlank() shouldBe false
            fixture.outbox.projections() shouldBe emptyList()
            fixture.outbox.replay { _, _ -> error("Other account has no work") } shouldBe true
            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            fixture.selectedScope = TEST_SCOPE
            fixture.restart()
            fixture.outbox.replay { _, _ -> } shouldBe true

            coVerify(exactly = 1) { fixture.remote.create(any()) }
        }

        "stop FIFO at the first failure without advancing edits or other tasks" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            fixture.outbox.enqueue(testTodo().apply { text = "edited fixture" }, TodoOperationType.UPDATE)
            fixture.outbox.enqueue(testTodo(OTHER_TASK_ID), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } throws IOException("offline fixture")

            fixture.outbox.replay { _, _ -> } shouldBe false

            fixture.store.operations(TEST_SCOPE).size shouldBe 3
            fixture.store.operations(TEST_SCOPE).map { it.attempted } shouldBe listOf(true, false, false)
            coVerify(exactly = 1) { fixture.remote.create(any()) }
            coVerify(exactly = 0) { fixture.remote.update(any(), any()) }
        }

        "preserve the completed edited projection while earlier create edit and move responses arrive" {
            val fixture = OutboxFixture()
            val task = testTodo()
            fixture.outbox.enqueue(task, TodoOperationType.CREATE)
            task.text = "final edited fixture"
            fixture.outbox.enqueue(task, TodoOperationType.UPDATE)
            task.position = 2
            fixture.outbox.enqueue(task, TodoOperationType.MOVE, JsonObject().apply { addProperty("position", 2) })
            task.completed = true
            fixture.outbox.enqueue(task, TodoOperationType.SCORE, completionPayload(true))
            coEvery { fixture.remote.create(any()) } returns testTodo()
            coEvery { fixture.remote.update(TEST_TASK_ID, any()) } returns testTodo().apply { text = "server edit" }
            coEvery { fixture.remote.move(TEST_TASK_ID, 2) } returns listOf(OTHER_TASK_ID, TEST_TASK_ID)
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo()
            coEvery { fixture.remote.score(TEST_TASK_ID, true) } returns TaskDirectionData()
            val applied = mutableListOf<TodoOperationType>()

            fixture.outbox.replay { operation, _ ->
                applied.add(operation.type)
                val projection = requireNotNull(fixture.outbox.projections().single().second)
                projection.text shouldBe "final edited fixture"
                projection.completed shouldBe true
                projection.position shouldBe 2
            } shouldBe true

            applied shouldBe listOf(TodoOperationType.CREATE, TodoOperationType.UPDATE, TodoOperationType.MOVE, TodoOperationType.SCORE)
            coVerifyOrder {
                fixture.remote.create(any())
                fixture.remote.update(TEST_TASK_ID, match { !it.has("completed") && it.get("text").asString == "final edited fixture" })
                fixture.remote.move(TEST_TASK_ID, 2)
                fixture.remote.find(TEST_TASK_ID)
                fixture.remote.find(TEST_TASK_ID)
                fixture.remote.score(TEST_TASK_ID, true)
            }
        }

        "retain a newly enqueued edit when the previous response is being applied" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } returns testTodo(OTHER_TASK_ID)
            coEvery { fixture.remote.update(OTHER_TASK_ID, any()) } returns testTodo(OTHER_TASK_ID).apply { text = "new fixture edit" }
            val applied = mutableListOf<TodoOperationType>()

            fixture.outbox.replay { operation, _ ->
                applied.add(operation.type)
                if (operation.type == TodoOperationType.CREATE) {
                    fixture.outbox.enqueue(testTodo(OTHER_TASK_ID).apply { text = "new fixture edit" }, TodoOperationType.UPDATE)
                    val overlay = fixture.outbox.projections(operation.sequence).single()
                    overlay.first shouldBe TEST_TASK_ID
                    overlay.second?.id shouldBe OTHER_TASK_ID
                    overlay.second?.text shouldBe "new fixture edit"
                }
            } shouldBe true

            applied shouldBe listOf(TodoOperationType.CREATE, TodoOperationType.UPDATE)
            fixture.store.hasPending(TEST_SCOPE) shouldBe false
            coVerify(exactly = 1) { fixture.remote.update(OTHER_TASK_ID, match { it.get("text").asString == "new fixture edit" }) }
        }

        "reconcile a lost completion response instead of repeating scoring and quest effects" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo().apply { completed = true }, TodoOperationType.SCORE, completionPayload(true))
            var completedOnServer = false
            coEvery { fixture.remote.find(TEST_TASK_ID) } answers { testTodo().apply { completed = completedOnServer } }
            coEvery { fixture.remote.score(TEST_TASK_ID, true) } answers {
                completedOnServer = true
                throw IOException("score response lost fixture")
            }
            fixture.outbox.replay { _, _ -> } shouldBe false
            fixture.restart()

            fixture.outbox.replay { _, reply ->
                reply.refreshUser shouldBe true
                reply.score shouldBe null
                fixture.outbox.codec.restore(requireNotNull(reply.snapshot)).completed shouldBe true
            } shouldBe true

            coVerify(exactly = 1) { fixture.remote.score(TEST_TASK_ID, true) }
        }

        "reconcile the server already-scored rejection only when the desired state is present" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo().apply { completed = true }, TodoOperationType.SCORE, completionPayload(true))
            coEvery { fixture.remote.find(TEST_TASK_ID) } returnsMany listOf(testTodo(), testTodo().apply { completed = true })
            coEvery { fixture.remote.score(TEST_TASK_ID, true) } throws outboxHttpError(401)

            fixture.outbox.replay { _, reply -> reply.refreshUser shouldBe true } shouldBe true

            coVerify(exactly = 1) { fixture.remote.score(TEST_TASK_ID, true) }
        }

        "keep a genuine score rejection and its dependent operations pending" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo().apply { completed = true }, TodoOperationType.SCORE, completionPayload(true))
            coEvery { fixture.remote.find(TEST_TASK_ID) } answers { testTodo() }
            coEvery { fixture.remote.score(TEST_TASK_ID, true) } throws outboxHttpError(401)

            fixture.outbox.replay { _, _ -> error("Rejected scores cannot be acknowledged") } shouldBe false

            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            fixture.store.operations(TEST_SCOPE).single().response shouldBe null
        }

        "call the full down-scoring operation without a reward presentation dependency" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.SCORE, completionPayload(false))
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo().apply { completed = true }
            val score = TaskDirectionData()
            coEvery { fixture.remote.score(TEST_TASK_ID, false) } returns score

            fixture.outbox.replay { _, reply ->
                reply.score shouldBe score
                reply.refreshUser shouldBe true
                fixture.outbox.codec.restore(requireNotNull(reply.snapshot)).completed shouldBe false
            } shouldBe true

            coVerify(exactly = 1) { fixture.remote.score(TEST_TASK_ID, false) }
            coVerify(exactly = 0) { fixture.remote.update(any(), any()) }
        }

        "reconcile a checklist toggle whose response was lost without toggling it back" {
            val fixture = OutboxFixture()
            val task = testTodo().apply { checklist = RealmList(ChecklistItem("item-id", "fixture item", true)) }
            fixture.outbox.enqueue(task, TodoOperationType.CHECKLIST, checklistPayload(true))
            var serverCompleted = false
            coEvery { fixture.remote.find(TEST_TASK_ID) } answers {
                testTodo().apply { checklist = RealmList(ChecklistItem("item-id", "fixture item", serverCompleted)) }
            }
            coEvery { fixture.remote.checklist(TEST_TASK_ID, "item-id") } answers {
                serverCompleted = true
                throw IOException("checklist response lost fixture")
            }
            fixture.outbox.replay { _, _ -> } shouldBe false
            fixture.restart()

            fixture.outbox.replay { _, reply ->
                fixture.outbox.codec.restore(requireNotNull(reply.snapshot)).checklist?.single()?.completed shouldBe true
            } shouldBe true

            coVerify(exactly = 1) { fixture.remote.checklist(TEST_TASK_ID, "item-id") }
        }

        "keep a checklist operation when the server item has not appeared yet" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CHECKLIST, checklistPayload(true))
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo()

            fixture.outbox.replay { _, _ -> } shouldBe false

            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            coVerify(exactly = 0) { fixture.remote.checklist(any(), any()) }
        }

        "retain a checklist request when the toggle response disagrees with the desired state" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CHECKLIST, checklistPayload(true))
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo().apply {
                checklist = RealmList(ChecklistItem("item-id", "fixture item", false))
            }
            coEvery { fixture.remote.checklist(TEST_TASK_ID, "item-id") } returns testTodo().apply {
                checklist = RealmList(ChecklistItem("item-id", "fixture item", false))
            }

            fixture.outbox.replay { _, _ -> error("Conflicting checklist state must not be acknowledged") } shouldBe false

            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            fixture.store.operations(TEST_SCOPE).single().response shouldBe null
            coVerify(exactly = 1) { fixture.remote.checklist(TEST_TASK_ID, "item-id") }
        }

        "retain a checklist request when its target disappears from the toggle response" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CHECKLIST, checklistPayload(true))
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo().apply {
                checklist = RealmList(ChecklistItem("item-id", "fixture item", false))
            }
            coEvery { fixture.remote.checklist(TEST_TASK_ID, "item-id") } returns testTodo()

            fixture.outbox.replay { _, _ -> error("Missing checklist state must not be acknowledged") } shouldBe false

            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            fixture.store.operations(TEST_SCOPE).single().response shouldBe null
        }

        "acknowledge a delete returning not-found and remove its tombstone" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.DELETE)
            fixture.outbox.projections().single().second shouldBe null
            coEvery { fixture.remote.delete(TEST_TASK_ID) } throws outboxHttpError(404)

            fixture.outbox.replay { _, reply -> reply.deleted shouldBe true } shouldBe true

            fixture.outbox.projections() shouldBe emptyList()
            coVerify(exactly = 0) { fixture.remote.find(any()) }
        }

        "reconcile a lost delete response when GET confirms absence" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.DELETE)
            coEvery { fixture.remote.delete(TEST_TASK_ID) } throws IOException("delete response lost fixture")
            fixture.outbox.replay { _, _ -> } shouldBe false
            fixture.restart()
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns null

            fixture.outbox.replay { _, reply -> reply.deleted shouldBe true } shouldBe true

            coVerify(exactly = 1) { fixture.remote.delete(TEST_TASK_ID) }
        }

        "preserve cancellation and pending data instead of converting cancellation to success" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } throws CancellationException("cancelled fixture")

            shouldThrow<CancellationException> { fixture.outbox.replay { _, _ -> } }

            fixture.store.operations(TEST_SCOPE).single().attempted shouldBe true
            fixture.store.operations(TEST_SCOPE).single().response shouldBe null
            fixture.outbox.projections().single().second?.id shouldBe TEST_TASK_ID
        }

        "return a retry outcome when selecting the account-bound gateway fails" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            val outbox = TodoOutbox(
                fixture.store,
                { TEST_SCOPE },
                { throw TodoRemoteScopeChangedException() },
                {},
            )

            outbox.replay { _, _ -> error("A stale gateway cannot apply work") } shouldBe false

            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            fixture.store.operations(TEST_SCOPE).single().attempted shouldBe false
            fixture.store.operations(TEST_SCOPE).single().response shouldBe null
        }

        "reject a reconciliation result from another account without acknowledging the request" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            fixture.store.markAttempted(fixture.store.operations(TEST_SCOPE).single().sequence)
            coEvery { fixture.remote.find(TEST_TASK_ID) } returns testTodo().apply { ownerID = "other-user" }

            fixture.outbox.replay { _, _ -> error("Foreign tasks must not enter local storage") } shouldBe false

            fixture.store.hasPending(TEST_SCOPE) shouldBe true
            coVerify(exactly = 0) { fixture.remote.create(any()) }
        }

        "resume only schedules durable work belonging to the selected account" {
            val fixture = OutboxFixture()
            fixture.outbox.resume()
            fixture.scheduled shouldBe 0
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            fixture.scheduled shouldBe 1
            fixture.restart()
            fixture.outbox.resume()
            fixture.scheduled shouldBe 2
            fixture.selectedScope = TEST_SCOPE.copy(userId = "other-user")
            fixture.outbox.resume()
            fixture.scheduled shouldBe 2
        }

        "restore pending full ordering across coordinator recreation but defer to the final server order at acknowledgement" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            val task = testTodo().apply { position = 1 }
            fixture.outbox.enqueue(task, TodoOperationType.MOVE, JsonObject().apply {
                addProperty("position", 1)
                add(TodoTaskOrdering.LOCAL_ORDER, JsonArray().apply { add(OTHER_TASK_ID); add(TEST_TASK_ID) })
            })
            fixture.restart()
            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID)) shouldBe listOf(OTHER_TASK_ID, TEST_TASK_ID)
            val create = fixture.store.operations(TEST_SCOPE).first()
            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID), create.sequence) shouldBe listOf(OTHER_TASK_ID, TEST_TASK_ID)
            fixture.store.acknowledge(create.sequence)
            val move = fixture.store.operations(TEST_SCOPE).single()

            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID), move.sequence) shouldBe null
            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID)) shouldBe listOf(OTHER_TASK_ID, TEST_TASK_ID)
        }

        "preserve queued completion and position when an older editor snapshot is saved afterward" {
            val fixture = OutboxFixture()
            val staleEditor = testTodo().apply { position = 0; completed = false }
            val checkedTask = testTodo().apply { position = 0; completed = true }
            fixture.outbox.enqueue(checkedTask, TodoOperationType.SCORE, completionPayload(true))
            checkedTask.position = 2
            fixture.outbox.enqueue(checkedTask, TodoOperationType.MOVE, JsonObject().apply { addProperty("position", 2) })
            staleEditor.text = "edited fixture from old form"

            fixture.outbox.enqueue(staleEditor, TodoOperationType.UPDATE)
            fixture.restart()

            val restored = requireNotNull(fixture.outbox.projections().single().second)
            restored.text shouldBe "edited fixture from old form"
            restored.completed shouldBe true
            restored.position shouldBe 2
            val operations = fixture.store.operations(TEST_SCOPE)
            operations.map { it.type } shouldBe listOf(TodoOperationType.SCORE, TodoOperationType.MOVE, TodoOperationType.UPDATE)
            JsonParser.parseString(operations.last().payload).asJsonObject.has("completed") shouldBe false
        }

        "project completion removal and undo append only while those score requests remain pending" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo().apply { completed = true }, TodoOperationType.SCORE, completionPayload(true))
            val score = fixture.store.operations(TEST_SCOPE).single()

            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID)) shouldBe listOf(OTHER_TASK_ID)
            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID), score.sequence) shouldBe null
            fixture.outbox.enqueue(testTodo(), TodoOperationType.SCORE, completionPayload(false))
            fixture.restart()

            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID)) shouldBe listOf(OTHER_TASK_ID, TEST_TASK_ID)
            fixture.selectedScope = TEST_SCOPE.copy(userId = "other-user")
            fixture.outbox.pendingOrder(listOf(TEST_TASK_ID, OTHER_TASK_ID)) shouldBe null
        }

        "merge canonical and stale-placeholder edits under one key after creation mapping is acknowledged" {
            val fixture = OutboxFixture()
            fixture.outbox.enqueue(testTodo(), TodoOperationType.CREATE)
            coEvery { fixture.remote.create(any()) } returns testTodo(OTHER_TASK_ID)
            fixture.outbox.replay { _, _ -> } shouldBe true
            fixture.outbox.projections() shouldBe emptyList()
            fixture.outbox.resolve(TEST_TASK_ID) shouldBe OTHER_TASK_ID

            fixture.outbox.enqueue(testTodo(OTHER_TASK_ID).apply { text = "canonical fixture edit" }, TodoOperationType.UPDATE)
            fixture.outbox.enqueue(testTodo(TEST_TASK_ID).apply { text = "later stale-form fixture edit" }, TodoOperationType.UPDATE)
            fixture.restart()

            val pending = fixture.store.operations(TEST_SCOPE)
            pending.map { it.taskId } shouldBe listOf(OTHER_TASK_ID, OTHER_TASK_ID)
            pending.map { JsonParser.parseString(it.payload).asJsonObject.get("_id").asString } shouldBe
                listOf(OTHER_TASK_ID, OTHER_TASK_ID)
            val projection = fixture.outbox.projections().single()
            projection.first shouldBe OTHER_TASK_ID
            projection.second?.id shouldBe OTHER_TASK_ID
            projection.second?.text shouldBe "later stale-form fixture edit"
        }
    }
})

private val TEST_SCOPE = TodoScope("https://example.com", "test-user")
private const val TEST_TASK_ID = "11111111-1111-4111-8111-111111111111"
private const val OTHER_TASK_ID = "22222222-2222-4222-8222-222222222222"

/** Creates an unmanaged, wholly fictional personal Todo. */
private fun testTodo(taskId: String = TEST_TASK_ID): Task = Task().apply {
    id = taskId
    ownerID = TEST_SCOPE.userId
    type = TaskType.TODO
    text = "fixture task"
}

/** Builds an immutable desired completion operation. */
private fun completionPayload(up: Boolean): JsonObject = JsonObject().apply { addProperty("up", up) }

/** Builds an immutable desired checklist state operation. */
private fun checklistPayload(completed: Boolean): JsonObject = JsonObject().apply {
    addProperty("itemId", "item-id")
    addProperty("completed", completed)
}

/** Creates an HTTP fixture containing no actual task or account data. */
private fun outboxHttpError(code: Int): HttpException =
    HttpException(Response.error<Unit>(code, "{}".toResponseBody("application/json".toMediaType())))

/** Keeps durable fixtures while replacing only the in-memory replay coordinator. */
private class OutboxFixture {
    val store = MemoryTodoOutboxStore()
    val remote = mockk<TodoRemoteApi>()
    var selectedScope: TodoScope? = TEST_SCOPE
    var scheduled = 0
    var outbox = coordinator()

    /** Recreates a process-local coordinator without deleting its durable records. */
    fun restart() {
        outbox = coordinator()
    }

    /** Supplies a quiet remote gateway and a counted scheduler to the coordinator. */
    private fun coordinator(): TodoOutbox = TodoOutbox(store, { selectedScope }, { remote }, { scheduled++ })
}

/** Models persistence semantics for coordinator tests; actual database tests live separately. */
private class MemoryTodoOutboxStore : TodoOutboxStore {
    private var sequence = 0L
    private val pending = linkedMapOf<Long, TodoOperation>()
    private val views = linkedMapOf<Pair<TodoScope, String>, TodoProjection>()
    private val mappings = mutableMapOf<Pair<TodoScope, String>, String>()

    /** Appends immutable payload data and replaces only the latest task projection. */
    override fun enqueue(scope: TodoScope, taskId: String, type: TodoOperationType, payload: String, snapshot: String, deleted: Boolean): TodoOperation {
        JsonParser.parseString(payload).isJsonObject shouldBe true
        val operation = TodoOperation(++sequence, scope, taskId, type, payload)
        pending[operation.sequence] = operation
        views[scope to taskId] = TodoProjection(taskId, snapshot, deleted)
        return operation
    }

    /** Returns scoped immutable requests in insertion sequence. */
    override fun operations(scope: TodoScope): List<TodoOperation> = pending.values.filter { it.scope == scope }

    /** Returns the latest views for one server and account. */
    override fun projections(scope: TodoScope): List<TodoProjection> = views.filterKeys { it.first == scope }.values.toList()

    /** Resolves only mappings that belong to the requested server and account. */
    override fun resolve(scope: TodoScope, taskId: String): String = mappings[scope to taskId] ?: taskId

    /** Checkpoints delivery without changing the immutable request payload. */
    override fun markAttempted(sequence: Long) {
        pending[sequence]?.let { pending[sequence] = it.copy(attempted = true) }
    }

    /** Saves a response receipt and server mapping together in the fixture. */
    override fun recordResponse(sequence: Long, response: String, serverId: String?) {
        val operation = requireNotNull(pending[sequence])
        check(operation.response == null || operation.response == response)
        if (serverId != null) {
            check(operation.type == TodoOperationType.CREATE)
            val key = operation.scope to operation.taskId
            check(mappings[key] == null || mappings[key] == serverId)
            mappings[key] = serverId
        }
        pending[sequence] = operation.copy(response = response)
    }

    /** Retires a request but keeps local truth until all requests for its task are handled. */
    override fun acknowledge(sequence: Long) {
        val operation = pending.remove(sequence) ?: return
        if (pending.values.none { it.scope == operation.scope && it.taskId == operation.taskId }) {
            views.remove(operation.scope to operation.taskId)
        }
    }

    /** Checks whether the requested account has unacknowledged work. */
    override fun hasPending(scope: TodoScope): Boolean = pending.values.any { it.scope == scope }
}
