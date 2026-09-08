package com.habitrpg.android.habitica.data.sync

import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.tasks.TaskType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import retrofit2.HttpException
import java.io.IOException
import java.util.Date
import java.util.UUID

/** Durable, ordered requests; never owns or transforms the task-list query or plays audio. */
class TodoOutbox(
    private val store: TodoOutboxStore,
    private val currentScope: () -> TodoScope?,
    private val remoteForScope: (TodoScope) -> TodoRemoteApi,
    private val schedule: () -> Unit,
    val codec: TodoTaskCodec = TodoTaskCodec(),
    private val invalidateReads: () -> Unit = {}
) {
    private val replayMutex = Mutex()
    private val gson = Gson()

    /** Returns the current authenticated account/server, not a cached login or a credential. */
    fun scope(): TodoScope? = currentScope()

    /** Restricts the first implementation to the current account's personal Todo list. */
    fun handles(task: Task): Boolean {
        val scope = scope() ?: return false
        return task.type == TaskType.TODO && !task.isGroupTask &&
            (task.ownerID.isBlank() || task.ownerID == scope.userId)
    }

    /** Persists an immutable request and final local state before reporting success to the UI. */
    fun enqueue(task: Task, type: TodoOperationType, payload: JsonObject? = null): Task {
        val scope = requireNotNull(scope())
        require(handles(task))
        if (task.id.isNullOrBlank()) task.id = UUID.randomUUID().toString()
        if (type != TodoOperationType.CREATE) task.id = store.resolve(scope, requireNotNull(task.id))
        task.ownerID = scope.userId
        if (task.dateCreated == null) task.dateCreated = Date()
        task.isSaving = false
        task.isCreating = false
        task.hasErrored = false
        val pending = store.projections(scope).firstOrNull {
            store.resolve(scope, it.taskId) == task.id
        }
        val key = pending?.taskId ?: requireNotNull(task.id)
        if (type != TodoOperationType.SCORE && type != TodoOperationType.CREATE && pending != null) {
            // An edit form may have been opened before a queued score; edits cannot undo scoring.
            val previous = codec.restore(pending.snapshot)
            task.completed = previous.completed
            if (type == TodoOperationType.UPDATE) task.position = previous.position
        }
        val request = payload ?: codec.request(task, type == TodoOperationType.CREATE)
        store.enqueue(scope, key, type, request.toString(), codec.snapshot(task), type == TodoOperationType.DELETE)
        invalidateReads()
        schedule()
        return task
    }

    /** Restores pending local truth after refresh/restart without wrapping Realm query results. */
    fun projections(acknowledgingSequence: Long? = null): List<Pair<String, Task?>> {
        val scope = scope() ?: return emptyList()
        val remaining = acknowledgingSequence?.let {
            store.operations(scope).filter { it.sequence != acknowledgingSequence }.map { it.taskId }.toSet()
        }
        return store.projections(scope).filter { remaining == null || it.taskId in remaining }.map { pending ->
            pending.taskId to if (pending.deleted) null else codec.restore(pending.snapshot).apply {
                id = store.resolve(scope, pending.taskId)
                ownerID = scope.userId
            }
        }
    }

    /** Resolves a placeholder identity for both local lookups and subsequent API operations. */
    fun resolve(taskId: String): String = scope()?.let { store.resolve(it, taskId) } ?: taskId

    /** Reconstructs pending personal-Todo ordering after refresh without overriding final server order. */
    fun pendingOrder(currentOrder: List<String>, acknowledgingSequence: Long? = null): List<String>? {
        val scope = scope() ?: return null
        val operations = store.operations(scope).filter { it.sequence != acknowledgingSequence }
        if (operations.none { it.type == TodoOperationType.CREATE || it.type == TodoOperationType.MOVE ||
                it.type == TodoOperationType.DELETE || it.type == TodoOperationType.SCORE }) {
            return null
        }
        return TodoTaskOrdering.project(currentOrder, operations) { store.resolve(scope, it) }
    }

    /** Schedules a constrained retry only when persisted work actually remains. */
    fun resume() {
        scope()?.let { if (store.hasPending(it)) schedule() }
    }

    /** Replays one account's queue serially, checkpointing each response before local application. */
    suspend fun replay(apply: suspend (TodoOperation, TodoReply) -> Unit): Boolean = replayMutex.withLock {
        val scope = scope() ?: return@withLock true
        val remote = try {
            remoteForScope(scope)
        } catch (_: IOException) {
            return@withLock false
        }
        while (currentScope() == scope) {
            val operation = store.operations(scope).firstOrNull() ?: return@withLock true
            try {
                val reply = operation.response?.let { gson.fromJson(it, TodoReply::class.java) }
                    ?: execute(operation, remote).also {
                        store.recordResponse(
                            operation.sequence, gson.toJson(it),
                            it.serverId.takeIf { operation.type == TodoOperationType.CREATE }
                        )
                        invalidateReads()
                    }
                // A response for the old account is retained, never applied to a different login.
                if (currentScope() != scope) return@withLock false
                apply(operation, reply)
                // A GET begun before this acknowledgement must not erase the just-applied result.
                invalidateReads()
                store.acknowledge(operation.sequence)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: IOException) {
                return@withLock false
            } catch (_: HttpException) {
                // Keep the original request and every dependent request, including on 4xx.
                return@withLock false
            }
        }
        false
    }

    /** Reconciles uncertain requests before repeating non-idempotent scoring/toggle endpoints. */
    private suspend fun execute(operation: TodoOperation, remote: TodoRemoteApi): TodoReply {
        val scope = operation.scope
        val id = store.resolve(scope, operation.taskId)
        val payload = JsonParser.parseString(operation.payload).asJsonObject
        // The attempt flag survives process death even if the response never reached the phone.
        store.markAttempted(operation.sequence)
        return when (operation.type) {
            TodoOperationType.CREATE -> {
                val existing = if (operation.attempted) remote.find(id) else null
                val task = existing ?: remote.create(payload.apply { addProperty("_id", id) })
                replyFor(task, scope)
            }
            TodoOperationType.UPDATE -> replyFor(remote.update(id, payload.apply { addProperty("_id", id) }), scope)
            TodoOperationType.DELETE -> {
                if (!operation.attempted || remote.find(id) != null) {
                    try {
                        remote.delete(id)
                    } catch (failure: HttpException) {
                        if (failure.code() != 404) throw failure
                    }
                }
                TodoReply(serverId = id, deleted = true)
            }
            TodoOperationType.MOVE -> {
                val order = remote.move(id, payload.get("position").asInt)
                replyFor(requireOwned(remote.find(id), scope), scope).copy(order = order)
            }
            TodoOperationType.SCORE -> {
                val up = payload.get("up").asBoolean
                val before = requireOwned(remote.find(id), scope)
                if (before.completed == up) {
                    replyFor(before, scope).copy(refreshUser = true)
                } else {
                    val score = try {
                        remote.score(id, up)
                    } catch (failure: HttpException) {
                        if (failure.code() != 401) throw failure
                        val reconciled = requireOwned(remote.find(id), scope)
                        if (reconciled.completed != up) throw failure
                        return replyFor(reconciled, scope).copy(refreshUser = true)
                    }
                    before.completed = up
                    before.value += score.delta
                    replyFor(before, scope).copy(score = score, refreshUser = true)
                }
            }
            TodoOperationType.CHECKLIST -> {
                val itemId = payload.get("itemId").asString
                val completed = payload.get("completed").asBoolean
                val before = requireOwned(remote.find(id), scope)
                val item = before.checklist?.firstOrNull { it.id == itemId }
                    ?: throw IOException("Pending checklist item is not yet available")
                val result = if (item.completed == completed) before else requireOwned(remote.checklist(id, itemId), scope)
                if (result.checklist?.firstOrNull { it.id == itemId }?.completed != completed) {
                    throw IOException("Pending checklist result could not be reconciled")
                }
                replyFor(result, scope)
            }
        }
    }

    /** Rejects mismatched ownership/type without writing the returned task into another cache. */
    private fun requireOwned(task: Task?, scope: TodoScope): Task {
        if (task == null || task.id.isNullOrBlank() || task.ownerID != scope.userId ||
            task.type != TaskType.TODO || task.isGroupTask
        ) throw IOException("Pending Todo could not be reconciled for its account")
        return task
    }

    /** Serializes a verified server task as a durable response receipt before Realm changes. */
    private fun replyFor(task: Task, scope: TodoScope): TodoReply {
        val owned = requireOwned(task, scope)
        return TodoReply(serverId = owned.id, snapshot = codec.snapshot(owned))
    }
}

/** A local receipt, not a server-side idempotency token or an exactly-once guarantee. */
data class TodoReply(
    val serverId: String? = null,
    val snapshot: String? = null,
    val deleted: Boolean = false,
    val order: List<String>? = null,
    val score: TaskDirectionData? = null,
    val refreshUser: Boolean = false
)
