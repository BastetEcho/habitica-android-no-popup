package com.habitrpg.android.habitica.data.sync

/** Identifies an account at a public API server, without authentication credentials. */
data class TodoScope(val server: String, val userId: String)

/** Identifies the server operation represented by a queued personal Todo request. */
enum class TodoOperationType { CREATE, UPDATE, SCORE, DELETE, MOVE, CHECKLIST }

/** An immutable request with separately persisted delivery and response checkpoints. */
data class TodoOperation(
    val sequence: Long,
    val scope: TodoScope,
    val taskId: String,
    val type: TodoOperationType,
    val payload: String,
    val attempted: Boolean = false,
    val response: String? = null,
)

/** The latest local view of a task while one or more requests remain queued. */
data class TodoProjection(val taskId: String, val snapshot: String, val deleted: Boolean)

/** Persists task data independently of Realm; callers must never include credentials in JSON. */
interface TodoOutboxStore {
    /** Atomically appends an immutable request and replaces its task's local projection. */
    fun enqueue(
        scope: TodoScope,
        taskId: String,
        type: TodoOperationType,
        payload: String,
        snapshot: String,
        deleted: Boolean = false,
    ): TodoOperation

    /** Returns this account's pending requests in durable, globally increasing sequence order. */
    fun operations(scope: TodoScope): List<TodoOperation>

    /** Returns this account's latest local task views, including pending deletion tombstones. */
    fun projections(scope: TodoScope): List<TodoProjection>

    /** Resolves a local task ID to its acknowledged server ID, or returns the original ID. */
    fun resolve(scope: TodoScope, taskId: String): String

    /** Records that delivery may have begun before the caller performs a network request. */
    fun markAttempted(sequence: Long)

    /** Atomically records a response and, for task creation, its durable server ID mapping. */
    fun recordResponse(sequence: Long, response: String, serverId: String? = null)

    /** Removes a handled request, retaining its projection while any task request remains. */
    fun acknowledge(sequence: Long)

    /** Reports whether this account has any requests awaiting local acknowledgement. */
    fun hasPending(scope: TodoScope): Boolean
}
