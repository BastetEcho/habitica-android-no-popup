package com.habitrpg.android.habitica.data.sync

import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteException
import android.database.sqlite.SQLiteOpenHelper
import dagger.hilt.android.qualifiers.ApplicationContext
import javax.inject.Inject
import javax.inject.Singleton

/** Stores personal Todo requests in an app-private database that never opens the Realm cache. */
@Singleton
class SqliteTodoOutboxStore @Inject constructor(
    @ApplicationContext context: Context,
) : SQLiteOpenHelper(context, DATABASE_NAME, null, 1), TodoOutboxStore {
    /** Creates the first schema without interacting with any previous offline implementation. */
    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE todo_operations (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                server TEXT NOT NULL,
                user_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                attempted INTEGER NOT NULL DEFAULT 0,
                response TEXT
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE todo_projections (
                server TEXT NOT NULL,
                user_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                snapshot TEXT NOT NULL,
                deleted INTEGER NOT NULL,
                PRIMARY KEY (server, user_id, task_id)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE todo_id_mappings (
                server TEXT NOT NULL,
                user_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                server_id TEXT NOT NULL,
                PRIMARY KEY (server, user_id, task_id)
            )
            """.trimIndent()
        )
        db.execSQL("CREATE INDEX todo_operations_scope ON todo_operations (server, user_id, sequence)")
        db.execSQL("CREATE INDEX todo_operations_task ON todo_operations (server, user_id, task_id)")
    }

    /** Refuses unknown upgrades rather than deleting pending user work. */
    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        throw SQLiteException("An explicit Todo outbox migration is required: $oldVersion to $newVersion")
    }

    /** Refuses downgrades rather than silently replacing the outbox. */
    override fun onDowngrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        throw SQLiteException("Todo outbox downgrade is not supported: $oldVersion to $newVersion")
    }

    /** Commits a request and its latest projection together, or neither if either write fails. */
    @Synchronized
    override fun enqueue(
        scope: TodoScope,
        taskId: String,
        type: TodoOperationType,
        payload: String,
        snapshot: String,
        deleted: Boolean,
    ): TodoOperation {
        require(scope.server.isNotBlank() && scope.userId.isNotBlank() && taskId.isNotBlank())
        return transaction {
            val values = scopedValues(scope, taskId).apply {
                put("operation_type", type.name)
                put("payload", payload)
            }
            val sequence = insertOrThrow("todo_operations", null, values)
            val projection = scopedValues(scope, taskId).apply {
                put("snapshot", snapshot)
                put("deleted", if (deleted) 1 else 0)
            }
            if (insertWithOnConflict("todo_projections", null, projection, SQLiteDatabase.CONFLICT_REPLACE) == -1L) {
                throw SQLiteException("Could not persist the Todo projection")
            }
            TodoOperation(sequence, scope, taskId, type, payload)
        }
    }

    /** Reads ordered request checkpoints for exactly one server and account. */
    @Synchronized
    override fun operations(scope: TodoScope): List<TodoOperation> =
        readableDatabase.query(
            "todo_operations", null, SCOPE_SELECTION, scope.arguments(), null, null, "sequence ASC"
        ).use { cursor ->
            buildList {
                while (cursor.moveToNext()) add(cursor.operation())
            }
        }

    /** Reads the latest pending views without changing the application's Realm query results. */
    @Synchronized
    override fun projections(scope: TodoScope): List<TodoProjection> =
        readableDatabase.query(
            "todo_projections", null, SCOPE_SELECTION, scope.arguments(), null, null, "task_id ASC"
        ).use { cursor ->
            buildList {
                while (cursor.moveToNext()) {
                    add(TodoProjection(cursor.string("task_id"), cursor.string("snapshot"), cursor.integer("deleted") != 0))
                }
            }
        }

    /** Uses the durable mapping without modifying the stable task IDs in queued operations. */
    @Synchronized
    override fun resolve(scope: TodoScope, taskId: String): String =
        readableDatabase.mappedId(scope, taskId) ?: taskId

    /** Persists the delivery checkpoint; retrying it after acknowledgement is harmless. */
    @Synchronized
    override fun markAttempted(sequence: Long) {
        writableDatabase.update(
            "todo_operations", ContentValues().apply { put("attempted", 1) }, "sequence = ?", arrayOf(sequence.toString())
        )
    }

    /** Saves response and ID mapping in the same transaction, preserving earlier checkpoints. */
    @Synchronized
    override fun recordResponse(sequence: Long, response: String, serverId: String?) {
        require(serverId == null || serverId.isNotBlank())
        transaction {
            val operation = operation(sequence) ?: error("The Todo operation no longer exists")
            check(operation.response == null || operation.response == response) { "The Todo operation already has a different response" }
            if (serverId != null) {
                check(operation.type == TodoOperationType.CREATE) { "Only Todo creation can assign a server ID" }
                val previousId = mappedId(operation.scope, operation.taskId)
                check(previousId == null || previousId == serverId) { "The Todo operation already has a different server ID" }
                if (previousId == null) {
                    insertOrThrow(
                        "todo_id_mappings", null,
                        scopedValues(operation.scope, operation.taskId).apply { put("server_id", serverId) }
                    )
                }
            }
            update(
                "todo_operations", ContentValues().apply { put("response", response) },
                "sequence = ?", arrayOf(sequence.toString())
            )
        }
    }

    /** Retires handled work while retaining newer projections and permanent ID mappings. */
    @Synchronized
    override fun acknowledge(sequence: Long) {
        transaction {
            val operation = operation(sequence) ?: return@transaction
            delete("todo_operations", "sequence = ?", arrayOf(sequence.toString()))
            val arguments = operation.scope.arguments(operation.taskId)
            val hasTaskWork = query(
                "todo_operations", arrayOf("sequence"), TASK_SELECTION, arguments, null, null, null, "1"
            ).use { it.moveToFirst() }
            if (!hasTaskWork) delete("todo_projections", TASK_SELECTION, arguments)
        }
    }

    /** Checks for pending work without loading stored request bodies. */
    @Synchronized
    override fun hasPending(scope: TodoScope): Boolean =
        readableDatabase.query(
            "todo_operations", arrayOf("sequence"), SCOPE_SELECTION, scope.arguments(), null, null, null, "1"
        ).use { it.moveToFirst() }

    /** Runs related writes atomically and always ends the transaction on exceptions. */
    private inline fun <T> transaction(block: SQLiteDatabase.() -> T): T {
        val database = writableDatabase
        database.beginTransaction()
        try {
            val result = database.block()
            database.setTransactionSuccessful()
            return result
        } finally {
            database.endTransaction()
        }
    }

    /** Retrieves one request inside its caller's transaction without exposing another account. */
    private fun SQLiteDatabase.operation(sequence: Long): TodoOperation? =
        query("todo_operations", null, "sequence = ?", arrayOf(sequence.toString()), null, null, null).use {
            if (it.moveToFirst()) it.operation() else null
        }

    /** Looks up a mapping using all components of its account boundary. */
    private fun SQLiteDatabase.mappedId(scope: TodoScope, taskId: String): String? =
        query("todo_id_mappings", arrayOf("server_id"), TASK_SELECTION, scope.arguments(taskId), null, null, null).use {
            if (it.moveToFirst()) it.string("server_id") else null
        }

    /** Decodes the current row without deserializing caller-provided task JSON. */
    private fun Cursor.operation(): TodoOperation =
        TodoOperation(
            getLong(getColumnIndexOrThrow("sequence")),
            TodoScope(string("server"), string("user_id")),
            string("task_id"),
            TodoOperationType.valueOf(string("operation_type")),
            string("payload"),
            integer("attempted") != 0,
            getColumnIndexOrThrow("response").let { if (isNull(it)) null else getString(it) },
        )

    /** Reads a required text column from the current row. */
    private fun Cursor.string(column: String): String = getString(getColumnIndexOrThrow(column))

    /** Reads a required integer column from the current row. */
    private fun Cursor.integer(column: String): Int = getInt(getColumnIndexOrThrow(column))

    /** Builds bound SQL arguments, optionally narrowed to one stable task ID. */
    private fun TodoScope.arguments(taskId: String? = null): Array<String> =
        if (taskId == null) arrayOf(server, userId) else arrayOf(server, userId, taskId)

    /** Builds an insert row containing only task identity and account scope. */
    private fun scopedValues(scope: TodoScope, taskId: String): ContentValues =
        ContentValues().apply {
            put("server", scope.server)
            put("user_id", scope.userId)
            put("task_id", taskId)
        }

    companion object {
        internal const val DATABASE_NAME = "personal-todo-outbox.db"
        private const val SCOPE_SELECTION = "server = ? AND user_id = ?"
        private const val TASK_SELECTION = "$SCOPE_SELECTION AND task_id = ?"
    }
}
