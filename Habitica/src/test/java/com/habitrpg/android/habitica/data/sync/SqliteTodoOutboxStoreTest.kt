package com.habitrpg.android.habitica.data.sync

import android.app.Application
import android.content.Context
import android.database.sqlite.SQLiteException
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.annotation.SQLiteMode
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Exercises actual SQLite durability and transactions without starting Habitica services. */
@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE, application = Application::class, sdk = [28])
@SQLiteMode(SQLiteMode.Mode.LEGACY)
class SqliteTodoOutboxStoreTest {
    private lateinit var context: Context
    private lateinit var store: SqliteTodoOutboxStore
    private val scope = TodoScope("https://example.com", "example-user")

    /** Starts each test with an isolated app-private test database. */
    @Before
    fun createStore() {
        context = RuntimeEnvironment.getApplication()
        context.deleteDatabase(SqliteTodoOutboxStore.DATABASE_NAME)
        store = SqliteTodoOutboxStore(context)
    }

    /** Closes test connections and removes only the isolated fixture database. */
    @After
    fun closeStore() {
        store.close()
        context.deleteDatabase(SqliteTodoOutboxStore.DATABASE_NAME)
    }

    /** Keeps immutable request bodies in sequence while projecting only the latest task view. */
    @Test
    fun enqueuePreservesRequestsAndLatestProjection() {
        val first = enqueue("local-task", TodoOperationType.CREATE, "first")
        val second = enqueue("local-task", TodoOperationType.UPDATE, "second")

        assertTrue(second.sequence > first.sequence)
        assertEquals(listOf(first, second), store.operations(scope))
        assertEquals(listOf(TodoProjection("local-task", "second", false)), store.projections(scope))
        assertTrue(store.hasPending(scope))
    }

    /** Isolates requests, projections, and mappings by both account and server. */
    @Test
    fun accountAndServerScopesNeverShareWork() {
        val otherUser = TodoScope(scope.server, "other-user")
        val otherServer = TodoScope("https://other.example.com", scope.userId)
        val first = enqueue("local-task", TodoOperationType.CREATE, "first")
        val second = store.enqueue(otherUser, "local-task", TodoOperationType.CREATE, "second", "second")
        val third = store.enqueue(otherServer, "local-task", TodoOperationType.CREATE, "third", "third")
        store.recordResponse(first.sequence, "created", "server-task")

        assertEquals(listOf(first.copy(response = "created")), store.operations(scope))
        assertEquals(listOf(second), store.operations(otherUser))
        assertEquals(listOf(third), store.operations(otherServer))
        assertEquals(listOf(TodoProjection("local-task", "second", false)), store.projections(otherUser))
        assertEquals(listOf(TodoProjection("local-task", "third", false)), store.projections(otherServer))
        assertEquals("server-task", store.resolve(scope, "local-task"))
        assertEquals("local-task", store.resolve(otherUser, "local-task"))
        assertEquals("local-task", store.resolve(otherServer, "local-task"))
        assertTrue(first.sequence < second.sequence && second.sequence < third.sequence)
    }

    /** Persists attempted requests, responses, ID mappings, and projections across reopen. */
    @Test
    fun restartRetainsEveryDeliveryCheckpoint() {
        val first = enqueue("local-task", TodoOperationType.CREATE, "create-body")
        store.markAttempted(first.sequence)
        store.recordResponse(first.sequence, "created-response", "server-task")
        val second = enqueue("local-task", TodoOperationType.SCORE, "score-body")
        reopen()

        assertEquals(listOf(first.copy(attempted = true, response = "created-response"), second), store.operations(scope))
        assertEquals("server-task", store.resolve(scope, "local-task"))
        assertEquals(listOf(TodoProjection("local-task", "score-body", false)), store.projections(scope))
    }

    /** Keeps a newer local view until every request for that task has been handled. */
    @Test
    fun acknowledgementRetainsNewerProjectionAndDurableMapping() {
        val first = enqueue("local-task", TodoOperationType.CREATE, "first")
        store.recordResponse(first.sequence, "created", "server-task")
        val second = enqueue("local-task", TodoOperationType.DELETE, "deleted", deleted = true)
        val unrelated = enqueue("other-task", TodoOperationType.UPDATE, "other")
        store.acknowledge(first.sequence)

        assertEquals(listOf(second, unrelated), store.operations(scope))
        assertTrue(store.projections(scope).contains(TodoProjection("local-task", "deleted", true)))
        store.acknowledge(second.sequence)
        assertEquals(listOf(TodoProjection("other-task", "other", false)), store.projections(scope))
        store.acknowledge(unrelated.sequence)
        reopen()

        assertFalse(store.hasPending(scope))
        assertTrue(store.projections(scope).isEmpty())
        assertEquals("server-task", store.resolve(scope, "local-task"))
    }

    /** Does not discard a projection even if a caller acknowledges work out of order. */
    @Test
    fun outOfOrderAcknowledgementDoesNotLosePendingProjection() {
        val first = enqueue("local-task", TodoOperationType.CREATE, "first")
        val second = enqueue("local-task", TodoOperationType.UPDATE, "second")
        store.acknowledge(second.sequence)

        assertEquals(listOf(first), store.operations(scope))
        assertEquals(listOf(TodoProjection("local-task", "second", false)), store.projections(scope))
    }

    /** Failing the projection write also rolls back the immutable operation insert. */
    @Test
    fun failedProjectionWriteRollsBackEnqueue() {
        store.writableDatabase.execSQL(
            "CREATE TRIGGER reject_projection BEFORE INSERT ON todo_projections BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"
        )
        assertThrows(SQLiteException::class.java) { enqueue("local-task", TodoOperationType.CREATE, "first") }
        reopen()

        assertFalse(store.hasPending(scope))
        assertTrue(store.operations(scope).isEmpty())
        assertTrue(store.projections(scope).isEmpty())
    }

    /** Failing the response update cannot leave an ID mapping partially committed. */
    @Test
    fun failedResponseWriteRollsBackMapping() {
        val operation = enqueue("local-task", TodoOperationType.CREATE, "first")
        store.writableDatabase.execSQL(
            "CREATE TRIGGER reject_response BEFORE UPDATE ON todo_operations BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"
        )
        assertThrows(SQLiteException::class.java) { store.recordResponse(operation.sequence, "created", "server-task") }
        reopen()

        assertEquals("local-task", store.resolve(scope, "local-task"))
        assertEquals(listOf(operation), store.operations(scope))
    }

    /** Failing projection retirement also leaves its operation available for later retry. */
    @Test
    fun failedAcknowledgementRollsBackRequestDeletion() {
        val operation = enqueue("local-task", TodoOperationType.CREATE, "first")
        store.writableDatabase.execSQL(
            "CREATE TRIGGER reject_retirement BEFORE DELETE ON todo_projections BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"
        )
        assertThrows(SQLiteException::class.java) { store.acknowledge(operation.sequence) }
        reopen()

        assertEquals(listOf(operation), store.operations(scope))
        assertEquals(listOf(TodoProjection("local-task", "first", false)), store.projections(scope))
    }

    /** Repeating a checkpoint is harmless, but contradictory responses cannot overwrite it. */
    @Test
    fun responseCheckpointIsIdempotentAndCannotBeReassigned() {
        val operation = enqueue("local-task", TodoOperationType.CREATE, "first")
        store.recordResponse(operation.sequence, "created", "server-task")
        store.recordResponse(operation.sequence, "created", "server-task")
        assertThrows(IllegalStateException::class.java) { store.recordResponse(operation.sequence, "different", "server-task") }
        assertThrows(IllegalStateException::class.java) { store.recordResponse(operation.sequence, "created", "different-task") }

        assertEquals(listOf(operation.copy(response = "created")), store.operations(scope))
        assertEquals("server-task", store.resolve(scope, "local-task"))
    }

    /** A stale acknowledgement or attempt marker cannot resurrect already handled work. */
    @Test
    fun repeatedAcknowledgementAndAttemptDoNotResurrectWork() {
        val operation = enqueue("local-task", TodoOperationType.CREATE, "first")
        store.acknowledge(operation.sequence)
        store.acknowledge(operation.sequence)
        store.markAttempted(operation.sequence)

        assertFalse(store.hasPending(scope))
        assertTrue(store.projections(scope).isEmpty())
        assertThrows(IllegalStateException::class.java) { store.recordResponse(operation.sequence, "created", "server-task") }
    }

    /** New work receives a greater sequence even after the queue is emptied and reopened. */
    @Test
    fun globalSequenceIsNotReusedAfterRestart() {
        val first = enqueue("local-task", TodoOperationType.CREATE, "first")
        store.acknowledge(first.sequence)
        reopen()
        val second = enqueue("other-task", TodoOperationType.CREATE, "second")

        assertTrue(second.sequence > first.sequence)
        assertNotEquals(first.sequence, second.sequence)
    }

    /** Concurrent callers cannot lose requests or reorder the latest projection. */
    @Test
    fun concurrentEnqueuesHaveUniqueSequencesAndConsistentProjection() {
        val executor = Executors.newFixedThreadPool(4)
        try {
            val futures = (1..40).map { number ->
                executor.submit(Callable { enqueue("local-task", TodoOperationType.UPDATE, "snapshot-$number") })
            }
            val submitted = futures.map { it.get(30, TimeUnit.SECONDS) }
            val ordered = store.operations(scope)

            assertEquals(40, ordered.size)
            assertEquals(40, ordered.map { it.sequence }.toSet().size)
            assertEquals(submitted.sortedBy { it.sequence }, ordered)
            assertEquals(listOf(TodoProjection("local-task", ordered.last().payload, false)), store.projections(scope))
            reopen()
            assertEquals(ordered, store.operations(scope))
        } finally {
            executor.shutdownNow()
        }
    }

    /** Unknown schema transitions fail without dropping requests from the current schema. */
    @Test
    fun schemaChangesNeverDestructivelyRecreateTheOutbox() {
        val operation = enqueue("local-task", TodoOperationType.CREATE, "first")
        assertThrows(SQLiteException::class.java) { store.onUpgrade(store.writableDatabase, 1, 2) }
        assertThrows(SQLiteException::class.java) { store.onDowngrade(store.writableDatabase, 2, 1) }

        assertEquals(listOf(operation), store.operations(scope))
    }

    /** Enqueues a small opaque fixture without coupling storage tests to task serialization. */
    private fun enqueue(
        taskId: String,
        type: TodoOperationType,
        snapshot: String,
        deleted: Boolean = false,
    ): TodoOperation = store.enqueue(scope, taskId, type, snapshot, snapshot, deleted)

    /** Simulates process-level connection closure while retaining the same app-private file. */
    private fun reopen() {
        store.close()
        store = SqliteTodoOutboxStore(context)
    }
}
