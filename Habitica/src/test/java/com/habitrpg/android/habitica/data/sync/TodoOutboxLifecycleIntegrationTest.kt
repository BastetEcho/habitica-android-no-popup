package com.habitrpg.android.habitica.data.sync

import android.app.Application
import android.content.Context
import com.google.firebase.perf.FirebasePerformance
import com.google.firebase.perf.metrics.Trace
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.habitrpg.android.habitica.HabiticaBaseApplication
import com.habitrpg.android.habitica.api.GSonFactoryCreator
import com.habitrpg.android.habitica.data.implementation.ApiClientImpl
import com.habitrpg.android.habitica.helpers.NotificationsManager
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.common.habitica.api.HostConfig
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.unmockkObject
import io.mockk.unmockkStatic
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import okhttp3.Dns
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.annotation.Config
import org.robolectric.annotation.SQLiteMode
import java.io.File
import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Exercises persisted replay through the production converter and authenticated HTTP gateway. */
@RunWith(RobolectricTestRunner::class)
@Config(manifest = Config.NONE, application = Application::class, sdk = [28])
@SQLiteMode(SQLiteMode.Mode.LEGACY)
class TodoOutboxLifecycleIntegrationTest {
    private lateinit var context: Context
    private lateinit var store: SqliteTodoOutboxStore
    private lateinit var server: MockWebServer
    private lateinit var cacheDirectory: File
    private lateinit var notifications: NotificationsManager
    private lateinit var api: ApiClientImpl
    private lateinit var scope: TodoScope
    private lateinit var outbox: TodoOutbox
    private val builders = mutableListOf<OkHttpClient.Builder>()
    private val online = AtomicBoolean(false)
    private var scheduled = 0

    /** Creates isolated SQLite and loopback fixtures with deliberately unavailable DNS. */
    @Before
    fun setUp() {
        context = RuntimeEnvironment.getApplication()
        context.deleteDatabase(SqliteTodoOutboxStore.DATABASE_NAME)
        store = SqliteTodoOutboxStore(context)
        server = MockWebServer().apply { start() }
        cacheDirectory = Files.createTempDirectory("habitica-outbox-lifecycle-").toFile()
        val apiContext = mockk<Context>(relaxed = true)
        every { apiContext.cacheDir } returns cacheDirectory
        notifications = mockk(relaxed = true)
        val address = server.url("/").newBuilder().host("example.invalid").build().toString()
        val host = HostConfig(address, "", "example-api-key", EXAMPLE_USER)
        scope = TodoScope(host.address, host.userID)
        val dns = object : Dns {
            /** Resolves only the synthetic host after connectivity returns. */
            override fun lookup(hostname: String): List<InetAddress> {
                if (!online.get()) throw UnknownHostException("Offline test fixture")
                return listOf(InetAddress.getByName("127.0.0.1"))
            }
        }
        api = spyk(ApiClientImpl(GSonFactoryCreator.create(), host, notifications, apiContext) {
            OkHttpClient.Builder().dns(dns).also { builders.add(it) }
        })
        mockkObject(HabiticaBaseApplication.Companion)
        every { HabiticaBaseApplication.logout(any(), any()) } just Runs
        mockkStatic(FirebasePerformance::class)
        val performance = mockk<FirebasePerformance>()
        every { FirebasePerformance.getInstance() } returns performance
        every { performance.newTrace(any()) } returns mockk<Trace>(relaxed = true)
        outbox = coordinator()
    }

    /** Closes only fixture resources and removes the isolated test database and cache. */
    @After
    fun tearDown() {
        store.close()
        context.deleteDatabase(SqliteTodoOutboxStore.DATABASE_NAME)
        builders.forEach { builder ->
            builder.build().apply {
                cache?.close()
                connectionPool.evictAll()
                dispatcher.executorService.shutdown()
            }
        }
        server.shutdown()
        cacheDirectory.deleteRecursively()
        unmockkStatic(FirebasePerformance::class)
        unmockkObject(HabiticaBaseApplication.Companion)
    }

    /** Preserves offline intent across restart, applies canonical IDs, and never replays rewards twice. */
    @Test
    fun offlineCreateAndScoreSurviveRestartThenEditAndDeleteUseCanonicalIdentity() = runBlocking {
        val local = Task().apply {
            id = LOCAL_ID
            ownerID = EXAMPLE_USER
            type = TaskType.TODO
            text = "Example offline task"
        }
        outbox.enqueue(local, TodoOperationType.CREATE)
        local.completed = true
        outbox.enqueue(local, TodoOperationType.SCORE, JsonObject().apply { addProperty("up", true) })

        assertFalse(outbox.replay { _, _ -> error("Offline work must not be acknowledged") })
        assertEquals(0, server.requestCount)
        assertEquals(2, scheduled)
        assertEquals(listOf(TodoOperationType.CREATE, TodoOperationType.SCORE), store.operations(scope).map { it.type })
        assertTrue(store.operations(scope).first().attempted)
        assertTrue(requireNotNull(outbox.projections().single().second).completed)
        reopen()
        assertTrue(requireNotNull(outbox.projections().single().second).completed)
        assertEquals(EXAMPLE_USER, outbox.projections().single().second?.ownerID)

        online.set(true)
        // The first create attempt failed before reaching the server; replay must reconcile first.
        server.enqueue(MockResponse().setResponseCode(404))
        server.enqueue(taskResponse(completed = false))
        server.enqueue(taskResponse(completed = false))
        server.enqueue(envelope("""{"delta":1.25,"gp":3,"exp":5,"hp":50,"mp":8}"""))
        server.enqueue(envelope("""{"_id":"$EXAMPLE_USER","stats":{"gp":3},"tasksOrder":{"todos":[]}}"""))
        val applied = mutableListOf<TodoOperationType>()
        assertTrue(outbox.replay { operation, reply ->
            applied.add(operation.type)
            assertEquals(CANONICAL_ID, reply.serverId)
            if (reply.refreshUser) {
                assertEquals(EXAMPLE_USER, remote().user().id)
                assertTrue(outbox.codec.restore(requireNotNull(reply.snapshot)).completed)
            }
        })
        assertEquals(listOf(TodoOperationType.CREATE, TodoOperationType.SCORE), applied)
        val firstReplay = List(5) { takeRequest() }
        assertEquals(
            listOf(
                "GET /api/v4/tasks/$LOCAL_ID",
                "POST /api/v4/tasks/user",
                "GET /api/v4/tasks/$CANONICAL_ID",
                "POST /api/v4/tasks/$CANONICAL_ID/score/up",
                "GET /api/v4/user/"
            ),
            firstReplay.map { "${it.method} ${it.requestUrl?.encodedPath}" }
        )
        val createPayload = JsonParser.parseString(firstReplay[1].body.readUtf8()).asJsonObject
        assertEquals(LOCAL_ID, createPayload.get("_id").asString)
        assertFalse(createPayload.get("completed").asBoolean)
        assertTrue(store.operations(scope).isEmpty())
        assertTrue(store.projections(scope).isEmpty())
        assertTrue(outbox.replay { _, _ -> error("An acknowledged operation was replayed") })
        assertEquals(5, server.requestCount)

        // Reopening again proves the server-ID mapping is durable, not coordinator memory.
        reopen()
        assertEquals(CANONICAL_ID, outbox.resolve(LOCAL_ID))
        local.id = LOCAL_ID
        local.text = "Example updated task"
        outbox.enqueue(local, TodoOperationType.UPDATE)
        local.id = LOCAL_ID
        outbox.enqueue(local, TodoOperationType.DELETE)
        server.enqueue(taskResponse(completed = true, text = local.text))
        server.enqueue(MockResponse().setResponseCode(204))
        assertTrue(outbox.replay { operation, reply ->
            assertEquals(CANONICAL_ID, reply.serverId)
            if (operation.type == TodoOperationType.DELETE) assertTrue(reply.deleted)
        })
        val update = takeRequest()
        val delete = takeRequest()
        assertEquals("PUT", update.method)
        assertEquals("/api/v4/tasks/$CANONICAL_ID", update.requestUrl?.encodedPath)
        val updatePayload = JsonParser.parseString(update.body.readUtf8()).asJsonObject
        assertEquals(CANONICAL_ID, updatePayload.get("_id").asString)
        assertEquals(local.text, updatePayload.get("text").asString)
        assertFalse(updatePayload.has("completed"))
        assertEquals("DELETE", delete.method)
        assertEquals("/api/v4/tasks/$CANONICAL_ID", delete.requestUrl?.encodedPath)
        assertTrue(store.operations(scope).isEmpty())
        assertTrue(outbox.projections().isEmpty())
        assertTrue(outbox.replay { _, _ -> error("An acknowledged operation was replayed") })
        assertEquals(7, server.requestCount)
        assertNull(server.takeRequest(100, TimeUnit.MILLISECONDS))
        (firstReplay + listOf(update, delete)).forEach { request ->
            assertEquals(EXAMPLE_USER, request.getHeader("x-api-user"))
            assertEquals("no-cache, no-store", request.getHeader("Cache-Control"))
        }
        verify { notifications wasNot Called }
        verify(exactly = 0) { api.accept(any()) }
        verify(exactly = 0) { HabiticaBaseApplication.logout(any(), any()) }
    }

    /** Recreates the coordinator using the real database and the current scoped HTTP gateway. */
    private fun coordinator(): TodoOutbox = TodoOutbox(
        store, { scope }, { api.todoRemoteApi.forAccount(it.server, it.userId) },
        { scheduled++ }, invalidateReads = api::invalidateTaskReads
    )

    /** Reopens the persisted database without recreating or clearing any queued state. */
    private fun reopen() {
        store.close()
        store = SqliteTodoOutboxStore(context)
        outbox = coordinator()
    }

    /** Returns the same strict gateway used by production score-receipt reconciliation. */
    private fun remote(): TodoRemoteApi = api.todoRemoteApi.forAccount(scope.server, scope.userId)

    /** Reads one expected request with a bounded timeout rather than waiting indefinitely. */
    private fun takeRequest(): RecordedRequest = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))

    /** Creates an owned canonical server task rather than assuming the requested local ID survives. */
    private fun taskResponse(completed: Boolean, text: String = "Example offline task"): MockResponse = envelope(
        """{"_id":"$CANONICAL_ID","userId":"$EXAMPLE_USER","type":"todo","text":"$text","completed":$completed,"challenge":{}}"""
    )

    /** Includes a notification in every successful envelope to detect accidental presentation callbacks. */
    private fun envelope(data: String): MockResponse = MockResponse()
        .addHeader("Content-Type", "application/json")
        .setBody("""{"success":true,"data":$data,"notifications":[{"id":"example-notification","type":"unknown"}]}""")

    private companion object {
        const val EXAMPLE_USER = "example-user"
        const val LOCAL_ID = "00000000-0000-4000-8000-000000000001"
        const val CANONICAL_ID = "00000000-0000-4000-8000-000000000002"
    }
}
