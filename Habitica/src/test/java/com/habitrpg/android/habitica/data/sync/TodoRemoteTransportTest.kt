package com.habitrpg.android.habitica.data.sync

import android.content.Context
import com.google.firebase.perf.FirebasePerformance
import com.google.firebase.perf.metrics.Trace
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.habitrpg.android.habitica.HabiticaBaseApplication
import com.habitrpg.android.habitica.R
import com.habitrpg.android.habitica.api.GSonFactoryCreator
import com.habitrpg.android.habitica.data.implementation.ApiClientImpl
import com.habitrpg.android.habitica.helpers.NotificationsManager
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.common.habitica.api.HostConfig
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.Called
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.unmockkObject
import io.mockk.unmockkStatic
import io.mockk.verify
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import okhttp3.Call
import okhttp3.Dns
import okhttp3.EventListener
import okhttp3.OkHttpClient
import okhttp3.ResponseBody.Companion.toResponseBody
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import retrofit2.HttpException
import retrofit2.Response
import java.io.File
import java.net.InetAddress
import java.net.SocketException
import java.net.UnknownHostException
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Exercises the production Retrofit, converter and OkHttp interceptors against a local server. */
class TodoRemoteTransportTest : WordSpec({
    lateinit var server: MockWebServer
    lateinit var context: Context
    lateinit var notifications: NotificationsManager
    lateinit var host: HostConfig
    lateinit var cacheDirectory: File
    val builders = mutableListOf<OkHttpClient.Builder>()

    /** Constructs the actual API client while retaining only test-owned resources for cleanup. */
    fun client(dns: Dns = Dns.SYSTEM, listener: EventListener = EventListener.NONE): ApiClientImpl =
        spyk(ApiClientImpl(GSonFactoryCreator.create(), host, notifications, context) {
            OkHttpClient.Builder().dns(dns).eventListener(listener).also { builders.add(it) }
        })

    beforeEach {
        server = MockWebServer().apply { start() }
        cacheDirectory = Files.createTempDirectory("habitica-todo-transport-").toFile()
        context = mockk(relaxed = true)
        every { context.cacheDir } returns cacheDirectory
        notifications = mockk(relaxed = true)
        host = HostConfig(server.url("/").toString(), "", "example-api-key", "example-user")
        mockkObject(HabiticaBaseApplication.Companion)
        every { HabiticaBaseApplication.logout(any(), any()) } just Runs
    }

    afterEach {
        builders.forEach { builder ->
            builder.build().apply {
                cache?.close()
                connectionPool.evictAll()
                dispatcher.executorService.shutdown()
            }
        }
        builders.clear()
        server.shutdown()
        cacheDirectory.deleteRecursively()
        unmockkObject(HabiticaBaseApplication.Companion)
    }

    "production Todo transport" should {
        "stamp current task lists for every list endpoint and bypass the HTTP cache" {
            withModelTraces {
                val api = client()
                api.invalidateTaskReads()
                val expectedGeneration = api.taskReadGeneration
                repeat(3) { server.enqueue(transportTaskListResponse()) }

                api.getTasks()?.readGeneration shouldBe expectedGeneration
                api.getTasks("completedTodos")?.readGeneration shouldBe expectedGeneration
                api.getTasks("dailys", "2026-01-01")?.readGeneration shouldBe expectedGeneration

                repeat(3) {
                    requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
                        .getHeader("Cache-Control") shouldBe "no-cache, no-store"
                }
            }
        }

        "discard every task-list response started before a mutation was accepted" {
            withModelTraces {
                val api = client()
                val reads: List<suspend () -> TaskList?> = listOf(
                    { api.getTasks() },
                    { api.getTasks("completedTodos") },
                    { api.getTasks("dailys", "2026-01-01") }
                )
                clearMocks(context, notifications, api, answers = false)
                reads.forEach { read ->
                    val requestReceived = CountDownLatch(1)
                    val releaseResponse = CountDownLatch(1)
                    server.dispatcher = object : Dispatcher() {
                        /** Holds the old list snapshot until a local mutation invalidates it. */
                        override fun dispatch(request: RecordedRequest): MockResponse {
                            requestReceived.countDown()
                            check(releaseResponse.await(5, TimeUnit.SECONDS))
                            return transportTaskListResponse()
                        }
                    }
                    coroutineScope {
                        val attempt = async(Dispatchers.IO) { read() }
                        try {
                            requestReceived.await(5, TimeUnit.SECONDS) shouldBe true
                            api.invalidateTaskReads()
                        } finally {
                            releaseResponse.countDown()
                        }
                        attempt.await() shouldBe null
                    }
                }
                verify { notifications wasNot Called }
                verify { context wasNot Called }
                verify(exactly = 0) { api.accept(any()) }
            }
        }

        "discard a whole user refresh invalidated before its user response arrives" {
            withModelTraces {
                val api = client()
                val requestReceived = CountDownLatch(1)
                val releaseResponse = CountDownLatch(1)
                server.dispatcher = object : Dispatcher() {
                    /** Delays the initial user leg so the task snapshot must not start. */
                    override fun dispatch(request: RecordedRequest): MockResponse {
                        requestReceived.countDown()
                        check(releaseResponse.await(5, TimeUnit.SECONDS))
                        return transportUserResponse()
                    }
                }
                clearMocks(context, notifications, api, answers = false)
                coroutineScope {
                    val attempt = async(Dispatchers.IO) { api.retrieveUser(true) }
                    try {
                        requestReceived.await(5, TimeUnit.SECONDS) shouldBe true
                        api.invalidateTaskReads()
                    } finally {
                        releaseResponse.countDown()
                    }
                    attempt.await() shouldBe null
                }
                server.requestCount shouldBe 1
                verify { notifications wasNot Called }
                verify { context wasNot Called }
            }
        }

        "discard user stats and task order together when the second refresh leg becomes stale" {
            withModelTraces {
                val api = client()
                val tasksRequested = CountDownLatch(1)
                val releaseTasks = CountDownLatch(1)
                server.dispatcher = object : Dispatcher() {
                    /** Returns user state first, then holds the task list across invalidation. */
                    override fun dispatch(request: RecordedRequest): MockResponse {
                        if (request.path?.startsWith("/api/v4/user/") == true) return transportUserResponse()
                        tasksRequested.countDown()
                        check(releaseTasks.await(5, TimeUnit.SECONDS))
                        return transportTaskListResponse()
                    }
                }
                clearMocks(context, notifications, api, answers = false)
                coroutineScope {
                    val attempt = async(Dispatchers.IO) { api.retrieveUser(true) }
                    try {
                        tasksRequested.await(5, TimeUnit.SECONDS) shouldBe true
                        api.invalidateTaskReads()
                    } finally {
                        releaseTasks.countDown()
                    }
                    attempt.await() shouldBe null
                }
                server.requestCount shouldBe 2
                verify { notifications wasNot Called }
                verify { context wasNot Called }
            }
        }

        "stamp one shared generation on a current user and its task list" {
            withModelTraces {
                val api = client()
                server.enqueue(transportUserResponse())
                server.enqueue(transportTaskListResponse())

                val user = requireNotNull(api.retrieveUser(true))

                user.taskReadGeneration shouldBe api.taskReadGeneration
                user.tasks?.readGeneration shouldBe user.taskReadGeneration
            }
        }

        "not log out from an obsolete normal task-read authentication response" {
            val api = client()
            val requestReceived = CountDownLatch(1)
            val releaseResponse = CountDownLatch(1)
            server.dispatcher = object : Dispatcher() {
                /** Releases a stale authentication failure after its read generation changes. */
                override fun dispatch(request: RecordedRequest): MockResponse {
                    requestReceived.countDown()
                    check(releaseResponse.await(5, TimeUnit.SECONDS))
                    return MockResponse().setResponseCode(401)
                        .setBody("{\"error\":\"invalid_credentials\"}")
                }
            }
            clearMocks(context, notifications, api, answers = false)
            coroutineScope {
                val attempt = async(Dispatchers.IO) { api.getTasks() }
                try {
                    requestReceived.await(5, TimeUnit.SECONDS) shouldBe true
                    api.invalidateTaskReads()
                } finally {
                    releaseResponse.countDown()
                }
                attempt.await() shouldBe null
            }
            verify(exactly = 0) { HabiticaBaseApplication.logout(any(), any()) }
            verify(exactly = 0) { api.accept(any()) }
            verify { context wasNot Called }
        }

        "reject an obsolete normal read before attaching a later account's credentials" {
            val dnsEntered = CountDownLatch(1)
            val resumeDns = CountDownLatch(1)
            val dns = object : Dns {
                /** Delays transport so account invalidation occurs before header construction. */
                override fun lookup(hostname: String): List<InetAddress> {
                    dnsEntered.countDown()
                    check(resumeDns.await(5, TimeUnit.SECONDS))
                    return listOf(InetAddress.getByName("127.0.0.1"))
                }
            }
            host.address = server.url("/").newBuilder().host("example.invalid").build().toString()
            val api = client(dns)
            coroutineScope {
                val attempt = async(Dispatchers.IO) { api.getTasks() }
                try {
                    dnsEntered.await(5, TimeUnit.SECONDS) shouldBe true
                    api.invalidateTaskReads()
                    host.userID = "example-other-user"
                    host.apiKey = "example-other-api-key"
                } finally {
                    resumeDns.countDown()
                }
                attempt.await() shouldBe null
            }
            server.takeRequest(250, TimeUnit.MILLISECONDS) shouldBe null
            server.requestCount shouldBe 0
        }

        "fetch user rewards without fetching tasks or delivering notification callbacks" {
            mockkStatic(FirebasePerformance::class)
            try {
                val performance = mockk<FirebasePerformance>()
                every { FirebasePerformance.getInstance() } returns performance
                every { performance.newTrace(any()) } returns mockk<Trace>(relaxed = true)
                val api = client()
                val remote = api.todoRemoteApi.forAccount(host.address, host.userID)
                server.enqueue(MockResponse().setBody(
                    """{"success":true,"data":{"_id":"example-user","stats":{"gp":12}},"notifications":[{"id":"example-notification","type":"unknown"}]}"""
                ))
                clearMocks(context, notifications, api, answers = false)

                val user = remote.user()

                user.id shouldBe "example-user"
                user.stats?.gp shouldBe 12.0
                server.requestCount shouldBe 1
                requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)).apply {
                    method shouldBe "GET"
                    requestUrl?.encodedPath shouldBe "/api/v4/user/"
                    requestUrl?.queryParameter("fields")?.contains("stats") shouldBe true
                    getHeader("x-api-user") shouldBe "example-user"
                }
                verify { context wasNot Called }
                verify { notifications wasNot Called }
                verify(exactly = 0) { api.accept(any()) }
                verify(exactly = 0) { HabiticaBaseApplication.logout(any(), any()) }
            } finally {
                unmockkStatic(FirebasePerformance::class)
            }
        }

        "start replay with a bounded total timeout instead of the upstream long read timeout" {
            val startedTimeouts = mutableListOf<Long>()
            val api = client(listener = object : EventListener() {
                /** Records only the configured timeout of this local synthetic request. */
                override fun callStart(call: Call) {
                    startedTimeouts.add(call.timeout().timeoutNanos())
                }
            })
            server.enqueue(transportTaskResponse())

            api.todoRemoteApi.forAccount(host.address, host.userID).find("example-task")

            startedTimeouts shouldBe listOf(TimeUnit.SECONDS.toNanos(30))
        }

        "send the raw create and edit fields with captured authentication" {
            val api = client()
            val remote = api.todoRemoteApi.forAccount(host.address, host.userID)
            val payload = JsonObject().apply {
                addProperty("_id", "example-task")
                addProperty("type", "todo")
                addProperty("text", "Example task")
                addProperty("completed", true)
                add("date", com.google.gson.JsonNull.INSTANCE)
            }
            server.enqueue(transportTaskResponse())
            server.enqueue(transportTaskResponse())

            remote.create(payload).id shouldBe "example-task"
            remote.update("example-task", payload).id shouldBe "example-task"

            val create = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
            create.method shouldBe "POST"
            create.path shouldBe "/api/v4/tasks/user"
            create.getHeader("x-api-user") shouldBe "example-user"
            create.getHeader("x-api-key") shouldBe "example-api-key"
            JsonParser.parseString(create.body.readUtf8()) shouldBe payload
            val edit = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
            edit.method shouldBe "PUT"
            edit.path shouldBe "/api/v4/tasks/example-task"
            JsonParser.parseString(edit.body.readUtf8()) shouldBe payload
        }

        "bypass cached task lookups and callbacks even when a response carries notifications" {
            val api = client()
            val remote = api.todoRemoteApi.forAccount(host.address, host.userID)
            server.enqueue(transportTaskResponse().addHeader("Cache-Control", "max-age=86400"))
            server.enqueue(transportTaskResponse().addHeader("Cache-Control", "max-age=86400"))
            clearMocks(context, notifications, api, answers = false)

            remote.find("example-task")?.id shouldBe "example-task"
            remote.find("example-task")?.id shouldBe "example-task"

            server.requestCount shouldBe 2
            requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
                .getHeader("Cache-Control") shouldBe "no-cache, no-store"
            verify { notifications wasNot Called }
            verify { context wasNot Called }
            verify(exactly = 0) { api.accept(any()) }
        }

        "retain score and checklist POST operations and accept a genuinely empty delete" {
            val api = client()
            val remote = api.todoRemoteApi.forAccount(host.address, host.userID)
            server.enqueue(MockResponse().setBody("{\"success\":true,\"data\":{}}"))
            server.enqueue(transportTaskResponse())
            server.enqueue(MockResponse().setResponseCode(204))
            clearMocks(context, notifications, api, answers = false)

            remote.score("example-task", true)
            remote.checklist("example-task", "example-item")
            remote.delete("example-task")

            requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)).apply {
                method shouldBe "POST"
                path shouldBe "/api/v4/tasks/example-task/score/up"
            }
            requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)).apply {
                method shouldBe "POST"
                path shouldBe "/api/v4/tasks/example-task/checklist/example-item/score"
            }
            requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)).method shouldBe "DELETE"
            verify { notifications wasNot Called }
            verify { context wasNot Called }
            verify(exactly = 0) { api.accept(any()) }
        }

        "preserve 401 failures without logging out or showing errors from replay" {
            val api = client()
            val remote = api.todoRemoteApi.forAccount(host.address, host.userID)
            clearMocks(context, notifications, api, answers = false)

            listOf("sessionOutdated", "invalid_credentials").forEach { error ->
                val body = "{\"success\":false,\"error\":\"$error\"}"
                server.enqueue(MockResponse().setResponseCode(401).setBody(body))

                val failure = shouldThrow<HttpException> { remote.score("example-task", true) }
                failure.code() shouldBe 401
                failure.response()?.errorBody()?.string() shouldBe body
            }

            host.userID shouldBe "example-user"
            verify(exactly = 0) { HabiticaBaseApplication.logout(any(), any()) }
            verify(exactly = 0) { api.accept(any()) }
            verify { notifications wasNot Called }
            verify { context wasNot Called }
        }

        "keep expected connectivity failures out of the normal API warning UI" {
            val api = client()
            clearMocks(context, notifications, answers = false)

            api.accept(UnknownHostException())
            api.accept(SocketException())

            verify { context wasNot Called }
            verify { notifications wasNot Called }
        }

        "retain real server errors in the normal API error UI" {
            val api = client()
            clearMocks(context, notifications, answers = false)

            api.accept(HttpException(Response.error<Any>(500, "{}".toResponseBody())))

            verify(exactly = 1) { context.getString(R.string.internal_error_api) }
        }

        "block a queued body if the account changes while DNS is stalled" {
            val dnsEntered = CountDownLatch(1)
            val resumeDns = CountDownLatch(1)
            val dns = object : Dns {
                /** Pauses the local DNS result before authentication is attached to a request. */
                override fun lookup(hostname: String): List<InetAddress> {
                    dnsEntered.countDown()
                    check(resumeDns.await(5, TimeUnit.SECONDS))
                    return listOf(InetAddress.getByName("127.0.0.1"))
                }
            }
            host.address = server.url("/").newBuilder().host("example.invalid").build().toString()
            val api = client(dns)
            val remote = api.todoRemoteApi.forAccount(host.address, host.userID)
            val payload = JsonObject().apply { addProperty("text", "Example pending task") }

            coroutineScope {
                val attempt = async(Dispatchers.IO) { runCatching { remote.create(payload) } }
                try {
                    dnsEntered.await(5, TimeUnit.SECONDS) shouldBe true
                    host.userID = "example-other-user"
                    host.apiKey = "example-other-api-key"
                } finally {
                    resumeDns.countDown()
                }
                (attempt.await().exceptionOrNull() is TodoRemoteScopeChangedException) shouldBe true
            }

            server.takeRequest(250, TimeUnit.MILLISECONDS) shouldBe null
            server.requestCount shouldBe 0
        }

        "not apply a response after switching accounts during an in-flight request" {
            val responseReady = CountDownLatch(1)
            val releaseResponse = CountDownLatch(1)
            server.dispatcher = object : Dispatcher() {
                /** Holds the synthetic response until the test switches accounts. */
                override fun dispatch(request: RecordedRequest): MockResponse {
                    responseReady.countDown()
                    check(releaseResponse.await(5, TimeUnit.SECONDS))
                    return transportTaskResponse()
                }
            }
            val api = client()
            val remote = api.todoRemoteApi.forAccount(host.address, host.userID)

            coroutineScope {
                val attempt = async(Dispatchers.IO) { runCatching { remote.find("example-task") } }
                try {
                    responseReady.await(5, TimeUnit.SECONDS) shouldBe true
                    host.userID = "example-other-user"
                    host.apiKey = "example-other-api-key"
                } finally {
                    releaseResponse.countDown()
                }
                (attempt.await().exceptionOrNull() is TodoRemoteScopeChangedException) shouldBe true
            }

            val sent = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
            sent.getHeader("x-api-user") shouldBe "example-user"
            sent.getHeader("x-api-key") shouldBe "example-api-key"
            verify(exactly = 0) { HabiticaBaseApplication.logout(any(), any()) }
        }

        "resolve the newly rebuilt server service instead of retaining the previous one" {
            val api = client()
            val oldRemote = api.todoRemoteApi.forAccount(host.address, host.userID)
            val oldGeneration = api.taskReadGeneration
            val replacement = MockWebServer().apply { start() }
            try {
                api.updateServerUrl(replacement.url("/").toString())
                (api.taskReadGeneration > oldGeneration) shouldBe true
                shouldThrow<TodoRemoteScopeChangedException> { oldRemote.find("example-task") }
                replacement.enqueue(transportTaskResponse())

                api.todoRemoteApi.forAccount(host.address, host.userID).find("example-task")?.id shouldBe "example-task"

                server.requestCount shouldBe 0
                replacement.requestCount shouldBe 1
            } finally {
                replacement.shutdown()
            }
        }
    }
})

/** Supplies a synthetic server task and notification through the production Gson converter. */
private fun transportTaskResponse(): MockResponse = MockResponse()
    .addHeader("Content-Type", "application/json")
    .setBody(
        """{"success":true,"data":{"_id":"example-task","type":"todo","text":"Example task","challenge":{}},"notifications":[{"id":"example-notification","type":"unknown"}]}"""
    )

/** Uses the production model converters without initializing Firebase in JVM tests. */
private suspend fun <T> withModelTraces(block: suspend () -> T): T {
    mockkStatic(FirebasePerformance::class)
    return try {
        val performance = mockk<FirebasePerformance>()
        every { FirebasePerformance.getInstance() } returns performance
        every { performance.newTrace(any()) } returns mockk<Trace>(relaxed = true)
        block()
    } finally {
        unmockkStatic(FirebasePerformance::class)
    }
}

/** Supplies a task-list envelope that would dispatch callbacks if a stale read were accepted. */
private fun transportTaskListResponse(): MockResponse = MockResponse()
    .addHeader("Content-Type", "application/json")
    .setBody("""{"success":true,"data":[],"notifications":[{"id":"example-notification","type":"unknown"}]}""")

/** Supplies synthetic user stats and order for generation-fenced refresh tests. */
private fun transportUserResponse(): MockResponse = MockResponse()
    .addHeader("Content-Type", "application/json")
    .setBody("""{"success":true,"data":{"_id":"example-user","stats":{"gp":12},"tasksOrder":{"todos":[]}},"notifications":[{"id":"example-notification","type":"unknown"}]}""")
