package com.habitrpg.android.habitica.data.sync

import com.google.gson.JsonObject
import com.habitrpg.android.habitica.api.ApiService
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.common.habitica.api.HostConfig
import com.habitrpg.common.habitica.models.HabitResponse
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.ResponseBody.Companion.toResponseBody
import retrofit2.HttpException
import retrofit2.Response
import java.io.IOException
import java.net.SocketException
import java.net.UnknownHostException

/** Tests quiet replay response semantics and account-bound request authentication. */
class TodoRemoteApiTest : WordSpec({
    lateinit var service: ApiService
    lateinit var host: HostConfig
    lateinit var scope: TodoRequestScope
    lateinit var remote: TodoRemoteApi
    var serviceIsCurrent = true

    beforeEach {
        service = mockk()
        host = HostConfig("https://example.com", "", "example-api-key", "example-user")
        serviceIsCurrent = true
        scope = TodoRequestScope(host) { serviceIsCurrent }
        remote = RetrofitTodoRemoteApi(service, scope).forAccount(host.address, host.userID)
    }

    "quiet Todo requests" should {
        "return authoritative user state without separately fetching tasks" {
            val user = User()
            coEvery { service.getTodoUser(any(), scope) } returns todoResponse(user)

            remote.user() shouldBe user
            coVerify(exactly = 1) { service.getTodoUser(any(), scope) }
            coVerify(exactly = 0) { service.getTasks(any<Boolean>()) }
            coVerify(exactly = 0) { service.getUser(any()) }
        }

        "return the authoritative task from lookup" {
            val task = Task()
            coEvery { service.findTodo("example-task", scope) } returns todoResponse(task)

            remote.find("example-task") shouldBe task
            coVerify(exactly = 1) { service.findTodo("example-task", scope) }
        }

        "return null only for a missing task" {
            coEvery { service.findTodo("example-task", scope) } returns todoError(404)

            remote.find("example-task") shouldBe null
        }

        "preserve authentication status and response detail for outbox classification" {
            val errorBody = "{\"error\":\"sessionOutdated\"}"
            coEvery { service.findTodo("example-task", scope) } returns todoError(401, errorBody)

            val failure = shouldThrow<HttpException> { remote.find("example-task") }
            failure.code() shouldBe 401
            failure.response()?.errorBody()?.string() shouldBe errorBody
        }

        "propagate server failures rather than treating them as missing tasks" {
            coEvery { service.findTodo("example-task", scope) } returns todoError(503)

            shouldThrow<HttpException> { remote.find("example-task") }.code() shouldBe 503
        }

        "send create JSON unchanged including fields omitted by TaskSerializer" {
            val payload = JsonObject().apply {
                addProperty("_id", "example-task")
                addProperty("text", "Example task")
                addProperty("type", "todo")
                addProperty("completed", false)
            }
            val task = Task()
            coEvery { service.createTodo(payload, scope) } returns todoResponse(task)

            remote.create(payload) shouldBe task
            coVerify(exactly = 1) { service.createTodo(payload, scope) }
        }

        "send edit JSON unchanged" {
            val payload = JsonObject().apply {
                addProperty("text", "Updated example")
                add("date", com.google.gson.JsonNull.INSTANCE)
            }
            val task = Task()
            coEvery { service.updateTodo("example-task", payload, scope) } returns todoResponse(task)

            remote.update("example-task", payload) shouldBe task
            coVerify(exactly = 1) { service.updateTodo("example-task", payload, scope) }
        }

        "use the full score endpoint for completion and undo" {
            val result = TaskDirectionData()
            coEvery { service.scoreTodo("example-task", "up", scope) } returns todoResponse(result)
            coEvery { service.scoreTodo("example-task", "down", scope) } returns todoResponse(result)

            remote.score("example-task", true) shouldBe result
            remote.score("example-task", false) shouldBe result
            coVerify(exactly = 1) { service.scoreTodo("example-task", "up", scope) }
            coVerify(exactly = 1) { service.scoreTodo("example-task", "down", scope) }
        }

        "retain the checklist scoring operation instead of replacing it with an edit" {
            val task = Task()
            coEvery { service.scoreTodoChecklist("example-task", "example-item", scope) } returns todoResponse(task)

            remote.checklist("example-task", "example-item") shouldBe task
            coVerify(exactly = 1) { service.scoreTodoChecklist("example-task", "example-item", scope) }
            coVerify(exactly = 0) { service.updateTodo(any(), any(), any()) }
        }

        "accept a successful empty delete response" {
            coEvery { service.deleteTodo("example-task", scope) } returns Response.success(204, null)

            remote.delete("example-task")
            coVerify(exactly = 1) { service.deleteTodo("example-task", scope) }
        }

        "accept a successful delete envelope without data" {
            coEvery { service.deleteTodo("example-task", scope) } returns
                Response.success(HabitResponse<Void>().apply { success = true })

            remote.delete("example-task")
        }

        "not mistake a failed delete for success" {
            coEvery { service.deleteTodo("example-task", scope) } returns todoError(404)

            shouldThrow<HttpException> { remote.delete("example-task") }.code() shouldBe 404
        }

        "return the authoritative move order" {
            val order = listOf("example-other", "example-task")
            coEvery { service.moveTodo("example-task", 1, scope) } returns todoResponse(order)

            remote.move("example-task", 1) shouldBe order
        }

        "propagate connectivity exceptions unchanged" {
            listOf(IOException(), UnknownHostException(), SocketException()).forEach { failure ->
                coEvery { service.findTodo("example-task", scope) } throws failure

                shouldThrow<IOException> { remote.find("example-task") } shouldBe failure
            }
        }

        "propagate coroutine cancellation unchanged" {
            val cancellation = CancellationException("Example cancellation")
            coEvery { service.findTodo("example-task", scope) } throws cancellation

            shouldThrow<CancellationException> { remote.find("example-task") } shouldBe cancellation
        }

        "reject a successful response missing required task data" {
            coEvery { service.findTodo("example-task", scope) } returns
                Response.success(HabitResponse<Task>().apply { success = true })

            shouldThrow<TodoRemoteResponseException> { remote.find("example-task") }
        }

        "reject a failed envelope even with HTTP success" {
            coEvery { service.deleteTodo("example-task", scope) } returns
                Response.success(HabitResponse<Void>().apply { success = false })

            shouldThrow<TodoRemoteResponseException> { remote.delete("example-task") }
        }
    }

    "Todo request scope" should {
        "reject a queue belonging to another user before dispatch" {
            shouldThrow<TodoRemoteScopeChangedException> {
                remote.forAccount(host.address, "example-other-user")
            }
            coVerify(exactly = 0) { service.findTodo(any(), any()) }
        }

        "reject a queue belonging to another server" {
            shouldThrow<TodoRemoteScopeChangedException> {
                remote.forAccount("https://other.example.com", host.userID)
            }
        }

        "reject a stale service after the server client is rebuilt" {
            serviceIsCurrent = false

            shouldThrow<TodoRemoteScopeChangedException> { remote.find("example-task") }
            coVerify(exactly = 0) { service.findTodo(any(), any()) }
        }

        "reject credentials changed before dispatch" {
            host.apiKey = "example-replacement-key"

            shouldThrow<TodoRemoteScopeChangedException> { remote.delete("example-task") }
            coVerify(exactly = 0) { service.deleteTodo(any(), any()) }
        }

        "reject a response received after an account switch" {
            coEvery { service.findTodo("example-task", scope) } coAnswers {
                host.userID = "example-other-user"
                todoResponse(Task())
            }

            shouldThrow<TodoRemoteScopeChangedException> { remote.find("example-task") }
        }

        "authenticate only the captured account on its original API origin" {
            val request = Request.Builder().url("https://example.com/api/v4/tasks/example-task").build()
            val authenticated = scope.authenticate(request).build()

            authenticated.header("x-api-user") shouldBe "example-user"
            authenticated.header("x-api-key") shouldBe "example-api-key"
        }

        "reject a changed account at the last network dispatch boundary" {
            val request = Request.Builder().url("https://example.com/api/v4/tasks/example-task").build()
            host.userID = "example-other-user"

            shouldThrow<TodoRemoteScopeChangedException> { scope.authenticate(request) }
        }

        "not attach credentials to redirects outside the captured API origin" {
            listOf(
                "https://other.example.com/api/v4/tasks/example-task",
                "http://example.com/api/v4/tasks/example-task",
                "https://example.com:444/api/v4/tasks/example-task",
                "https://example.com/unrelated"
            ).forEach { url ->
                val request = Request.Builder().url(url).build()

                shouldThrow<TodoRemoteScopeChangedException> { scope.authenticate(request) }
            }
        }
    }
})

/** Creates a successful envelope containing the supplied synthetic result. */
private fun <T> todoResponse(data: T): Response<HabitResponse<T>> =
    Response.success(HabitResponse<T>().apply {
        success = true
        this.data = data
    })

/** Creates a synthetic HTTP failure while retaining its status and body. */
private fun <T> todoError(status: Int, body: String = "{}"): Response<HabitResponse<T>> =
    Response.error(status, body.toResponseBody("application/json".toMediaType()))
