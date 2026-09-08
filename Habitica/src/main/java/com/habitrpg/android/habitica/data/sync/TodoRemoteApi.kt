package com.habitrpg.android.habitica.data.sync

import com.google.gson.JsonObject
import com.habitrpg.android.habitica.api.ApiService
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.common.habitica.api.HostConfig
import com.habitrpg.common.habitica.api.Server
import com.habitrpg.common.habitica.models.HabitResponse
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Request
import retrofit2.HttpException
import retrofit2.Response
import java.io.IOException

/** Executes Todo requests without presentation callbacks or swallowing retryable failures. */
interface TodoRemoteApi {
    /** Binds replay to the exact server and account that own the persisted operations. */
    fun forAccount(server: String, userId: String): TodoRemoteApi

    /** Fetches authoritative user rewards without tasks, notifications, or presentation callbacks. */
    suspend fun user(): User

    /** Fetches current server state, returning null only when the task does not exist. */
    suspend fun find(id: String): Task?

    /** Creates a task with the complete persisted JSON payload. */
    suspend fun create(payload: JsonObject): Task

    /** Applies the persisted edit to the task without reserializing a partial model. */
    suspend fun update(id: String, payload: JsonObject): Task

    /** Replays the original scoring operation, including server-side quest effects. */
    suspend fun score(id: String, up: Boolean): TaskDirectionData

    /** Replays the original checklist toggle, retaining server-side webhook effects. */
    suspend fun checklist(id: String, itemId: String): Task

    /** Deletes a task; a successful empty response still means success. */
    suspend fun delete(id: String)

    /** Moves a task and returns the server's updated order. */
    suspend fun move(id: String, position: Int): List<String>
}

/** Stops a queued request after account or server selection changes, without exposing identifiers. */
class TodoRemoteScopeChangedException : IOException("Todo request account or server changed")

/** Reports an invalid successful response without including private response content. */
class TodoRemoteResponseException : IllegalStateException("Todo request returned an invalid response")

/** Keeps credentials in memory and prevents a queued request from borrowing a later login. */
class TodoRequestScope(
    private val hostConfig: HostConfig,
    private val serviceIsCurrent: () -> Boolean
) {
    private val server = hostConfig.address
    private val userId = hostConfig.userID
    private val apiKey = hostConfig.apiKey
    private val baseUrl = Server(server).toString().toHttpUrl()

    /** Checks both the persisted owner and the current authenticated service before replay. */
    fun requireAccount(server: String, userId: String) {
        if (this.server != server || this.userId != userId) {
            throw TodoRemoteScopeChangedException()
        }
        requireCurrent()
    }

    /** Rejects stale gateway instances before any network request can be dispatched. */
    fun requireCurrent() {
        if (userId.isBlank() || apiKey.isBlank() ||
            hostConfig.address != server || hostConfig.userID != userId ||
            hostConfig.apiKey != apiKey || !serviceIsCurrent()
        ) {
            throw TodoRemoteScopeChangedException()
        }
    }

    /** Adds only the captured credentials after checking the live scope and destination. */
    fun authenticate(request: Request): Request.Builder {
        requireCurrent()
        val url = request.url
        if (url.scheme != baseUrl.scheme || url.host != baseUrl.host || url.port != baseUrl.port ||
            !url.encodedPath.startsWith(baseUrl.encodedPath.trimEnd('/') + "/")
        ) {
            throw TodoRemoteScopeChangedException()
        }
        return request.newBuilder()
            .header("x-api-user", userId)
            .header("x-api-key", apiKey)
    }
}

/** Calls the existing authenticated Retrofit service without global error or reward handling. */
class RetrofitTodoRemoteApi(
    private val apiService: ApiService,
    private val scope: TodoRequestScope
) : TodoRemoteApi {
    /** Validates the persisted owner against this current-service snapshot. */
    override fun forAccount(server: String, userId: String): TodoRemoteApi {
        scope.requireAccount(server, userId)
        return this
    }

    /** Refreshes the user after acknowledged scoring without entering the normal UI callback path. */
    override suspend fun user(): User {
        scope.requireCurrent()
        return requireData(apiService.getTodoUser(USER_FIELDS, scope))
    }

    /** Reads uncached state and preserves every failure except a genuine missing task. */
    override suspend fun find(id: String): Task? {
        scope.requireCurrent()
        val response = apiService.findTodo(id, scope)
        scope.requireCurrent()
        if (response.code() == 404) return null
        return requireData(response)
    }

    /** Sends the persisted create body without TaskSerializer field loss. */
    override suspend fun create(payload: JsonObject): Task {
        scope.requireCurrent()
        return requireData(apiService.createTodo(payload, scope))
    }

    /** Sends the exact persisted edit body. */
    override suspend fun update(id: String, payload: JsonObject): Task {
        scope.requireCurrent()
        return requireData(apiService.updateTodo(id, payload, scope))
    }

    /** Invokes server scoring without any local reward presentation. */
    override suspend fun score(id: String, up: Boolean): TaskDirectionData {
        scope.requireCurrent()
        return requireData(apiService.scoreTodo(id, if (up) "up" else "down", scope))
    }

    /** Invokes server checklist scoring without presentation callbacks. */
    override suspend fun checklist(id: String, itemId: String): Task {
        scope.requireCurrent()
        return requireData(apiService.scoreTodoChecklist(id, itemId, scope))
    }

    /** Accepts HTTP success with either a Habitica envelope or an empty body. */
    override suspend fun delete(id: String) {
        scope.requireCurrent()
        val response = apiService.deleteTodo(id, scope)
        requireSuccess(response)
    }

    /** Replays reordering through the same endpoint used by online task moves. */
    override suspend fun move(id: String, position: Int): List<String> {
        scope.requireCurrent()
        return requireData(apiService.moveTodo(id, position, scope))
    }

    /** Retains HTTP status and error body so the outbox can classify server failures. */
    private fun <T> requireSuccess(response: Response<HabitResponse<T>>) {
        scope.requireCurrent()
        if (!response.isSuccessful) throw HttpException(response)
        if (response.body()?.success == false) throw TodoRemoteResponseException()
    }

    /** Requires a task result only for endpoints that promise response data. */
    private fun <T> requireData(response: Response<HabitResponse<T>>): T {
        requireSuccess(response)
        return response.body()?.data ?: throw TodoRemoteResponseException()
    }

    companion object {
        private const val USER_FIELDS =
            "balance,items,permissions,challenges,lastCron,needsCron,loginIncentives,achievements," +
                "backer,contributor,purchased,invitations,party,profile,stats,tasksOrder,pushDevices," +
                "tags,pinnedItems,unpinnedItems,pinnedItemsOrder"
    }
}
