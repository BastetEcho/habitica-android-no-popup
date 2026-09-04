package com.habitrpg.android.habitica.data.implementation

import android.util.Log
import com.google.gson.Gson
import com.google.gson.annotations.SerializedName
import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.TaskServerState
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.OfflineTaskSyncScheduler
import com.habitrpg.android.habitica.data.sync.canQueueOfflineCreation
import com.habitrpg.android.habitica.data.sync.canQueueOfflineTodoOperation
import com.habitrpg.android.habitica.data.sync.canQueueOfflineTodoScore
import com.habitrpg.android.habitica.data.sync.hasPendingTodoScore
import com.habitrpg.android.habitica.data.sync.hasQueuedOfflineTodoOperation
import com.habitrpg.android.habitica.data.sync.isQueuedOfflineTodoCompletion
import com.habitrpg.android.habitica.data.sync.offlineCreateAlias
import com.habitrpg.android.habitica.helpers.Analytics
import com.habitrpg.android.habitica.helpers.AppConfigManager
import com.habitrpg.android.habitica.helpers.EventCategory
import com.habitrpg.android.habitica.helpers.HitType
import com.habitrpg.android.habitica.interactors.ScoreTaskLocallyInteractor
import com.habitrpg.android.habitica.models.BaseMainObject
import com.habitrpg.android.habitica.models.Tag
import com.habitrpg.android.habitica.models.responses.BulkTaskScoringData
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.RemindersItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.OwnedItem
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.common.habitica.api.normalizeServerOrigin
import com.habitrpg.common.habitica.helpers.launchCatching
import com.habitrpg.shared.habitica.models.responses.TaskDirection
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.responses.TaskScoringResult
import com.habitrpg.shared.habitica.models.tasks.TaskType
import com.habitrpg.shared.habitica.models.tasks.TasksOrder
import io.realm.RealmList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.MainScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

private val offlineTaskSyncMutex = Mutex()
private val recentTaskIDReplacements = mutableMapOf<Triple<String, String, String>, String>()
private const val OFFLINE_TASK_SYNC_LOG_TAG = "OfflineTaskSync"
private const val EDIT_TEXT = "text"
private const val EDIT_NOTES = "notes"
private const val EDIT_PRIORITY = "priority"
private const val EDIT_ATTRIBUTE = "attribute"
private const val EDIT_TAGS = "tags"
private const val EDIT_DUE_DATE = "dueDate"
private const val EDIT_CHECKLIST = "checklist"
private const val EDIT_REMINDERS = "reminders"
private val TODO_EDIT_FIELDS =
    setOf(
        EDIT_TEXT,
        EDIT_NOTES,
        EDIT_PRIORITY,
        EDIT_ATTRIBUTE,
        EDIT_TAGS,
        EDIT_DUE_DATE,
        EDIT_CHECKLIST,
        EDIT_REMINDERS,
    )
private val todoScoreBoundaryGson = Gson()
private const val LEGACY_TODO_SCORE_BOUNDARY_VERSION = 1
private const val TODO_SCORE_BOUNDARY_VERSION = 2

private data class TodoScoreBoundary(
    @SerializedName("version") val version: Int = TODO_SCORE_BOUNDARY_VERSION,
    @SerializedName("serverOrigin") val serverOrigin: String? = null,
    @SerializedName("direction") val direction: String,
    @SerializedName("requiresCreate") val requiresCreate: Boolean,
    @SerializedName("editFields") val editFields: List<String>,
    @SerializedName("position") val position: Int?,
    @SerializedName("checklistTargets") val checklistTargets: List<TodoChecklistTarget>,
    @SerializedName("task") val task: TodoTaskSnapshot,
)

private data class TodoChecklistTarget(
    @SerializedName("id") val id: String?,
    @SerializedName("text") val text: String?,
    @SerializedName("position") val position: Int,
    @SerializedName("completed") val completed: Boolean,
)

private data class TodoChecklistSnapshot(
    @SerializedName("id") val id: String?,
    @SerializedName("text") val text: String?,
    @SerializedName("position") val position: Int,
    @SerializedName("completed") val completed: Boolean,
)

private data class TodoReminderSnapshot(
    @SerializedName("id") val id: String?,
    @SerializedName("startDate") val startDate: String?,
    @SerializedName("time") val time: String?,
    @SerializedName("type") val type: String?,
)

private data class TodoTaskSnapshot(
    @SerializedName("id") val id: String?,
    @SerializedName("alias") val alias: String?,
    @SerializedName("ownerID") val ownerID: String,
    @SerializedName("text") val text: String,
    @SerializedName("notes") val notes: String?,
    @SerializedName("priority") val priority: Float,
    @SerializedName("attribute") val attribute: String?,
    @SerializedName("tagIDs") val tagIDs: List<String>,
    @SerializedName("dueDate") val dueDate: Long?,
    @SerializedName("value") val value: Double,
    @SerializedName("completed") val completed: Boolean,
    @SerializedName("position") val position: Int,
    @SerializedName("checklist") val checklist: List<TodoChecklistSnapshot>,
    @SerializedName("reminders") val reminders: List<TodoReminderSnapshot>,
)

internal fun clearTaskIDReplacements() {
    recentTaskIDReplacements.clear()
}

@ExperimentalCoroutinesApi
class TaskRepositoryImpl(
    localRepository: TaskLocalRepository,
    apiClient: ApiClient,
    authenticationHandler: AuthenticationHandler,
    val appConfigManager: AppConfigManager,
    private val offlineTaskSyncScheduler: OfflineTaskSyncScheduler
) : BaseRepositoryImpl<TaskLocalRepository>(localRepository, apiClient, authenticationHandler),
    TaskRepository {
    private val lastTaskActions = mutableMapOf<String, Long>()

    private fun accountMatches(expectedUserID: String): Boolean {
        return expectedUserID.isNotBlank() && currentUserID == expectedUserID
    }

    /** Confirms both halves of the durable outbox identity are still active. */
    private fun outboxIdentityMatches(
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Boolean {
        val authentication = apiClient.hostConfig.authenticationSnapshot()
        return accountMatches(expectedUserID) && expectedServerOrigin.isNotBlank() &&
            authentication.userID == expectedUserID && authentication.apiKey.isNotBlank() &&
            authentication.serverOrigin == expectedServerOrigin
    }

    /** Confirms that one queued row belongs to the active account and API server. */
    private fun taskOutboxIdentityMatches(
        task: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Boolean {
        return outboxIdentityMatches(expectedUserID, expectedServerOrigin) &&
            task.ownerID == expectedUserID && task.outboxServerOrigin == expectedServerOrigin
    }

    /** Binds legacy unscoped outbox state before its first request. */
    private fun bindTaskOutboxIdentity(
        task: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task? {
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin) ||
            task.ownerID != expectedUserID
        ) {
            return null
        }
        val queuedTask = localRepository.getUnmanagedCopy(task)
        if (!queuedTask.hasQueuedOfflineTodoOperation()) {
            if (queuedTask.outboxServerOrigin != null) {
                queuedTask.outboxServerOrigin = null
                localRepository.save(queuedTask)
            }
            return queuedTask
        }
        when {
            queuedTask.outboxServerOrigin.isNullOrBlank() -> {
                queuedTask.outboxServerOrigin = expectedServerOrigin
                localRepository.save(queuedTask)
            }
            queuedTask.outboxServerOrigin != expectedServerOrigin -> return null
        }
        return queuedTask
    }

    /** Stamps newly queued work and rejects an existing row from another server. */
    private fun stampTaskOutboxIdentity(task: Task): String? {
        val authentication = apiClient.hostConfig.authenticationSnapshot()
        val serverOrigin = authentication.serverOrigin
        if (!accountMatches(task.ownerID) || authentication.userID != task.ownerID ||
            authentication.apiKey.isBlank() || serverOrigin.isBlank()
        ) {
            return null
        }
        if (task.outboxServerOrigin.isNullOrBlank()) {
            task.outboxServerOrigin = serverOrigin
        }
        return serverOrigin.takeIf { task.outboxServerOrigin == it }
    }

    /** Removes identity metadata only after every durable operation is acknowledged. */
    private fun clearTaskOutboxIdentityIfIdle(task: Task) {
        if (!task.hasQueuedOfflineTodoOperation()) {
            task.outboxServerOrigin = null
        }
    }

    /** Hides retained pending rows that came from a different API server. */
    private fun taskIsVisibleForActiveServer(task: Task): Boolean {
        val queuedOrigin = task.outboxServerOrigin
        return queuedOrigin.isNullOrBlank() || queuedOrigin == apiClient.hostConfig.serverOrigin()
    }

    override fun currentUserIDForSync(): String = currentUserID

    override fun refreshLocalData() {
        val r = localRepository.realm
        if (r.isClosed) return
        try {
            r.refresh()
        } catch (_: IllegalStateException) {
        }
    }

    override fun getTasks(
        taskType: TaskType,
        userID: String?,
        includedGroupIDs: Array<String>
    ): Flow<List<Task>> =
        this.localRepository.getTasks(
            taskType,
            userID ?: authenticationHandler.currentUserID ?: "",
            includedGroupIDs
        ).map { tasks -> tasks.filter(::taskIsVisibleForActiveServer) }

    override fun saveTasks(
        userId: String,
        order: TasksOrder,
        tasks: TaskList
    ) {
        localRepository.saveTasks(userId, order, tasks)
    }

    override suspend fun retrieveTasks(
        userId: String,
        tasksOrder: TasksOrder
    ): TaskList? {
        val tasks = apiClient.getTasks() ?: return null
        this.localRepository.saveTasks(userId, tasksOrder, tasks)
        return tasks
    }

    override suspend fun retrieveCompletedTodos(userId: String?): TaskList? {
        val taskList = this.apiClient.getTasks("completedTodos") ?: return null
        val tasks = taskList.tasks
        this.localRepository.saveCompletedTodos(
            userId ?: authenticationHandler.currentUserID ?: "",
            tasks.values
        )
        return taskList
    }

    override suspend fun retrieveTasks(
        userId: String,
        tasksOrder: TasksOrder,
        dueDate: Date
    ): TaskList? {
        val formatter = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.US)
        val taskList = this.apiClient.getTasks("dailys", formatter.format(dueDate)) ?: return null
        this.localRepository.saveTasks(userId, tasksOrder, taskList)
        return taskList
    }

    @Suppress("ReturnCount")
    override suspend fun taskChecked(
        user: User?,
        task: Task,
        up: Boolean,
        force: Boolean,
        notifyFunc: ((TaskScoringResult) -> Unit)?
    ): TaskScoringResult? {
        return if (force) {
            taskCheckedInternal(user, task, up, force, notifyFunc)
        } else {
            offlineTaskSyncMutex.withLock {
                taskCheckedInternal(
                    user,
                    getLatestLocalTask(task),
                    up,
                    force,
                    notifyFunc,
                )
            }
        }
    }

    @Suppress("ReturnCount")
    private suspend fun taskCheckedInternal(
        user: User?,
        task: Task,
        up: Boolean,
        force: Boolean,
        notifyFunc: ((TaskScoringResult) -> Unit)?,
        expectedUserID: String? = null,
    ): TaskScoringResult? {
        val pinnedUserID =
            expectedUserID ?: task.ownerID.takeIf {
                task.canQueueOfflineTodoOperation() && it.isNotBlank()
            }
        val pinnedServerOrigin =
            pinnedUserID?.let {
                task.outboxServerOrigin?.takeIf(String::isNotBlank)
                    ?: apiClient.hostConfig.serverOrigin()
            }
        if (pinnedUserID != null &&
            !outboxIdentityMatches(pinnedUserID, pinnedServerOrigin.orEmpty())
        ) {
            return null
        }
        if (task.pendingDelete) return null

        val canQueueTodoScore = task.canQueueOfflineTodoScore()
        val hadPendingTodoScore = !force && task.hasPendingTodoScore()
        val isQueuedTaskCreation = task.pendingCreate && task.canQueueOfflineTodoOperation()
        val hasQueuedPrerequisite =
            isQueuedTaskCreation || task.pendingUpdate || task.pendingChecklist ||
                task.pendingPosition
        val wasQueuedTodoScore = task.isQueuedOfflineTodoCompletion()
        val deferTodoEffects = task.canQueueOfflineTodoOperation()
        val localData =
            if (!deferTodoEffects && user != null && appConfigManager.enableLocalTaskScoring()) {
                ScoreTaskLocallyInteractor.score(
                    user,
                    task,
                    if (up) TaskDirection.UP else TaskDirection.DOWN
                )
            } else {
                null
            }
        if (user != null && localData != null) {
            val stats = user.stats
            val result = TaskScoringResult(localData, stats)
            notifyFunc?.invoke(result)

            handleTaskResponse(user, localData, task, up, 0f)
        }
        if (!force && hasQueuedPrerequisite && task.type == TaskType.TODO) {
            queueTaskOperation(
                task,
                pendingScoreDirection = if (up) TaskDirection.UP else TaskDirection.DOWN,
                pendingReorder = task.pendingPosition,
                requiresCreation = task.pendingCreate,
            )
            return null
        }

        val id = task.id ?: return null
        if (!force && !canQueueTodoScore) {
            val now = Date().time
            val lastTaskAction = lastTaskActions[id] ?: 0L
            if (lastTaskAction > now - 500) return null
            lastTaskActions[id] = now
        }

        if (!force && canQueueTodoScore) {
            queueTaskOperation(
                task,
                pendingScoreDirection = if (up) TaskDirection.UP else TaskDirection.DOWN,
                requiresCreation = false,
            )
            if (hadPendingTodoScore) return null
        }
        val serverTaskID =
            if (!force && canQueueTodoScore) {
                getExactServerTask(
                    id,
                    pinnedUserID ?: return null,
                    pinnedServerOrigin ?: return null,
                )?.id ?: return null
            } else {
                id
            }
        val res =
            this.apiClient.postTaskDirection(
                serverTaskID,
                (if (up) TaskDirection.UP else TaskDirection.DOWN).text,
                suppressConnectionErrors = canQueueTodoScore,
                expectedUserID = pinnedUserID,
                expectedServerOrigin = pinnedServerOrigin,
            )
        if (res == null) return null
        if (pinnedUserID != null &&
            !outboxIdentityMatches(pinnedUserID, pinnedServerOrigin.orEmpty())
        ) {
            return null
        }
        // There are cases where the user object is not set correctly. So the app refetches it as a fallback
        val thisUser =
            user ?: localRepository.getUser(pinnedUserID ?: currentUserID).firstOrNull()
                ?: return if (wasQueuedTodoScore || canQueueTodoScore) TaskScoringResult() else null
        if (pinnedUserID != null &&
            !outboxIdentityMatches(pinnedUserID, pinnedServerOrigin.orEmpty())
        ) {
            return null
        }
        // save local task changes

        Analytics.sendEvent(
            "task_scored",
            EventCategory.BEHAVIOUR,
            HitType.EVENT,
            mapOf(
                "type" to (task.type ?: ""),
                "scored_up" to up,
                "value" to task.value
            )
        )
        if (res.lvl == 0 && !task.canQueueOfflineTodoOperation()) {
            // Team tasks that require approval have weird data that we should just ignore.
            return TaskScoringResult()
        }
        val result = TaskScoringResult(res, thisUser.stats)
        handleTaskResponse(thisUser, res, task, up, localData?.delta ?: 0f)
        if (localData == null) {
            notifyFunc?.invoke(result)
        }
        return result
    }

    private fun queueTaskOperation(
        task: Task,
        pendingScoreDirection: TaskDirection? = null,
        pendingReorder: Boolean = false,
        newPosition: Int? = null,
        requiresCreation: Boolean = task.pendingCreate,
    ): Task {
        val pendingTask = localRepository.getUnmanagedCopy(task)
        pendingTask.pendingCreate = requiresCreation
        if (pendingReorder) {
            pendingTask.pendingPosition = true
        }
        newPosition?.let { pendingTask.position = it }
        if (pendingScoreDirection != null && pendingTask.type == TaskType.TODO) {
            if (stampTaskOutboxIdentity(pendingTask) == null) return task
            val frozenDirection =
                when {
                    pendingTask.pendingScoreUp -> TaskDirection.UP
                    pendingTask.pendingScoreDown -> TaskDirection.DOWN
                    else -> pendingScoreDirection
                }
            freezeTodoScoreBoundary(pendingTask, frozenDirection)
        }
        if (stampTaskOutboxIdentity(pendingTask) == null) return task
        pendingTask.isSaving = false
        pendingTask.hasErrored = false
        localRepository.save(pendingTask)
        offlineTaskSyncScheduler.enqueue(pendingTask.ownerID)
        Log.i(OFFLINE_TASK_SYNC_LOG_TAG, "Queued one local task operation for server sync.")
        return pendingTask
    }

    /** Freezes everything that must reach the server before a queued To Do score. */
    private fun freezeTodoScoreBoundary(
        task: Task,
        direction: TaskDirection,
    ) {
        val authoritativeDirection =
            parseTodoScoreBoundary(task.pendingScoreSnapshot)?.direction?.let { storedDirection ->
                if (storedDirection == TaskDirection.UP.text) TaskDirection.UP else TaskDirection.DOWN
            } ?: direction
        if (task.pendingScoreSnapshot == null) {
            task.pendingScoreSnapshot =
                todoScoreBoundaryGson.toJson(createTodoScoreBoundary(task, authoritativeDirection))
            task.pendingUpdate = false
            task.pendingEditFields = null
            task.pendingPosition = false
            task.pendingChecklist = false
            task.checklist.orEmpty().forEach { it.pendingSync = false }
        }
        task.pendingScoreUp = authoritativeDirection == TaskDirection.UP
        task.pendingScoreDown = authoritativeDirection == TaskDirection.DOWN
    }

    /** Captures a stable, versioned score boundary without serializing Realm internals. */
    private fun createTodoScoreBoundary(
        task: Task,
        direction: TaskDirection,
    ): TodoScoreBoundary {
        val pendingEditFields = parsePendingEditFields(task.pendingEditFields)
        val capturedEditFields =
            if (task.pendingUpdate && pendingEditFields.isEmpty()) {
                TODO_EDIT_FIELDS
            } else {
                pendingEditFields
            }
        return TodoScoreBoundary(
            serverOrigin = task.outboxServerOrigin,
            direction = direction.text,
            requiresCreate = task.pendingCreate,
            editFields = TODO_EDIT_FIELDS.filter(capturedEditFields::contains),
            position = task.position.takeIf { task.pendingPosition },
            checklistTargets =
                task.checklist.orEmpty().filter { it.pendingSync }.map { item ->
                    TodoChecklistTarget(item.id, item.text, item.position, item.completed)
                },
            task =
                TodoTaskSnapshot(
                    id = task.id,
                    alias = task.alias,
                    ownerID = task.ownerID,
                    text = task.text,
                    notes = task.notes,
                    priority = task.priority,
                    attribute = task.attribute?.value,
                    tagIDs = task.tags.orEmpty().map { it.id },
                    dueDate = task.dueDate?.time,
                    value = task.value,
                    completed = task.completed,
                    position = task.position,
                    checklist =
                        task.checklist.orEmpty().map { item ->
                            TodoChecklistSnapshot(
                                item.id,
                                item.text,
                                item.position,
                                item.completed,
                            )
                        },
                    reminders =
                        task.reminders.orEmpty().map { reminder ->
                            TodoReminderSnapshot(
                                reminder.id,
                                reminder.startDate,
                                reminder.time,
                                reminder.type,
                            )
                        },
                ),
        )
    }

    /** Parses and validates a persisted score boundary; invalid data stays queued for retry. */
    private fun parseTodoScoreBoundary(value: String?): TodoScoreBoundary? {
        if (value == null) return null
        return runCatching {
            todoScoreBoundaryGson.fromJson(value, TodoScoreBoundary::class.java)
        }.getOrNull()?.takeIf { boundary ->
            runCatching {
                    boundary.version in
                        LEGACY_TODO_SCORE_BOUNDARY_VERSION..TODO_SCORE_BOUNDARY_VERSION &&
                    (
                        boundary.version == LEGACY_TODO_SCORE_BOUNDARY_VERSION &&
                            boundary.serverOrigin.isNullOrBlank() ||
                            boundary.version == TODO_SCORE_BOUNDARY_VERSION &&
                            !boundary.serverOrigin.isNullOrBlank() &&
                            normalizeServerOrigin(boundary.serverOrigin) == boundary.serverOrigin
                    ) &&
                    boundary.direction in setOf(TaskDirection.UP.text, TaskDirection.DOWN.text) &&
                    boundary.editFields.all { it in TODO_EDIT_FIELDS } &&
                    boundary.checklistTargets.all {
                        it.position >= 0 && (!it.id.isNullOrBlank() || !it.text.isNullOrBlank())
                    } &&
                    boundary.task.ownerID.isNotBlank() &&
                    boundary.task.text.isNotBlank() &&
                    boundary.task.tagIDs.all { it.isNotBlank() } &&
                    boundary.task.checklist.all {
                        it.position >= 0 && (!it.id.isNullOrBlank() || !it.text.isNullOrBlank())
                    } &&
                    boundary.task.reminders.all {
                        (it.id == null || it.id.isNotBlank()) &&
                            (it.startDate == null || it.startDate.isNotBlank()) &&
                            (it.time == null || it.time.isNotBlank()) &&
                            (it.type == null || it.type.isNotBlank())
                    }
            }.getOrDefault(false)
        }
    }

    /** Recreates the API-facing To Do state stored at the score boundary. */
    private fun taskFromTodoScoreBoundary(boundary: TodoScoreBoundary): Task {
        val snapshot = boundary.task
        return Task().apply {
            id = snapshot.id
            ownerID = snapshot.ownerID
            alias = snapshot.alias
            type = TaskType.TODO
            text = snapshot.text
            notes = snapshot.notes
            priority = snapshot.priority
            attribute = com.habitrpg.shared.habitica.models.tasks.Attribute.from(snapshot.attribute)
            tags = RealmList<Tag>().apply {
                snapshot.tagIDs.forEach { tagID -> add(Tag().apply { id = tagID }) }
            }
            dueDate = snapshot.dueDate?.let(::Date)
            value = snapshot.value
            completed = snapshot.completed
            position = snapshot.position
            checklist = RealmList<ChecklistItem>().apply {
                snapshot.checklist.forEach { item ->
                    add(
                        ChecklistItem(item.id, item.text, item.completed).apply {
                            position = item.position
                            pendingSync = false
                        }
                    )
                }
            }
            reminders = RealmList<RemindersItem>().apply {
                snapshot.reminders.forEach { reminder ->
                    add(
                        RemindersItem().apply {
                            id = reminder.id
                            startDate = reminder.startDate
                            time = reminder.time
                            type = reminder.type
                        }
                    )
                }
            }
            pendingCreate = boundary.requiresCreate
            pendingScoreUp = boundary.direction == TaskDirection.UP.text
            pendingScoreDown = boundary.direction == TaskDirection.DOWN.text
            outboxServerOrigin = boundary.serverOrigin
        }
    }

    private suspend fun clearQueuedTodoScore(
        task: Task,
        confirmedCompleted: Boolean,
        expectedUserID: String,
        expectedServerOrigin: String,
    ) {
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return
        val queuedTask = getLatestLocalTask(task)
        if (!taskOutboxIdentityMatches(queuedTask, expectedUserID, expectedServerOrigin)) return
        queuedTask.completeForUser(queuedTask.ownerID, confirmedCompleted)
        queuedTask.pendingScoreUp = false
        queuedTask.pendingScoreDown = false
        queuedTask.pendingScoreSnapshot = null
        queuedTask.pendingScoreRefresh = true
        queuedTask.hasErrored = false
        queuedTask.isSaving = false
        clearTaskOutboxIdentityIfIdle(queuedTask)
        localRepository.save(queuedTask)
    }

    override suspend fun bulkScoreTasks(data: List<Map<String, String>>): BulkTaskScoringData? {
        return apiClient.bulkScoreTasks(data)
    }

    private fun handleTaskResponse(
        user: User,
        res: TaskDirectionData,
        task: Task,
        up: Boolean,
        localDelta: Float
    ) {
        this.localRepository.executeTransaction { transactionRealm ->
            task.id ?: return@executeTransaction
            val bgTask = localRepository.getLiveObject(task) ?: task
            val userID = user.id ?: task.ownerID
            val bgUser = localRepository.getLiveObject(user) ?: user
            if (bgTask.type != TaskType.REWARD && (bgTask.value - localDelta) + res.delta != bgTask.value) {
                bgTask.value = (bgTask.value - localDelta) + res.delta
                if (TaskType.DAILY == bgTask.type) {
                    if (up) {
                        bgTask.streak = (bgTask.streak ?: 0) + 1
                    } else {
                        bgTask.streak = (bgTask.streak ?: 0) - 1
                    }
                } else if (TaskType.HABIT == bgTask.type) {
                    if (up) {
                        bgTask.counterUp = (bgTask.counterUp ?: 0) + 1
                    } else {
                        bgTask.counterDown = (bgTask.counterDown ?: 0) + 1
                    }
                }
            }

            if (TaskType.DAILY == bgTask.type || TaskType.TODO == bgTask.type) {
                bgTask.completeForUser(userID, up)
                if (TaskType.TODO == bgTask.type && bgTask.canQueueOfflineTodoOperation()) {
                    bgTask.pendingScoreUp = false
                    bgTask.pendingScoreDown = false
                    bgTask.pendingScoreSnapshot = null
                    bgTask.pendingScoreRefresh = true
                    bgTask.hasErrored = false
                    bgTask.isSaving = false
                }
                if (bgTask.isGroupTask) {
                    val entry =
                        bgTask.group?.assignedUsersDetail?.firstOrNull { it.assignedUserID == user.id }
                    entry?.completed = up
                    if (up) {
                        entry?.completedDate = Date()
                    } else {
                        entry?.completedDate = null
                    }
                }
            }

            val taskId = bgTask.id
            if (taskId != null) {
                try {
                    transactionRealm.where(Task::class.java).equalTo("id", taskId).findAll().forEach { sibling ->
                        if (sibling.ownerID != bgTask.ownerID) {
                            sibling.value = bgTask.value
                            sibling.streak = bgTask.streak
                            sibling.completed = bgTask.completed
                            sibling.counterUp = bgTask.counterUp
                            sibling.counterDown = bgTask.counterDown
                            if (sibling.isGroupTask) {
                                sibling.group?.assignedUsersDetail
                                    ?.firstOrNull { detail -> detail.assignedUserID == user.id }
                                    ?.let { detail ->
                                        detail.completed = up
                                        detail.completedDate = if (up) Date() else null
                                    }
                            }
                        }
                    }
                } catch (_: IllegalStateException) {
                    // The Realm can be closed while a local scoring callback is finishing.
                }
            }
            res._tmp?.drop?.key?.let { key ->
                val type =
                    when (res._tmp?.drop?.type?.lowercase(Locale.US)) {
                        "hatchingpotion" -> "hatchingPotions"
                        "egg" -> "eggs"
                        else -> res._tmp?.drop?.type?.lowercase(Locale.US)
                    }
                var item =
                    transactionRealm.where(OwnedItem::class.java).equalTo("itemType", type).equalTo("key", key)
                        .findFirst()
                if (item == null) {
                    item = OwnedItem()
                    item.key = key
                    item.itemType = type
                    item.userID = user.id

                    when (type) {
                        "eggs" -> bgUser.items?.eggs?.add(item)
                        "food" -> bgUser.items?.food?.add(item)
                        "hatchingPotions" -> bgUser.items?.hatchingPotions?.add(item)
                        "quests" -> bgUser.items?.quests?.add(item)
                    }
                }
                item.numberOwned += 1
            }

            bgUser.stats?.hp = res.hp
            bgUser.stats?.exp = res.exp
            bgUser.stats?.mp = res.mp
            bgUser.stats?.gp = res.gp
            bgUser.stats?.lvl = res.lvl
            bgUser.party?.quest?.progress?.up = (
                bgUser.party?.quest?.progress?.up
                    ?: 0F
                ) + (res._tmp?.quest?.progressDelta?.toFloat() ?: 0F)
            if (!bgTask.isManaged) {
                localRepository.save(bgTask)
            }
            if (!bgUser.isManaged) {
                localRepository.save(bgUser)
            }
        }
    }

    override suspend fun markTaskNeedsWork(
        task: Task,
        userID: String
    ) {
        val savedTask = apiClient.markTaskNeedsWork(task.id ?: "", userID)
        if (savedTask != null) {
            savedTask.id = task.id
            savedTask.position = task.position
            savedTask.group?.assignedUsersDetail?.firstOrNull { it.assignedUserID == userID }?.let {
                it.completed = false
                it.completedDate = null
            }
            localRepository.save(savedTask)
        }
    }

    override suspend fun taskChecked(
        user: User?,
        taskId: String,
        up: Boolean,
        force: Boolean,
        notifyFunc: ((TaskScoringResult) -> Unit)?
    ): TaskScoringResult? {
        return if (force) {
            taskCheckedByIDInternal(user, taskId, up, force, notifyFunc)
        } else {
            offlineTaskSyncMutex.withLock {
                taskCheckedByIDInternal(user, taskId, up, force, notifyFunc)
            }
        }
    }

    private suspend fun taskCheckedByIDInternal(
        user: User?,
        taskId: String,
        up: Boolean,
        force: Boolean,
        notifyFunc: ((TaskScoringResult) -> Unit)?,
    ): TaskScoringResult? {
        val resolvedTaskID = resolveTaskIDUnscoped(taskId)
        val task =
            localRepository.getTask(resolvedTaskID, currentUserID).firstOrNull()
                ?: localRepository.getTask(resolvedTaskID).firstOrNull()
                    ?.takeUnless { it.canQueueOfflineTodoOperation() }
                ?: return null
        return taskCheckedInternal(user, task, up, force, notifyFunc)
    }

    override suspend fun scoreChecklistItem(
        taskId: String,
        itemId: String
    ): Task? {
        return offlineTaskSyncMutex.withLock {
            val resolvedTaskID = resolveTaskIDUnscoped(taskId)
            val localTask =
                localRepository.getTaskCopy(resolvedTaskID, currentUserID).firstOrNull()
                    ?: localRepository.getTaskCopy(resolvedTaskID).firstOrNull()
                        ?.takeUnless { it.canQueueOfflineTodoOperation() }
                ?: return@withLock null
            if (!localTask.canQueueOfflineTodoOperation()) {
                return@withLock scoreChecklistItemInternal(resolvedTaskID, itemId)
            }
            val ownerID = localTask.ownerID
            val expectedServerOrigin =
                localTask.outboxServerOrigin?.takeIf(String::isNotBlank)
                    ?: apiClient.hostConfig.serverOrigin()
            if (!outboxIdentityMatches(ownerID, expectedServerOrigin)) return@withLock null
            if (localTask.pendingDelete) return@withLock null

            val hadPendingChecklist = localTask.pendingChecklist
            val hadPendingScore = localTask.hasPendingTodoScore()
            val pendingTask = queueChecklistScore(localTask, itemId) ?: return@withLock null
            if (pendingTask.pendingCreate || pendingTask.pendingUpdate || hadPendingChecklist ||
                hadPendingScore
            ) {
                return@withLock pendingTask
            }
            val currentServerTask =
                getExactServerTask(resolvedTaskID, ownerID, expectedServerOrigin)
                    ?: return@withLock pendingTask
            val serverTaskID = currentServerTask.id ?: return@withLock pendingTask
            val desiredCompletion =
                pendingTask.checklist?.firstOrNull { it.id == itemId }?.completed
                    ?: return@withLock pendingTask
            val authoritativeItem =
                currentServerTask.checklist?.firstOrNull { it.id == itemId }
            if (authoritativeItem == null) {
                val latestTask = getLatestLocalTask(pendingTask)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        ownerID,
                        expectedServerOrigin,
                    )
                ) {
                    return@withLock pendingTask
                }
                latestTask.checklist?.firstOrNull { it.id == itemId }?.pendingSync = false
                latestTask.pendingChecklist = latestTask.checklist.orEmpty().any { it.pendingSync }
                saveCreatedTask(
                    latestTask,
                    currentServerTask,
                    preservePendingActions = true,
                    preserveServerResponse = true,
                )
                return@withLock getLatestLocalTask(currentServerTask)
            }
            if (authoritativeItem.completed == desiredCompletion) {
                if (!outboxIdentityMatches(ownerID, expectedServerOrigin)) {
                    return@withLock pendingTask
                }
                return@withLock acknowledgeChecklistScore(
                    pendingTask,
                    currentServerTask,
                    itemId,
                    ownerID,
                    expectedServerOrigin,
                )
            }
            val serverTask =
                apiClient.scoreChecklistItem(
                    serverTaskID,
                    itemId,
                    suppressConnectionErrors = true,
                    expectedUserID = ownerID,
                    expectedServerOrigin = expectedServerOrigin,
                )?.takeIf { it.id == serverTaskID } ?: return@withLock pendingTask
            if (!outboxIdentityMatches(ownerID, expectedServerOrigin)) {
                return@withLock pendingTask
            }
            if (serverTask.checklist?.firstOrNull { it.id == itemId }?.completed !=
                desiredCompletion
            ) {
                return@withLock pendingTask
            }
            acknowledgeChecklistScore(
                pendingTask,
                serverTask,
                itemId,
                ownerID,
                expectedServerOrigin,
            )
        }
    }

    private suspend fun scoreChecklistItemInternal(
        taskId: String,
        itemId: String,
    ): Task? {
        val task = apiClient.scoreChecklistItem(taskId, itemId)
        val updatedItem: ChecklistItem? = task?.checklist?.lastOrNull { itemId == it.id }
        if (updatedItem != null) {
            localRepository.save(updatedItem)
        }
        return task
    }

    private fun queueChecklistScore(
        task: Task,
        itemId: String,
    ): Task? {
        val pendingTask = localRepository.getUnmanagedCopy(task)
        val item = pendingTask.checklist?.firstOrNull { it.id == itemId } ?: return null
        item.completed = !item.completed
        item.pendingSync = true
        pendingTask.pendingChecklist = true
        if (stampTaskOutboxIdentity(pendingTask) == null) return null
        pendingTask.isSaving = false
        pendingTask.hasErrored = false
        localRepository.save(pendingTask)
        offlineTaskSyncScheduler.enqueue(pendingTask.ownerID)
        Log.i(OFFLINE_TASK_SYNC_LOG_TAG, "Queued one checklist target for server sync.")
        return pendingTask
    }

    private suspend fun acknowledgeChecklistScore(
        localTask: Task,
        serverTask: Task,
        itemId: String,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task {
        val latestTask = getLatestLocalTask(localTask)
        if (!taskOutboxIdentityMatches(latestTask, expectedUserID, expectedServerOrigin)) {
            return latestTask
        }
        latestTask.checklist?.firstOrNull { it.id == itemId }?.pendingSync = false
        latestTask.pendingChecklist = latestTask.checklist.orEmpty().any { it.pendingSync }
        clearTaskOutboxIdentityIfIdle(latestTask)
        saveCreatedTask(latestTask, serverTask, preservePendingActions = true)
        return getLatestLocalTask(serverTask)
    }

    override fun getTask(taskId: String) =
        localRepository.getTask(resolveTaskIDUnscoped(taskId))
            .filter(::taskIsVisibleForActiveServer)

    override fun getTask(
        taskId: String,
        ownerID: String,
    ) = localRepository.getTask(resolveTaskID(taskId, ownerID), ownerID)
        .filter(::taskIsVisibleForActiveServer)

    override fun getTaskCopy(taskId: String) =
        localRepository.getTaskCopy(resolveTaskIDUnscoped(taskId))
            .filter(::taskIsVisibleForActiveServer)

    override fun getTaskCopy(
        taskId: String,
        ownerID: String,
    ) = localRepository.getTaskCopy(resolveTaskID(taskId, ownerID), ownerID)
        .filter(::taskIsVisibleForActiveServer)

    override suspend fun createTask(
        task: Task,
        force: Boolean
    ): Task? {
        return if (force) {
            createTaskInternal(task, force)
        } else {
            offlineTaskSyncMutex.withLock { createTaskInternal(task, force) }
        }
    }

    private suspend fun createTaskInternal(
        task: Task,
        force: Boolean,
    ): Task? {
        val canQueue = task.canQueueOfflineTodoOperation()
        if (canQueue) {
            ensureChecklistIdentity(task)
        }
        task.isSaving = true
        task.isCreating = true
        task.hasErrored = false
        task.pendingCreate = canQueue
        task.pendingDeleteAfterScore = false
        task.pendingUpdate = false
        task.pendingEditFields = null
        task.pendingPosition = canQueue
        task.pendingScoreUp = false
        task.pendingScoreDown = false
        task.pendingScoreSnapshot = null
        task.pendingScoreRefresh = false
        task.pendingChecklist = false
        task.ownerID =
            if (task.isGroupTask) {
                task.group?.groupID ?: ""
            } else {
                authenticationHandler.currentUserID?.takeIf { it.isNotBlank() } ?: task.ownerID
            }
        if (task.id == null) {
            task.id = UUID.randomUUID().toString()
        }
        if (task.pendingCreate) {
            task.alias = offlineCreateAlias(task.id.orEmpty())
        }
        val expectedServerOrigin =
            if (canQueue) stampTaskOutboxIdentity(task) ?: return null else null
        localRepository.save(task)
        if (canQueue) {
            offlineTaskSyncScheduler.enqueue(task.ownerID)
        }

        val savedTask =
            if (task.isGroupTask) {
                apiClient.createGroupTask(task.group?.groupID ?: "", task)
            } else {
                apiClient.createTask(
                    task,
                    suppressConnectionErrors = canQueue,
                    expectedUserID = task.ownerID.takeIf { canQueue },
                    expectedServerOrigin = expectedServerOrigin,
                )?.takeIf { serverTask ->
                    !canQueue ||
                        (serverTask.alias == task.alias && !serverTask.id.isNullOrBlank())
                }
            }
        if (canQueue &&
            !outboxIdentityMatches(task.ownerID, expectedServerOrigin.orEmpty())
        ) {
            return null
        }
        val latestLocalTask =
            task.id?.let { localRepository.getTaskCopy(it, task.ownerID).firstOrNull() } ?: task
        if (savedTask != null) {
            saveCreatedTask(
                latestLocalTask,
                savedTask,
                preservePendingActions = true,
            )
        } else {
            latestLocalTask.hasErrored = !canQueue
            latestLocalTask.isSaving = false
            latestLocalTask.pendingCreate = canQueue
            localRepository.save(latestLocalTask)
        }
        return savedTask
    }

    private fun saveCreatedTask(
        localTask: Task,
        serverTask: Task,
        preservePendingActions: Boolean = false,
        preserveServerResponse: Boolean = false,
    ) {
        if (preservePendingActions && localTask.hasQueuedOfflineTodoOperation()) {
            val expectedServerOrigin = localTask.outboxServerOrigin ?: return
            if (!taskOutboxIdentityMatches(
                    localTask,
                    localTask.ownerID,
                    expectedServerOrigin,
                )
            ) {
                return
            }
        }
        val savedTask =
            if (preserveServerResponse) copyTodoServerTask(serverTask) else serverTask
        val localTaskID = localTask.id
        val shouldReplaceLocalTask = savedTask.id != localTaskID && localTaskID != null
        if (savedTask.ownerID.isBlank()) {
            savedTask.ownerID = localTask.ownerID
        }
        if (savedTask.alias.isNullOrBlank()) {
            savedTask.alias = localTask.alias
        }
        savedTask.dateCreated = savedTask.dateCreated ?: Date()
        savedTask.position = localTask.position
        savedTask.tags = localTask.tags
        savedTask.isCreating = false
        savedTask.isSaving = false
        savedTask.hasErrored = false
        savedTask.pendingCreate = false
        savedTask.pendingDelete = preservePendingActions && localTask.pendingDelete
        savedTask.pendingDeleteAfterScore =
            preservePendingActions && localTask.pendingDeleteAfterScore
        savedTask.pendingUpdate = preservePendingActions && localTask.pendingUpdate
        savedTask.pendingEditFields =
            localTask.pendingEditFields.takeIf { savedTask.pendingUpdate }
        savedTask.pendingPosition = preservePendingActions && localTask.pendingPosition
        savedTask.pendingScoreUp = preservePendingActions && localTask.pendingScoreUp
        savedTask.pendingScoreDown = preservePendingActions && localTask.pendingScoreDown
        savedTask.pendingScoreSnapshot =
            localTask.pendingScoreSnapshot.takeIf { preservePendingActions }
        savedTask.pendingScoreRefresh =
            preservePendingActions && localTask.pendingScoreRefresh
        savedTask.pendingChecklist = preservePendingActions && localTask.pendingChecklist
        savedTask.outboxServerOrigin =
            localTask.outboxServerOrigin.takeIf {
                preservePendingActions && savedTask.hasQueuedOfflineTodoOperation()
            }
        if (savedTask.pendingUpdate) {
            parsePendingEditFields(savedTask.pendingEditFields).forEach { fieldName ->
                copyEditableField(
                    savedTask,
                    localTask,
                    fieldName,
                    preserveTargetChecklistCompletion = true,
                )
            }
        }
        restorePendingChecklistState(localTask, savedTask)
        clearTaskOutboxIdentityIfIdle(savedTask)
        if (shouldReplaceLocalTask && localTaskID != null) {
            savedTask.id?.let { replacementTaskID ->
                val serverOrigin =
                    localTask.outboxServerOrigin ?: apiClient.hostConfig.serverOrigin()
                recentTaskIDReplacements.entries
                    .filter {
                        it.key.first == localTask.ownerID && it.key.second == serverOrigin &&
                            it.value == localTaskID
                    }
                    .forEach { it.setValue(replacementTaskID) }
                recentTaskIDReplacements[
                    Triple(localTask.ownerID, serverOrigin, localTaskID)
                ] = replacementTaskID
            }
            localRepository.replaceTask(localTaskID, localTask.ownerID, savedTask)
        } else {
            localRepository.save(savedTask)
        }
    }

    /** Copies the authoritative personal To Do response before local pending state is overlaid. */
    private fun copyTodoServerTask(source: Task): Task {
        return Task().apply {
            id = source.id
            ownerID = source.ownerID
            alias = source.alias
            priority = source.priority
            text = source.text
            notes = source.notes
            type = source.type
            challengeID = source.challengeID
            challengeBroken = source.challengeBroken
            attribute = source.attribute
            value = source.value
            tags = RealmList<Tag>().apply {
                source.tags.orEmpty().forEach { sourceTag ->
                    add(
                        Tag().apply {
                            id = sourceTag.id
                            userId = sourceTag.userId
                            name = sourceTag.name
                            group = sourceTag.group
                        }
                    )
                }
            }
            dateCreated = source.dateCreated?.let { Date(it.time) }
            position = source.position
            completed = source.completed
            checklist = RealmList<ChecklistItem>().apply {
                source.checklist.orEmpty().forEach { add(ChecklistItem(it)) }
            }
            reminders = RealmList<RemindersItem>().apply {
                source.reminders.orEmpty().forEach { sourceReminder ->
                    add(
                        RemindersItem().apply {
                            id = sourceReminder.id
                            startDate = sourceReminder.startDate
                            time = sourceReminder.time
                            type = sourceReminder.type
                        }
                    )
                }
            }
            dueDate = source.dueDate?.let { Date(it.time) }
            isDue = source.isDue
            updatedAt = source.updatedAt?.let { Date(it.time) }
        }
    }

    private suspend fun prepareTaskEdit(
        submittedTask: Task,
        editBaseline: Task?,
    ): Task {
        val editedTask = localRepository.getUnmanagedCopy(submittedTask)
        if (!editedTask.canQueueOfflineTodoOperation()) return editedTask
        ensureChecklistIdentity(editedTask)
        val submittedTaskID = editedTask.id ?: return editedTask
        val resolvedTaskID = resolveTaskID(submittedTaskID, editedTask.ownerID)
        val latestTask =
            localRepository.getTaskCopy(resolvedTaskID, editedTask.ownerID).firstOrNull()
                ?: return editedTask
        if (latestTask.pendingDelete) return latestTask

        val changedFields = changedTodoEditFields(editBaseline ?: latestTask, editedTask)
        val existingFields = parsePendingEditFields(latestTask.pendingEditFields)
        val pendingFields = existingFields + changedFields
        TODO_EDIT_FIELDS.minus(changedFields).forEach { fieldName ->
            copyEditableField(editedTask, latestTask, fieldName)
        }
        mergeLatestTaskState(editedTask, latestTask)

        if (latestTask.pendingCreate && latestTask.canQueueOfflineCreation() &&
            latestTask.pendingScoreSnapshot == null
        ) {
            editedTask.pendingUpdate = false
            editedTask.pendingEditFields = null
            return editedTask
        }
        if (pendingFields.isEmpty() && !latestTask.pendingUpdate) return latestTask

        editedTask.pendingUpdate = pendingFields.isNotEmpty() || latestTask.pendingUpdate
        editedTask.pendingEditFields = serializePendingEditFields(pendingFields)
        return editedTask
    }

    private fun mergeLatestTaskState(
        editedTask: Task,
        latestTask: Task,
    ) {
        editedTask.id = latestTask.id
        editedTask.ownerID = latestTask.ownerID
        editedTask.alias = latestTask.alias
        editedTask.position = latestTask.position
        editedTask.value = latestTask.value
        editedTask.completed = latestTask.completed
        editedTask.updatedAt = latestTask.updatedAt
        editedTask.dateCreated = latestTask.dateCreated
        editedTask.isCreating = latestTask.isCreating
        editedTask.pendingCreate = latestTask.pendingCreate
        editedTask.pendingDelete = latestTask.pendingDelete
        editedTask.pendingDeleteAfterScore = latestTask.pendingDeleteAfterScore
        editedTask.pendingUpdate = latestTask.pendingUpdate
        editedTask.pendingEditFields = latestTask.pendingEditFields
        editedTask.pendingPosition = latestTask.pendingPosition
        editedTask.pendingScoreUp = latestTask.pendingScoreUp
        editedTask.pendingScoreDown = latestTask.pendingScoreDown
        editedTask.pendingScoreSnapshot = latestTask.pendingScoreSnapshot
        editedTask.pendingScoreRefresh = latestTask.pendingScoreRefresh
        editedTask.pendingChecklist = latestTask.pendingChecklist
        editedTask.outboxServerOrigin = latestTask.outboxServerOrigin

        val latestChecklist = latestTask.checklist?.associateBy { it.id }.orEmpty()
        editedTask.checklist?.forEach { editedItem ->
            latestChecklist[editedItem.id]?.let { latestItem ->
                editedItem.completed = latestItem.completed
                editedItem.pendingSync = latestItem.pendingSync
            }
        }
    }

    private fun changedTodoEditFields(
        previousTask: Task,
        editedTask: Task,
    ): Set<String> {
        val fields = mutableSetOf<String>()
        if (previousTask.text != editedTask.text) fields += EDIT_TEXT
        if (previousTask.notes.orEmpty() != editedTask.notes.orEmpty()) fields += EDIT_NOTES
        if (previousTask.priority != editedTask.priority) fields += EDIT_PRIORITY
        if (previousTask.attribute != editedTask.attribute) fields += EDIT_ATTRIBUTE
        if (previousTask.tags.orEmpty().mapNotNull { it.id }.toSet() !=
            editedTask.tags.orEmpty().mapNotNull { it.id }.toSet()
        ) {
            fields += EDIT_TAGS
        }
        if (previousTask.dueDate != editedTask.dueDate) fields += EDIT_DUE_DATE
        if (checklistStructure(previousTask) != checklistStructure(editedTask)) {
            fields += EDIT_CHECKLIST
        }
        if (reminderStructure(previousTask) != reminderStructure(editedTask)) {
            fields += EDIT_REMINDERS
        }
        return fields
    }

    private fun checklistStructure(task: Task): List<List<Any?>> {
        return task.checklist.orEmpty().map { item ->
            listOf(item.id, item.text, item.position)
        }
    }

    /** Gives locally editable checklist rows durable identities before they enter the outbox. */
    private fun ensureChecklistIdentity(task: Task): Boolean {
        var changed = false
        task.checklist.orEmpty().forEachIndexed { position, item ->
            if (item.id.isNullOrBlank()) {
                item.id = UUID.randomUUID().toString()
                changed = true
            }
            if (item.position != position) {
                item.position = position
                changed = true
            }
        }
        return changed
    }

    private fun reminderStructure(task: Task): List<List<String?>> {
        return task.reminders.orEmpty().map { reminder ->
            listOf(reminder.id, reminder.startDate, reminder.time, reminder.type)
        }
    }

    private fun parsePendingEditFields(value: String?): Set<String> {
        return value.orEmpty().split(',').filter { it in TODO_EDIT_FIELDS }.toSet()
    }

    private fun serializePendingEditFields(fields: Set<String>): String? {
        return TODO_EDIT_FIELDS.filter(fields::contains).joinToString(",").ifBlank { null }
    }

    private fun copyEditableField(
        target: Task,
        source: Task,
        fieldName: String,
        preserveTargetChecklistCompletion: Boolean = false,
    ) {
        when (fieldName) {
            EDIT_TEXT -> target.text = source.text
            EDIT_NOTES -> target.notes = source.notes
            EDIT_PRIORITY -> target.priority = source.priority
            EDIT_ATTRIBUTE -> target.attribute = source.attribute
            EDIT_TAGS -> {
                target.tags = RealmList<Tag>().apply { addAll(source.tags.orEmpty()) }
            }
            EDIT_DUE_DATE -> target.dueDate = source.dueDate?.let { Date(it.time) }
            EDIT_CHECKLIST -> copyChecklist(
                target,
                source,
                preserveTargetChecklistCompletion,
            )
            EDIT_REMINDERS -> {
                target.reminders = RealmList<RemindersItem>().apply {
                    source.reminders.orEmpty().forEach { reminder ->
                        add(
                            RemindersItem().apply {
                                id = reminder.id
                                startDate = reminder.startDate
                                time = reminder.time
                                type = reminder.type
                            }
                        )
                    }
                }
            }
        }
    }

    private fun buildTaskEditPayload(
        serverTask: Task,
        localTask: Task,
    ): Task {
        parsePendingEditFields(localTask.pendingEditFields).forEach { fieldName ->
            copyEditableField(
                serverTask,
                localTask,
                fieldName,
                preserveTargetChecklistCompletion = true,
            )
        }
        return serverTask
    }

    private fun copyChecklist(
        target: Task,
        source: Task,
        preserveTargetCompletion: Boolean,
    ) {
        val targetItems = target.checklist?.associateBy { it.id }.orEmpty()
        target.checklist = RealmList<ChecklistItem>().apply {
            source.checklist.orEmpty().forEach { sourceItem ->
                add(
                    ChecklistItem(sourceItem).apply {
                        if (preserveTargetCompletion) {
                            completed = targetItems[id]?.completed ?: false
                            pendingSync = false
                        }
                    }
                )
            }
        }
    }

    private fun restorePendingChecklistState(
        localTask: Task,
        savedTask: Task,
    ) {
        val savedItems = savedTask.checklist.orEmpty()
        localTask.checklist.orEmpty().filter { it.pendingSync }.forEach { localItem ->
            val savedItem =
                savedItems.firstOrNull { it.id == localItem.id }
                    ?: savedItems.filter {
                        it.text == localItem.text && it.position == localItem.position
                    }.singleOrNull()
            savedItem?.completed = localItem.completed
            savedItem?.pendingSync = true
        }
        savedTask.pendingChecklist =
            savedTask.pendingChecklist && savedItems.any { it.pendingSync }
    }

    private fun checklistCompletionSnapshot(task: Task): Map<String, Boolean> {
        return task.checklist.orEmpty().mapNotNull { item ->
            item.id?.let { it to item.completed }
        }.toMap()
    }

    private fun restoreChecklistCompletionSnapshot(
        task: Task,
        snapshot: Map<String, Boolean>,
    ) {
        task.checklist.orEmpty().forEach { item ->
            item.id?.let { itemID -> snapshot[itemID]?.let { item.completed = it } }
            item.pendingSync = false
        }
    }

    @Suppress("ReturnCount")
    override suspend fun updateTask(
        task: Task,
        force: Boolean,
        editBaseline: Task?,
    ): Task? {
        if (!force && task.canQueueOfflineTodoOperation() && !accountMatches(task.ownerID)) {
            return null
        }
        return if (force) {
            updateTaskInternal(task, force)
        } else {
            offlineTaskSyncMutex.withLock {
                val preparedTask = prepareTaskEdit(task, editBaseline)
                if (preparedTask.canQueueOfflineTodoOperation() &&
                    !preparedTask.pendingCreate &&
                    !preparedTask.pendingUpdate
                ) {
                    preparedTask
                } else {
                    updateTaskInternal(preparedTask, force)
                }
            }
        }
    }

    private suspend fun updateTaskInternal(
        task: Task,
        force: Boolean,
    ): Task? {
        if (task.canQueueOfflineTodoOperation() && !accountMatches(task.ownerID)) return null
        if (!task.isValid) return task
        if (task.pendingDelete) return task
        if (task.pendingCreate && task.canQueueOfflineCreation()) {
            val pendingTask = localRepository.getUnmanagedCopy(task)
            if (stampTaskOutboxIdentity(pendingTask) == null) return null
            pendingTask.isSaving = false
            pendingTask.hasErrored = false
            localRepository.save(pendingTask)
            offlineTaskSyncScheduler.enqueue(pendingTask.ownerID)
            return pendingTask
        }
        val id = task.id ?: return task
        val canQueue = task.canQueueOfflineTodoOperation() && !force
        val unmanagedTask = localRepository.getUnmanagedCopy(task)
        unmanagedTask.isSaving = true
        unmanagedTask.hasErrored = false
        unmanagedTask.pendingUpdate = canQueue || unmanagedTask.pendingUpdate
        val expectedServerOrigin =
            if (canQueue) stampTaskOutboxIdentity(unmanagedTask) ?: return null else null
        localRepository.save(unmanagedTask)
        if (canQueue) {
            offlineTaskSyncScheduler.enqueue(unmanagedTask.ownerID)
        }
        if (canQueue && unmanagedTask.hasPendingTodoScore()) {
            unmanagedTask.isSaving = false
            localRepository.save(unmanagedTask)
            return unmanagedTask
        }
        val updatePayload =
            if (canQueue) {
                getExactServerTask(
                    id,
                    unmanagedTask.ownerID,
                    expectedServerOrigin ?: return null,
                )?.let { serverTask ->
                    buildTaskEditPayload(serverTask, unmanagedTask)
                }
            } else {
                unmanagedTask
            }
        val savedTask =
            updatePayload?.let {
                apiClient.updateTask(
                    id,
                    it,
                    suppressConnectionErrors = canQueue,
                    expectedUserID = unmanagedTask.ownerID.takeIf { canQueue },
                    expectedServerOrigin = expectedServerOrigin,
                )?.takeIf { serverTask -> !canQueue || serverTask.id == id }
            }
        if (canQueue &&
            !outboxIdentityMatches(unmanagedTask.ownerID, expectedServerOrigin.orEmpty())
        ) {
            return null
        }
        val latestLocalTask =
            localRepository.getTaskCopy(id, unmanagedTask.ownerID).firstOrNull() ?: unmanagedTask
        if (canQueue &&
            !taskOutboxIdentityMatches(
                latestLocalTask,
                unmanagedTask.ownerID,
                expectedServerOrigin.orEmpty(),
            )
        ) {
            return null
        }
        savedTask?.position = latestLocalTask.position
        savedTask?.id = latestLocalTask.id
        savedTask?.ownerID = latestLocalTask.ownerID
        if (savedTask != null) {
            savedTask.pendingCreate = latestLocalTask.pendingCreate
            savedTask.pendingDelete = latestLocalTask.pendingDelete
            savedTask.pendingDeleteAfterScore = latestLocalTask.pendingDeleteAfterScore
            savedTask.pendingUpdate = false
            savedTask.pendingEditFields = null
            savedTask.pendingPosition = latestLocalTask.pendingPosition
            savedTask.pendingScoreUp = latestLocalTask.pendingScoreUp
            savedTask.pendingScoreDown = latestLocalTask.pendingScoreDown
            savedTask.pendingScoreSnapshot = latestLocalTask.pendingScoreSnapshot
            savedTask.pendingScoreRefresh = latestLocalTask.pendingScoreRefresh
            savedTask.pendingChecklist = latestLocalTask.pendingChecklist
            savedTask.outboxServerOrigin = latestLocalTask.outboxServerOrigin
            restorePendingChecklistState(latestLocalTask, savedTask)
            clearTaskOutboxIdentityIfIdle(savedTask)
            savedTask.isSaving = false
            savedTask.hasErrored = false
            localRepository.save(savedTask)
        } else {
            latestLocalTask.hasErrored = !canQueue
            latestLocalTask.isSaving = false
            latestLocalTask.pendingUpdate = canQueue
            localRepository.save(latestLocalTask)
        }
        return savedTask ?: latestLocalTask.takeIf { canQueue }
    }

    override suspend fun deleteTask(taskId: String): Boolean {
        return offlineTaskSyncMutex.withLock { deleteTaskInternal(taskId) }
    }

    private suspend fun deleteTaskInternal(taskId: String): Boolean {
        val unscopedTaskID = resolveTaskIDUnscoped(taskId)
        val scopedTask =
            localRepository.getTaskCopy(unscopedTaskID, currentUserID).firstOrNull()
        val unscopedTask =
            if (scopedTask == null) {
                localRepository.getTaskCopy(unscopedTaskID).firstOrNull()
            } else {
                null
            }
        if (scopedTask == null && unscopedTask?.canQueueOfflineTodoOperation() == true) {
            return false
        }
        val localTask = scopedTask ?: unscopedTask
        val ownerID = localTask?.ownerID.orEmpty()
        val expectedServerOrigin =
            localTask?.outboxServerOrigin?.takeIf(String::isNotBlank)
                ?: apiClient.hostConfig.serverOrigin()
        val resolvedTaskID =
            if (localTask?.canQueueOfflineTodoOperation() == true) {
                if (!outboxIdentityMatches(ownerID, expectedServerOrigin)) return false
                resolveTaskID(taskId, ownerID)
            } else {
                unscopedTaskID
            }
        if (localTask?.hasPendingTodoScore() == true || localTask?.pendingScoreRefresh == true) {
            val pendingDelete = localRepository.getUnmanagedCopy(localTask)
            pendingDelete.pendingDeleteAfterScore = true
            if (stampTaskOutboxIdentity(pendingDelete) == null) return false
            pendingDelete.hasErrored = false
            pendingDelete.isSaving = false
            localRepository.save(pendingDelete)
            offlineTaskSyncScheduler.enqueue(ownerID)
            return true
        }
        if (localTask?.canQueueOfflineTodoOperation() == true) {
            val pendingDelete = localRepository.getUnmanagedCopy(localTask)
            pendingDelete.pendingDelete = true
            pendingDelete.pendingDeleteAfterScore = false
            pendingDelete.pendingUpdate = false
            pendingDelete.pendingEditFields = null
            pendingDelete.pendingPosition = false
            pendingDelete.pendingScoreUp = false
            pendingDelete.pendingScoreDown = false
            pendingDelete.pendingScoreSnapshot = null
            pendingDelete.pendingChecklist = false
            pendingDelete.checklist?.forEach { it.pendingSync = false }
            if (stampTaskOutboxIdentity(pendingDelete) == null) return false
            localRepository.save(pendingDelete)
            offlineTaskSyncScheduler.enqueue(ownerID)
            Log.i(OFFLINE_TASK_SYNC_LOG_TAG, "Queued one local task deletion for server sync.")
            return true
        }
        if (!apiClient.deleteTask(resolvedTaskID)) return false
        localRepository.deleteTask(resolvedTaskID)
        return true
    }

    override fun saveTask(task: Task) {
        localRepository.save(task)
    }

    override suspend fun createTasks(newTasks: List<Task>) = apiClient.createTasks(newTasks)

    override fun markTaskCompleted(
        taskId: String,
        isCompleted: Boolean
    ) {
        localRepository.markTaskCompleted(taskId, isCompleted)
    }

    override fun <T : BaseMainObject> modify(
        obj: T,
        transaction: (T) -> Unit
    ) {
        localRepository.modify(obj, transaction)
    }

    override fun swapTaskPosition(
        firstPosition: Int,
        secondPosition: Int
    ) {
        localRepository.swapTaskPosition(firstPosition, secondPosition)
    }

    override suspend fun updateTaskPosition(
        taskType: TaskType,
        taskID: String,
        newPosition: Int
    ): List<String>? {
        return offlineTaskSyncMutex.withLock {
            updateTaskPositionInternal(taskType, taskID, newPosition)
        }
    }

    private suspend fun updateTaskPositionInternal(
        taskType: TaskType,
        taskID: String,
        newPosition: Int,
    ): List<String>? {
        val resolvedTaskID = resolveTaskIDUnscoped(taskID)
        val scopedTask = localRepository.getTask(resolvedTaskID, currentUserID).firstOrNull()
        val unscopedTask =
            if (scopedTask == null) {
                localRepository.getTask(resolvedTaskID).firstOrNull()
            } else {
                null
            }
        if (scopedTask == null && unscopedTask?.canQueueOfflineTodoOperation() == true) {
            return emptyList()
        }
        val task = scopedTask ?: unscopedTask
        if (task?.pendingDelete == true) return emptyList()
        val canQueue = task?.canQueueOfflineTodoOperation() == true
        val expectedServerOrigin =
            task?.outboxServerOrigin?.takeIf(String::isNotBlank)
                ?: apiClient.hostConfig.serverOrigin()
        if (canQueue && task != null &&
            !outboxIdentityMatches(task.ownerID, expectedServerOrigin)
        ) {
            return emptyList()
        }
        val queuedTask =
            if (canQueue && task != null) {
                val taskID = task.id ?: return emptyList()
                localRepository.moveTaskToPosition(
                    taskID,
                    task.ownerID,
                    expectedServerOrigin,
                    taskType,
                    newPosition,
                )
                val movedTask =
                    localRepository.getTaskCopy(taskID, task.ownerID).firstOrNull() ?: task
                if (stampTaskOutboxIdentity(movedTask) == null) return emptyList()
                localRepository.save(movedTask)
                offlineTaskSyncScheduler.enqueue(task.ownerID)
                movedTask
            } else {
                task
            }
        if (queuedTask?.pendingCreate == true || queuedTask?.pendingUpdate == true ||
            queuedTask?.pendingChecklist == true || queuedTask?.hasPendingTodoScore() == true
        ) {
            return emptyList()
        }
        val serverTaskID =
            if (canQueue && queuedTask != null) {
                getExactServerTask(
                    resolvedTaskID,
                    queuedTask.ownerID,
                    expectedServerOrigin,
                )?.id ?: return emptyList()
            } else {
                resolvedTaskID
            }
        val positions = if (queuedTask?.isGroupTask == true) {
            apiClient.postGroupTaskNewPosition(serverTaskID, newPosition)
        } else {
            apiClient.postTaskNewPosition(
                serverTaskID,
                newPosition,
                suppressConnectionErrors = canQueue,
                expectedUserID = queuedTask?.ownerID.takeIf { canQueue },
                expectedServerOrigin = expectedServerOrigin.takeIf { canQueue },
            )
        }
        if (positions == null) {
            return if (canQueue) emptyList() else null
        }
        if (canQueue && queuedTask != null &&
            !taskOutboxIdentityMatches(
                getLatestLocalTask(queuedTask),
                queuedTask.ownerID,
                expectedServerOrigin,
            )
        ) {
            return emptyList()
        }
        val positionOwnerID = queuedTask?.ownerID.orEmpty()
        if (queuedTask?.isGroupTask == true || positionOwnerID.isBlank()) {
            localRepository.updateTaskPositions(positions)
        } else {
            localRepository.updateTaskPositions(positions, positionOwnerID)
        }
        if (canQueue && queuedTask != null) {
            val currentTask = getLatestLocalTask(queuedTask)
            if (currentTask.position == newPosition) {
                currentTask.pendingPosition = false
                clearTaskOutboxIdentityIfIdle(currentTask)
                localRepository.save(currentTask)
            }
        }
        return positions
    }

    override fun getUnmanagedTask(taskid: String) =
        getTask(taskid).map { localRepository.getUnmanagedCopy(it) }

    override fun updateTaskInBackground(
        task: Task,
        assignChanges: Map<String, MutableList<String>>,
        onComplete: (suspend (Task) -> Unit)?,
        editBaseline: Task?,
    ) {
        MainScope().launchCatching {
            val updatedTask =
                updateTask(task, editBaseline = editBaseline)
                    ?: task.takeIf { it.canQueueOfflineTodoOperation() }?.let {
                        getLatestLocalTask(it)
                    } ?: return@launchCatching
            handleAssignmentChanges(updatedTask, assignChanges)
            awaitTaskPersisted(updatedTask)
            onComplete?.invoke(updatedTask)
        }
    }

    override fun createTaskInBackground(
        task: Task,
        assignChanges: Map<String, MutableList<String>>,
        onComplete: (suspend (Task) -> Unit)?
    ) {
        MainScope().launchCatching {
            val createdTask =
                createTask(task)
                    ?: task.takeIf { it.pendingCreate && it.canQueueOfflineTodoOperation() }?.let {
                        getLatestLocalTask(it)
                    } ?: return@launchCatching
            handleAssignmentChanges(createdTask, assignChanges)
            awaitTaskPersisted(createdTask)
            onComplete?.invoke(createdTask)
        }
    }

    private suspend fun awaitTaskPersisted(task: Task) {
        val id = task.id ?: return
        withTimeoutOrNull(3.seconds) {
            localRepository.getTasks(task.ownerID).first { tasks -> tasks.any { it.id == id } }
        }
    }

    private suspend fun handleAssignmentChanges(
        task: Task,
        assignChanges: Map<String, MutableList<String>>
    ) {
        val taskID = task.id ?: return
        assignChanges["assign"]?.let { assignments ->
            if (assignments.isEmpty()) return@let
            val savedTask = apiClient.assignToTask(taskID, assignments) ?: return@let
            savedTask.id = task.id
            savedTask.ownerID = task.ownerID
            savedTask.position = task.position
            localRepository.save(savedTask)
        }

        assignChanges["unassign"]?.let { unassignments ->
            var savedTask: Task? = null
            for (unassignment in unassignments) {
                savedTask = apiClient.unassignFromTask(taskID, unassignment)
            }
            if (savedTask != null) {
                savedTask.id = task.id
                savedTask.position = task.position
                savedTask.ownerID = task.ownerID
                localRepository.save(savedTask)
            }
        }
    }

    override fun getTaskCopies(): Flow<List<Task>> =
        authenticationHandler.userIDFlow.flatMapLatest {
            localRepository.getTasks(it)
        }.map { tasks ->
            localRepository.getUnmanagedCopy(tasks)
                .filter(::taskIsVisibleForActiveServer)
        }

    override fun getTaskCopies(tasks: List<Task>): List<Task> =
        localRepository.getUnmanagedCopy(tasks)

    override suspend fun retrieveDailiesFromDate(date: Date): TaskList? {
        val formatter = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.US)
        return apiClient.getTasks("dailys", formatter.format(date))
    }

    override suspend fun syncErroredTasks(): List<Task>? {
        return offlineTaskSyncMutex.withLock {
            val expectedUserID = currentUserID
            val expectedServerOrigin = apiClient.hostConfig.serverOrigin()
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@withLock null
            val tasks = localRepository.getErroredTasks(expectedUserID).firstOrNull()
                ?: return@withLock null
            val unmanagedTasks =
                tasks.map { localRepository.getUnmanagedCopy(it) }
                    .filterNot { it.pendingDelete }
            val (queuedCreations, nonCreationTasks) =
                unmanagedTasks.partition { task ->
                    task.pendingCreate && !task.pendingDelete &&
                        task.canQueueOfflineTodoOperation()
                }
            val (queuedActions, otherTasks) =
                nonCreationTasks.partition { task ->
                    (
                        task.pendingUpdate || task.pendingPosition || task.pendingChecklist ||
                            task.pendingScoreUp || task.pendingScoreDown || task.pendingScoreRefresh ||
                            task.pendingDeleteAfterScore
                    ) && task.canQueueOfflineTodoOperation()
                }
            val boundCreations =
                queuedCreations.mapNotNull {
                    bindTaskOutboxIdentity(it, expectedUserID, expectedServerOrigin)
                }
            val boundActions =
                queuedActions.mapNotNull {
                    bindTaskOutboxIdentity(it, expectedUserID, expectedServerOrigin)
                }
            syncQueuedTaskCreations(
                boundCreations,
                expectedUserID,
                expectedServerOrigin,
            ).filterNotNull() +
                syncQueuedTaskActions(
                    boundActions,
                    expectedUserID,
                    expectedServerOrigin,
                ).filterNotNull() +
                otherTasks.mapNotNull { task ->
                    if (task.isCreating) {
                        createTask(task, true)
                    } else {
                        updateTask(task, true)
                    }
                }
        }
    }

    override suspend fun syncPendingTaskCreations(
        expectedUserID: String?,
        expectedServerOrigin: String?,
    ): Boolean {
        return offlineTaskSyncMutex.withLock {
            val pinnedUserID = expectedUserID ?: currentUserID
            val pinnedServerOrigin = expectedServerOrigin ?: apiClient.hostConfig.serverOrigin()
            if (!outboxIdentityMatches(pinnedUserID, pinnedServerOrigin)) return@withLock false
            val tasks = localRepository.getPendingTaskCreations(pinnedUserID).firstOrNull().orEmpty()
            val queuedTasks =
                tasks.map { localRepository.getUnmanagedCopy(it) }
                    .mapNotNull { task ->
                        task.takeIf {
                            !it.pendingDelete && it.canQueueOfflineTodoOperation()
                        }?.let {
                            bindTaskOutboxIdentity(
                                task,
                                pinnedUserID,
                                pinnedServerOrigin,
                            )
                        }
                    }
            val pendingDeletions =
                localRepository.getPendingTaskDeletions(pinnedUserID).firstOrNull().orEmpty()
                    .map { localRepository.getUnmanagedCopy(it) }
                    .mapNotNull { task ->
                        task.takeIf { it.canQueueOfflineTodoOperation() }?.let {
                            bindTaskOutboxIdentity(it, pinnedUserID, pinnedServerOrigin)
                        }
                    }
            val pendingActions =
                localRepository.getPendingTaskActions(pinnedUserID).firstOrNull().orEmpty()
                    .map { localRepository.getUnmanagedCopy(it) }
                    .mapNotNull { task ->
                        task.takeIf {
                            !it.pendingDelete && it.canQueueOfflineTodoOperation()
                        }?.let {
                            bindTaskOutboxIdentity(
                                task,
                                pinnedUserID,
                                pinnedServerOrigin,
                            )
                        }
                    }
            Log.i(
                OFFLINE_TASK_SYNC_LOG_TAG,
                "Syncing ${queuedTasks.size} queued creations, " +
                    "${pendingDeletions.size} queued deletions, and " +
                    "${pendingActions.size} queued actions."
            )
            val deletionsSynced =
                syncQueuedTaskDeletions(
                    pendingDeletions,
                    pinnedUserID,
                    pinnedServerOrigin,
                ).all { it }
            val creationsSynced =
                syncQueuedTaskCreations(
                    queuedTasks,
                    pinnedUserID,
                    pinnedServerOrigin,
                ).all { task -> task != null }
            val actionsSynced =
                syncQueuedTaskActions(
                    pendingActions,
                    pinnedUserID,
                    pinnedServerOrigin,
                ).all { task -> task != null }
            Log.i(
                OFFLINE_TASK_SYNC_LOG_TAG,
                "Queued sync finished: creations=$creationsSynced, " +
                    "deletions=$deletionsSynced, actions=$actionsSynced."
            )
            deletionsSynced && creationsSynced && actionsSynced
        }
    }

    override suspend fun getPendingTodoScoreRefreshes(
        expectedUserID: String?,
        expectedServerOrigin: String?,
    ): List<Task> {
        val pinnedUserID = expectedUserID ?: currentUserID
        val pinnedServerOrigin = expectedServerOrigin ?: apiClient.hostConfig.serverOrigin()
        if (!outboxIdentityMatches(pinnedUserID, pinnedServerOrigin)) return emptyList()
        val pendingTasks =
            localRepository.getPendingTaskScoreRefreshes(pinnedUserID).firstOrNull().orEmpty()
        return pendingTasks
            .mapNotNull { task ->
                localRepository.getUnmanagedCopy(task)
                    .takeIf { it.pendingScoreRefresh && it.canQueueOfflineTodoOperation() }
                    ?.let { bindTaskOutboxIdentity(it, pinnedUserID, pinnedServerOrigin) }
            }
    }

    override suspend fun getTodoCopiesForSync(
        expectedUserID: String,
        expectedServerOrigin: String?,
    ): List<Task> {
        val pinnedServerOrigin = expectedServerOrigin ?: apiClient.hostConfig.serverOrigin()
        if (!outboxIdentityMatches(expectedUserID, pinnedServerOrigin)) return emptyList()
        return localRepository.getTasksIncludingPending(expectedUserID).firstOrNull().orEmpty()
            .filter { it.ownerID == expectedUserID && it.canQueueOfflineTodoOperation() }
            .mapNotNull {
                bindTaskOutboxIdentity(it, expectedUserID, pinnedServerOrigin)
            }
    }

    override suspend fun clearPendingTodoScoreRefreshes(
        expectedUserID: String,
        refreshedTasks: List<Task>,
        expectedServerOrigin: String?,
    ) {
        offlineTaskSyncMutex.withLock {
            val pinnedServerOrigin = expectedServerOrigin ?: apiClient.hostConfig.serverOrigin()
            if (!outboxIdentityMatches(expectedUserID, pinnedServerOrigin)) return@withLock
            refreshedTasks.forEach { refreshedTask ->
                val taskID = refreshedTask.id ?: return@forEach
                val latestTask =
                    localRepository.getTaskCopy(taskID, expectedUserID).firstOrNull()
                        ?: return@forEach
                if (taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        pinnedServerOrigin,
                    ) && latestTask.pendingScoreRefresh &&
                    latestTask.completed == refreshedTask.completed &&
                    latestTask.updatedAt == refreshedTask.updatedAt
                ) {
                    latestTask.pendingScoreRefresh = false
                    clearTaskOutboxIdentityIfIdle(latestTask)
                    localRepository.save(latestTask)
                }
            }
        }
    }

    override suspend fun refreshPendingTodoScoreTasks(
        tasks: List<Task>,
        expectedUserID: String,
        expectedServerOrigin: String?,
    ): List<Task> {
        return offlineTaskSyncMutex.withLock {
            val pinnedServerOrigin = expectedServerOrigin ?: apiClient.hostConfig.serverOrigin()
            tasks.mapNotNull { unboundTask ->
                val task =
                    bindTaskOutboxIdentity(
                        unboundTask,
                        expectedUserID,
                        pinnedServerOrigin,
                    ) ?: return@mapNotNull null
                if (!taskOutboxIdentityMatches(task, expectedUserID, pinnedServerOrigin)) {
                    return@mapNotNull null
                }
                val taskID = task.id ?: return@mapNotNull null
                var serverTask =
                    getExactServerTask(taskID, expectedUserID, pinnedServerOrigin)
                val taskAlias = task.persistedOfflineCreateAlias(taskID)
                if (serverTask == null && taskAlias != null) {
                    serverTask =
                        getServerTaskByOfflineAlias(
                            taskAlias,
                            expectedUserID,
                            pinnedServerOrigin,
                        )
                }
                if (serverTask == null) {
                    if (!outboxIdentityMatches(expectedUserID, pinnedServerOrigin)) {
                        return@mapNotNull null
                    }
                    val identifiers = listOfNotNull(taskID, taskAlias).distinct()
                    val serverStates =
                        identifiers.map { identifier ->
                            apiClient.getTaskServerState(
                                identifier,
                                expectedUserID,
                                pinnedServerOrigin,
                            )
                        }
                    if (!outboxIdentityMatches(expectedUserID, pinnedServerOrigin) ||
                        serverStates.any { it != TaskServerState.MISSING }
                    ) {
                        return@mapNotNull null
                    }
                    val latestTask =
                        localRepository.getTaskCopy(taskID, expectedUserID).firstOrNull()
                            ?: return@mapNotNull null
                    if (!taskOutboxIdentityMatches(
                            latestTask,
                            expectedUserID,
                            pinnedServerOrigin,
                        )
                    ) {
                        return@mapNotNull null
                    }
                    val hasPendingWrite =
                        latestTask.pendingCreate || latestTask.pendingDelete ||
                            latestTask.pendingUpdate || latestTask.pendingPosition ||
                            latestTask.pendingChecklist || latestTask.hasPendingTodoScore()
                    val missingResult = localRepository.getUnmanagedCopy(latestTask)
                    missingResult.missingDuringScoreRefresh = true
                    if (!hasPendingWrite || latestTask.pendingDelete ||
                        (latestTask.pendingDeleteAfterScore && !latestTask.hasPendingTodoScore())
                    ) {
                        missingResult.pendingDeleteAfterScore = true
                        val tombstone = localRepository.getUnmanagedCopy(latestTask)
                        tombstone.pendingDeleteAfterScore = true
                        localRepository.save(tombstone)
                        return@mapNotNull missingResult
                    }
                    val recoveryTask =
                        promoteTaskForRecreation(
                            latestTask,
                            expectedUserID,
                            pinnedServerOrigin,
                        )
                            ?: return@mapNotNull null
                    recoveryTask.pendingScoreRefresh = false
                    localRepository.save(recoveryTask)
                    return@mapNotNull missingResult
                }
                if (!outboxIdentityMatches(expectedUserID, pinnedServerOrigin)) {
                    return@mapNotNull null
                }
                val latestTask = getLatestLocalTask(task)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        pinnedServerOrigin,
                    ) || !latestTask.pendingScoreRefresh
                ) {
                    return@mapNotNull null
                }
                if (latestTask.pendingUpdate) {
                    buildTaskEditPayload(serverTask, latestTask)
                }
                saveCreatedTask(latestTask, serverTask, preservePendingActions = true)
                getLatestLocalTask(serverTask)
            }
        }
    }

    private suspend fun syncQueuedTaskDeletions(
        tasks: List<Task>,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): List<Boolean> {
        return tasks.map { unboundTask ->
            val task =
                bindTaskOutboxIdentity(
                    unboundTask,
                    expectedUserID,
                    expectedServerOrigin,
                ) ?: return@map true
            if (!taskOutboxIdentityMatches(task, expectedUserID, expectedServerOrigin)) {
                return@map false
            }
            val localTaskID = task.id ?: return@map false
            val exactServerTask =
                getExactServerTask(localTaskID, expectedUserID, expectedServerOrigin)
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map false
            if (exactServerTask != null) {
                if (!deleteVerifiedServerTask(
                        exactServerTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map false
                }
                val latestTask = getLatestLocalTask(task)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map false
                }
                localRepository.deleteTask(localTaskID, expectedUserID)
                return@map true
            }

            val exactState =
                apiClient.getTaskServerState(
                    localTaskID,
                    expectedUserID,
                    expectedServerOrigin,
                )
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin) ||
                exactState != TaskServerState.MISSING
            ) {
                return@map false
            }

            val taskAlias = task.persistedOfflineCreateAlias(localTaskID)
            if (taskAlias == null) {
                val latestTask = getLatestLocalTask(task)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map false
                }
                localRepository.deleteTask(localTaskID, expectedUserID)
                return@map true
            }

            val aliasedServerTask =
                getServerTaskByOfflineAlias(
                    taskAlias,
                    expectedUserID,
                    expectedServerOrigin,
                )
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map false
            if (aliasedServerTask != null) {
                if (!deleteVerifiedServerTask(
                        aliasedServerTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map false
                }
                val latestTask = getLatestLocalTask(task)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map false
                }
                localRepository.deleteTask(localTaskID, expectedUserID)
                aliasedServerTask.id?.takeIf { it != localTaskID }?.let { serverTaskID ->
                    localRepository.deleteTask(serverTaskID, expectedUserID)
                }
                return@map true
            }

            val aliasState =
                apiClient.getTaskServerState(
                    taskAlias,
                    expectedUserID,
                    expectedServerOrigin,
                )
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin) ||
                aliasState != TaskServerState.MISSING
            ) {
                return@map false
            }
            if (task.pendingCreate) {
                val recreatedTask =
                    apiClient.createTask(
                        task,
                        suppressConnectionErrors = true,
                        expectedUserID = expectedUserID,
                        expectedServerOrigin = expectedServerOrigin,
                    )?.takeIf {
                        it.alias == taskAlias && !it.id.isNullOrBlank()
                    } ?: return@map false
                if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin) ||
                    !deleteVerifiedServerTask(
                        recreatedTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map false
                }
            }
            val latestTask = getLatestLocalTask(task)
            if (!taskOutboxIdentityMatches(
                    latestTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return@map false
            }
            localRepository.deleteTask(localTaskID, expectedUserID)
            true
        }
    }

    private suspend fun syncQueuedTaskCreations(
        tasks: List<Task>,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): List<Task?> {
        if (tasks.isEmpty()) return emptyList()
        return tasks.map { unboundTask ->
            val task =
                bindTaskOutboxIdentity(
                    unboundTask,
                    expectedUserID,
                    expectedServerOrigin,
                ) ?: return@map unboundTask
            if (!taskOutboxIdentityMatches(task, expectedUserID, expectedServerOrigin)) {
                return@map null
            }
            val taskID = task.id ?: return@map null
            val checklistIdentityChanged = ensureChecklistIdentity(task)
            val taskIdentifier = offlineCreateAlias(taskID)
            val aliasChanged = task.alias != taskIdentifier
            if (aliasChanged) {
                task.alias = taskIdentifier
            }
            if (checklistIdentityChanged || aliasChanged) {
                if (!taskOutboxIdentityMatches(task, expectedUserID, expectedServerOrigin)) {
                    return@map null
                }
                localRepository.save(task)
            }
            var onlineTask =
                getServerTaskByOfflineAlias(
                    taskIdentifier,
                    expectedUserID,
                    expectedServerOrigin,
                )
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map null
            if (onlineTask == null) {
                onlineTask =
                    getExactServerTask(taskID, expectedUserID, expectedServerOrigin)
                if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map null
            }
            var serverAlreadyCompleted = false
            val syncedTask =
                if (onlineTask != null) {
                    serverAlreadyCompleted = onlineTask.completed
                    val checklistSnapshot = checklistCompletionSnapshot(onlineTask)
                    var latestLocalTask = getLatestLocalTask(task)
                    if (!taskOutboxIdentityMatches(
                            latestLocalTask,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                    ) {
                        return@map null
                    }
                    val boundaryState =
                        ensureTodoScoreBoundary(
                            latestLocalTask,
                            expectedUserID,
                            expectedServerOrigin,
                        ) ?: return@map null
                    latestLocalTask = boundaryState.first
                    val scoreBoundary = boundaryState.second
                    if (scoreBoundary == null) {
                        val changedFields = changedTodoEditFields(onlineTask, latestLocalTask)
                        if (changedFields.isNotEmpty()) {
                            latestLocalTask = localRepository.getUnmanagedCopy(latestLocalTask)
                            val pendingFields =
                                parsePendingEditFields(latestLocalTask.pendingEditFields) +
                                    changedFields
                            latestLocalTask.pendingUpdate = true
                            latestLocalTask.pendingEditFields =
                                serializePendingEditFields(pendingFields)
                        }
                    }
                    saveCreatedTask(
                        latestLocalTask,
                        onlineTask,
                        preservePendingActions = true,
                        preserveServerResponse = true,
                    )
                    restoreChecklistCompletionSnapshot(onlineTask, checklistSnapshot)
                    onlineTask
                } else {
                    val aliasState =
                        apiClient.getTaskServerState(
                            taskIdentifier,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                    if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) {
                        return@map null
                    }
                    val originalIDState =
                        apiClient.getTaskServerState(
                            taskID,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                    if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) {
                        return@map null
                    }
                    if (aliasState != TaskServerState.MISSING ||
                        originalIDState != TaskServerState.MISSING
                    ) {
                        return@map null
                    }
                    val latestLocalTask = getLatestLocalTask(task)
                    if (!taskOutboxIdentityMatches(
                            latestLocalTask,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                    ) {
                        return@map null
                    }
                    if (latestLocalTask.pendingDelete) return@map latestLocalTask
                    val boundaryState =
                        ensureTodoScoreBoundary(
                            latestLocalTask,
                            expectedUserID,
                            expectedServerOrigin,
                        ) ?: return@map null
                    val scoreBoundary = boundaryState.second
                    val creationPayload =
                        if (scoreBoundary?.requiresCreate == true) {
                            taskFromTodoScoreBoundary(scoreBoundary)
                        } else {
                            latestLocalTask
                        }
                    val createdTask =
                        apiClient.createTask(
                            creationPayload,
                            suppressConnectionErrors = true,
                            expectedUserID = expectedUserID,
                            expectedServerOrigin = expectedServerOrigin,
                        )?.takeIf { serverTask ->
                            serverTask.alias == taskIdentifier && !serverTask.id.isNullOrBlank()
                        }
                    if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) {
                        return@map null
                    }
                    serverAlreadyCompleted = createdTask?.completed == true
                    createdTask?.also {
                        val checklistSnapshot = checklistCompletionSnapshot(it)
                        val currentLocalTask = getLatestLocalTask(latestLocalTask)
                        if (!taskOutboxIdentityMatches(
                                currentLocalTask,
                                expectedUserID,
                                expectedServerOrigin,
                            )
                        ) {
                            return@map null
                        }
                        saveCreatedTask(
                            currentLocalTask,
                            it,
                            preservePendingActions = true,
                            preserveServerResponse = true,
                        )
                        restoreChecklistCompletionSnapshot(it, checklistSnapshot)
                    }
                }
            if (syncedTask == null) {
                syncedTask
            } else {
                val latestSyncedTask = getLatestLocalTask(syncedTask)
                when {
                    latestSyncedTask.pendingDelete -> latestSyncedTask
                    !latestSyncedTask.hasQueuedOfflineTodoOperation() -> latestSyncedTask
                    else ->
                        replayQueuedTaskActions(
                            syncedTask,
                            latestSyncedTask,
                            serverAlreadyCompleted,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                }
            }
        }
    }

    private suspend fun getLatestLocalTask(task: Task): Task {
        val taskID = task.id ?: return task
        val ownerID = task.ownerID.ifBlank { currentUserID }
        return localRepository.getTaskCopy(resolveTaskID(taskID, ownerID), ownerID).firstOrNull()
            ?: task
    }

    private fun resolveTaskID(
        taskID: String,
        ownerID: String = currentUserID,
    ): String {
        val replacementKey = Triple(ownerID, apiClient.hostConfig.serverOrigin(), taskID)
        return recentTaskIDReplacements[replacementKey] ?: taskID
    }

    private fun resolveTaskIDUnscoped(taskID: String): String {
        return recentTaskIDReplacements[
            Triple(currentUserID, apiClient.hostConfig.serverOrigin(), taskID)
        ] ?: taskID
    }

    /** Returns only the durable alias proving that this ID originated as an offline create. */
    private fun Task.persistedOfflineCreateAlias(taskID: String): String? {
        return alias?.takeIf { it == offlineCreateAlias(taskID) }
    }

    /** Fetches a server task only when the response is the exact requested task ID. */
    private suspend fun getExactServerTask(
        taskID: String,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task? {
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
        val serverTask =
            apiClient.getTask(
                taskID,
                suppressConnectionErrors = true,
                expectedUserID = expectedUserID,
                expectedServerOrigin = expectedServerOrigin,
            )
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
        return serverTask?.takeIf { it.id == taskID }
    }

    /** Fetches an offline-created server task only when its alias proves the identity. */
    private suspend fun getServerTaskByOfflineAlias(
        taskAlias: String,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task? {
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
        val serverTask =
            apiClient.getTask(
                taskAlias,
                suppressConnectionErrors = true,
                expectedUserID = expectedUserID,
                expectedServerOrigin = expectedServerOrigin,
            )
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
        return serverTask?.takeIf { it.alias == taskAlias && !it.id.isNullOrBlank() }
    }

    /** Deletes a task whose exact server identity was already verified. */
    private suspend fun deleteVerifiedServerTask(
        serverTask: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Boolean {
        val serverTaskID = serverTask.id ?: return false
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return false
        val deleted =
            apiClient.deleteTask(
                serverTaskID,
                suppressConnectionErrors = true,
                expectedUserID = expectedUserID,
                expectedServerOrigin = expectedServerOrigin,
            )
        if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return false
        if (deleted) return true
        val state =
            apiClient.getTaskServerState(
                serverTaskID,
                expectedUserID,
                expectedServerOrigin,
            )
        return outboxIdentityMatches(expectedUserID, expectedServerOrigin) &&
            state == TaskServerState.MISSING
    }

    /** Promotes a missing server task to an idempotent create without losing queued suffix work. */
    private fun promoteTaskForRecreation(
        task: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task? {
        if (!taskOutboxIdentityMatches(task, expectedUserID, expectedServerOrigin)) return null
        val taskID = task.id ?: return null
        val taskAlias = offlineCreateAlias(taskID)
        val recoveryTask = localRepository.getUnmanagedCopy(task)
        recoveryTask.pendingCreate = true
        recoveryTask.alias = taskAlias
        recoveryTask.outboxServerOrigin = expectedServerOrigin

        val serializedBoundary = recoveryTask.pendingScoreSnapshot
        if (serializedBoundary != null) {
            val boundary = parseTodoScoreBoundary(serializedBoundary) ?: return null
            recoveryTask.pendingScoreSnapshot =
                todoScoreBoundaryGson.toJson(
                    boundary.copy(
                        version = TODO_SCORE_BOUNDARY_VERSION,
                        serverOrigin = expectedServerOrigin,
                        requiresCreate = true,
                        position = boundary.position ?: boundary.task.position,
                        task =
                            boundary.task.copy(
                                id = taskID,
                                alias = taskAlias,
                                ownerID = expectedUserID,
                            ),
                    )
                )
        } else if (recoveryTask.hasPendingTodoScore()) {
            val direction =
                when {
                    recoveryTask.pendingScoreUp && !recoveryTask.pendingScoreDown ->
                        TaskDirection.UP
                    recoveryTask.pendingScoreDown && !recoveryTask.pendingScoreUp ->
                        TaskDirection.DOWN
                    else -> return null
                }
            recoveryTask.pendingPosition = true
            freezeTodoScoreBoundary(recoveryTask, direction)
        } else {
            recoveryTask.pendingPosition = true
        }
        return recoveryTask
    }

    private suspend fun syncQueuedTaskActions(
        tasks: List<Task>,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): List<Task?> {
        return tasks.map { unboundTask ->
            val task =
                bindTaskOutboxIdentity(
                    unboundTask,
                    expectedUserID,
                    expectedServerOrigin,
                ) ?: return@map unboundTask
            if (!taskOutboxIdentityMatches(task, expectedUserID, expectedServerOrigin)) {
                return@map null
            }
            if (task.pendingScoreRefresh) return@map null
            val taskID = task.id ?: return@map null
            var onlineTask =
                getExactServerTask(taskID, expectedUserID, expectedServerOrigin)
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map null
            val taskAlias = task.persistedOfflineCreateAlias(taskID)
            if (onlineTask == null && taskAlias != null) {
                onlineTask =
                    getServerTaskByOfflineAlias(
                        taskAlias,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map null
            }
            if (onlineTask != null) {
                val latestLocalTask = getLatestLocalTask(task)
                if (!taskOutboxIdentityMatches(
                        latestLocalTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return@map null
                }
                if (latestLocalTask.pendingDelete) return@map latestLocalTask
                val serverAlreadyCompleted = onlineTask.completed
                val reconciledLocalTask =
                    if (onlineTask.id != latestLocalTask.id) {
                        val serverChecklist = checklistCompletionSnapshot(onlineTask)
                        saveCreatedTask(
                            latestLocalTask,
                            onlineTask,
                            preservePendingActions = true,
                            preserveServerResponse = true,
                        )
                        restoreChecklistCompletionSnapshot(onlineTask, serverChecklist)
                        getLatestLocalTask(onlineTask).also { reconciledTask ->
                            TODO_EDIT_FIELDS.forEach { fieldName ->
                                copyEditableField(reconciledTask, latestLocalTask, fieldName)
                            }
                            reconciledTask.position = latestLocalTask.position
                            reconciledTask.completed = latestLocalTask.completed
                            if (!taskOutboxIdentityMatches(
                                    reconciledTask,
                                    expectedUserID,
                                    expectedServerOrigin,
                                )
                            ) {
                                return@map null
                            }
                            localRepository.save(reconciledTask)
                        }
                    } else {
                        latestLocalTask
                    }
                replayQueuedTaskActions(
                    onlineTask,
                    reconciledLocalTask,
                    serverAlreadyCompleted,
                    expectedUserID,
                    expectedServerOrigin,
                )
            } else {
                val serverStates =
                    listOfNotNull(taskID, taskAlias).distinct().map { identifier ->
                        apiClient.getTaskServerState(
                            identifier,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                    }
                if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return@map null
                when {
                    serverStates.all { it == TaskServerState.MISSING } -> {
                        val latestLocalTask = getLatestLocalTask(task)
                        if (!taskOutboxIdentityMatches(
                                latestLocalTask,
                                expectedUserID,
                                expectedServerOrigin,
                            )
                        ) {
                            return@map null
                        }
                        if (latestLocalTask.pendingDelete) return@map latestLocalTask
                        if (latestLocalTask.pendingDeleteAfterScore &&
                            !latestLocalTask.pendingScoreRefresh &&
                            !latestLocalTask.hasPendingTodoScore()
                        ) {
                            if (!taskOutboxIdentityMatches(
                                    latestLocalTask,
                                    expectedUserID,
                                    expectedServerOrigin,
                                )
                            ) {
                                return@map null
                            }
                            localRepository.deleteTask(taskID, expectedUserID)
                            return@map latestLocalTask
                        }
                        val recreationTask =
                            promoteTaskForRecreation(
                                latestLocalTask,
                                expectedUserID,
                                expectedServerOrigin,
                            )
                                ?: return@map null
                        val queuedCreation =
                            queueTaskOperation(
                                recreationTask,
                                pendingReorder = recreationTask.pendingPosition,
                                requiresCreation = true,
                            )
                        syncQueuedTaskCreations(
                            listOf(queuedCreation),
                            expectedUserID,
                            expectedServerOrigin,
                        ).firstOrNull()
                    }
                    else -> null
                }
            }
        }
    }

    private suspend fun replayQueuedTaskActions(
        initialServerTask: Task,
        desiredTask: Task,
        serverAlreadyCompleted: Boolean,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task? {
        if (!taskOutboxIdentityMatches(
                desiredTask,
                expectedUserID,
                expectedServerOrigin,
            )
        ) {
            return null
        }
        val taskID = initialServerTask.id ?: return null
        var serverTask = initialServerTask
        var currentTask = getLatestLocalTask(desiredTask)
        if (!taskOutboxIdentityMatches(currentTask, expectedUserID, expectedServerOrigin)) {
            return null
        }
        if (serverTask.ownerID.isBlank()) {
            serverTask.ownerID = currentTask.ownerID
        }
        if (currentTask.pendingDelete) return currentTask

        val boundaryState =
            ensureTodoScoreBoundary(
                currentTask,
                expectedUserID,
                expectedServerOrigin,
            ) ?: return null
        currentTask = boundaryState.first
        val scoreBoundary = boundaryState.second
        if (scoreBoundary != null) {
            return replayTodoScoreBoundary(
                scoreBoundary,
                currentTask,
                serverTask,
                serverAlreadyCompleted,
                expectedUserID,
                expectedServerOrigin,
            )
        }

        if (currentTask.pendingDeleteAfterScore) {
            val pendingDelete = localRepository.getUnmanagedCopy(currentTask)
            pendingDelete.pendingDeleteAfterScore = false
            pendingDelete.pendingDelete = true
            pendingDelete.pendingUpdate = false
            pendingDelete.pendingEditFields = null
            pendingDelete.pendingPosition = false
            pendingDelete.pendingChecklist = false
            pendingDelete.checklist.orEmpty().forEach { it.pendingSync = false }
            if (!taskOutboxIdentityMatches(
                    pendingDelete,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            localRepository.save(pendingDelete)
            return if (
                syncQueuedTaskDeletions(
                    listOf(pendingDelete),
                    expectedUserID,
                    expectedServerOrigin,
                ).singleOrNull() == true
            ) {
                serverTask
            } else {
                null
            }
        }

        if (currentTask.pendingUpdate) {
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            val updatePayload = buildTaskEditPayload(serverTask, currentTask)
            val updatedServerTask =
                apiClient.updateTask(
                    taskID,
                    updatePayload,
                    suppressConnectionErrors = true,
                    expectedUserID = expectedUserID,
                    expectedServerOrigin = expectedServerOrigin,
                )?.takeIf { it.id == taskID } ?: return null
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
            val checklistSnapshot = checklistCompletionSnapshot(updatedServerTask)
            val latestTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    latestTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            if (latestTask.pendingDelete) return latestTask
            latestTask.pendingUpdate = false
            latestTask.pendingEditFields = null
            saveCreatedTask(
                latestTask,
                updatedServerTask,
                preservePendingActions = true,
                preserveServerResponse = true,
            )
            restoreChecklistCompletionSnapshot(updatedServerTask, checklistSnapshot)
            serverTask = updatedServerTask
            currentTask = getLatestLocalTask(updatedServerTask)
            if (!currentTask.hasQueuedOfflineTodoOperation()) return currentTask
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
        }

        if (currentTask.pendingChecklist) {
            val replayedChecklist =
                replayQueuedChecklistTargets(
                    currentTask,
                    serverTask,
                    expectedUserID,
                    expectedServerOrigin,
                ) ?: return null
            currentTask = replayedChecklist.first
            serverTask = replayedChecklist.second
        }

        val desiredPosition = currentTask.position
        if (currentTask.pendingPosition) {
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            val positions =
                apiClient.postTaskNewPosition(
                    taskID,
                    desiredPosition,
                    suppressConnectionErrors = true,
                    expectedUserID = expectedUserID,
                    expectedServerOrigin = expectedServerOrigin,
                ) ?: return null
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
            val latestTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    latestTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            if (latestTask.pendingDelete) return latestTask
            if (latestTask.pendingPosition && latestTask.position != desiredPosition) {
                return null
            }
            localRepository.updateTaskPositions(positions, expectedUserID)
            latestTask.pendingPosition = false
            clearTaskOutboxIdentityIfIdle(latestTask)
            localRepository.save(latestTask)
            currentTask = latestTask
        }

        return if (currentTask.pendingDelete ||
            currentTask.pendingUpdate ||
            currentTask.pendingPosition ||
            currentTask.pendingChecklist ||
            currentTask.pendingScoreUp ||
            currentTask.pendingScoreDown
        ) {
            null
        } else {
            currentTask
        }
    }

    /** Restores or creates the immutable boundary for a queued To Do score. */
    private suspend fun ensureTodoScoreBoundary(
        task: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Pair<Task, TodoScoreBoundary?>? {
        var latestTask = getLatestLocalTask(task)
        if (!taskOutboxIdentityMatches(
                latestTask,
                expectedUserID,
                expectedServerOrigin,
            )
        ) {
            return null
        }
        val serializedBoundary = latestTask.pendingScoreSnapshot
        if (serializedBoundary != null) {
            var boundary = parseTodoScoreBoundary(serializedBoundary) ?: return null
            if (boundary.task.ownerID != expectedUserID) return null
            boundary =
                when {
                    boundary.version == LEGACY_TODO_SCORE_BOUNDARY_VERSION &&
                        boundary.serverOrigin.isNullOrBlank() -> {
                        boundary.copy(
                            version = TODO_SCORE_BOUNDARY_VERSION,
                            serverOrigin = expectedServerOrigin,
                        )
                    }
                    boundary.version == TODO_SCORE_BOUNDARY_VERSION &&
                        boundary.serverOrigin == expectedServerOrigin -> boundary
                    else -> return null
                }
            val scoreUp = boundary.direction == TaskDirection.UP.text
            if (latestTask.pendingScoreSnapshot != todoScoreBoundaryGson.toJson(boundary) ||
                latestTask.pendingScoreUp != scoreUp || latestTask.pendingScoreDown == scoreUp
            ) {
                latestTask = localRepository.getUnmanagedCopy(latestTask)
                latestTask.pendingScoreSnapshot = todoScoreBoundaryGson.toJson(boundary)
                latestTask.pendingScoreUp = scoreUp
                latestTask.pendingScoreDown = !scoreUp
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return null
                }
                localRepository.save(latestTask)
            }
            return Pair(latestTask, boundary)
        }
        if (!latestTask.hasPendingTodoScore()) return Pair(latestTask, null)
        val direction =
            when {
                latestTask.pendingScoreUp && !latestTask.pendingScoreDown -> TaskDirection.UP
                latestTask.pendingScoreDown && !latestTask.pendingScoreUp -> TaskDirection.DOWN
                else -> return null
            }
        latestTask = localRepository.getUnmanagedCopy(latestTask)
        freezeTodoScoreBoundary(latestTask, direction)
        if (!taskOutboxIdentityMatches(latestTask, expectedUserID, expectedServerOrigin)) {
            return null
        }
        localRepository.save(latestTask)
        return Pair(latestTask, parseTodoScoreBoundary(latestTask.pendingScoreSnapshot) ?: return null)
    }

    /** Replays immutable pre-score work, then the score, without touching suffix flags. */
    private suspend fun replayTodoScoreBoundary(
        boundary: TodoScoreBoundary,
        desiredTask: Task,
        initialServerTask: Task,
        serverAlreadyCompleted: Boolean,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Task? {
        if (boundary.version != TODO_SCORE_BOUNDARY_VERSION ||
            boundary.serverOrigin != expectedServerOrigin ||
            boundary.task.ownerID != expectedUserID ||
            !taskOutboxIdentityMatches(desiredTask, expectedUserID, expectedServerOrigin)
        ) {
            return null
        }
        var currentTask = getLatestLocalTask(desiredTask)
        var serverTask = initialServerTask
        val taskID = serverTask.id ?: return null
        val boundaryTask = taskFromTodoScoreBoundary(boundary)
        val prefixEditFields =
            boundary.editFields.toSet() +
                if (boundary.requiresCreate) {
                    changedTodoEditFields(serverTask, boundaryTask)
                } else {
                    emptySet()
                }

        if (prefixEditFields.isNotEmpty()) {
            prefixEditFields.forEach { fieldName ->
                copyEditableField(
                    serverTask,
                    boundaryTask,
                    fieldName,
                    preserveTargetChecklistCompletion = true,
                )
            }
            val updatedServerTask =
                apiClient.updateTask(
                    taskID,
                    serverTask,
                    suppressConnectionErrors = true,
                    expectedUserID = expectedUserID,
                    expectedServerOrigin = expectedServerOrigin,
                )?.takeIf { it.id == taskID } ?: return null
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
            val serverChecklist = checklistCompletionSnapshot(updatedServerTask)
            currentTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            if (currentTask.pendingDelete) return currentTask
            saveCreatedTask(
                currentTask,
                updatedServerTask,
                preservePendingActions = true,
                preserveServerResponse = true,
            )
            restoreChecklistCompletionSnapshot(updatedServerTask, serverChecklist)
            serverTask = updatedServerTask
            currentTask = getLatestLocalTask(updatedServerTask)
        }

        val checklistResult =
            replayTodoScoreBoundaryChecklist(
                boundary.checklistTargets,
                currentTask,
                serverTask,
                expectedUserID,
                expectedServerOrigin,
            ) ?: return null
        currentTask = checklistResult.first
        serverTask = checklistResult.second

        boundary.position?.let { prefixPosition ->
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            val suffixPosition = currentTask.position.takeIf { currentTask.pendingPosition }
            val positions =
                apiClient.postTaskNewPosition(
                    taskID,
                    prefixPosition,
                    suppressConnectionErrors = true,
                    expectedUserID = expectedUserID,
                    expectedServerOrigin = expectedServerOrigin,
                ) ?: return null
            if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
            localRepository.updateTaskPositions(positions, expectedUserID)
            currentTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            suffixPosition?.let {
                currentTask.position = it
                currentTask.pendingPosition = true
                localRepository.save(currentTask)
            }
            serverTask.position = prefixPosition
        }

        currentTask = getLatestLocalTask(currentTask)
        if (!taskOutboxIdentityMatches(
                currentTask,
                expectedUserID,
                expectedServerOrigin,
            ) || currentTask.pendingDelete
        ) {
            return null
        }
        val scoreUp = boundary.direction == TaskDirection.UP.text
        val serverAtTarget =
            if (scoreUp) {
                serverAlreadyCompleted || serverTask.completed
            } else {
                !serverTask.completed
            }
        if (serverAtTarget) {
            clearQueuedTodoScore(
                currentTask,
                scoreUp,
                expectedUserID,
                expectedServerOrigin,
            )
        } else if (
            taskCheckedInternal(
                null,
                currentTask,
                scoreUp,
                true,
                null,
                expectedUserID,
            ) == null
        ) {
            return null
        }
        currentTask = getLatestLocalTask(currentTask)
        return currentTask.takeIf {
            taskOutboxIdentityMatches(it, expectedUserID, expectedServerOrigin) &&
                it.pendingScoreRefresh && !it.hasPendingTodoScore()
        }
    }

    /** Brings prefix checklist items to their desired states idempotently. */
    private suspend fun replayTodoScoreBoundaryChecklist(
        targets: List<TodoChecklistTarget>,
        queuedTask: Task,
        initialServerTask: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Pair<Task, Task>? {
        var currentTask = getLatestLocalTask(queuedTask)
        if (!taskOutboxIdentityMatches(
                currentTask,
                expectedUserID,
                expectedServerOrigin,
            )
        ) {
            return null
        }
        var serverTask = initialServerTask
        val taskID = serverTask.id ?: return null
        targets.forEach { target ->
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            currentTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            if (currentTask.pendingDelete) return null
            val serverItem =
                serverTask.checklist.orEmpty().firstOrNull { it.id == target.id }
                    ?: serverTask.checklist.orEmpty().filter {
                        it.text == target.text && it.position == target.position
                    }.singleOrNull()
            if (serverItem == null) {
                val latestTask = getLatestLocalTask(currentTask)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return null
                }
                saveCreatedTask(
                    latestTask,
                    serverTask,
                    preservePendingActions = true,
                    preserveServerResponse = true,
                )
                currentTask = getLatestLocalTask(serverTask)
                return@forEach
            }
            if (serverItem.completed != target.completed) {
                val serverItemID = serverItem.id ?: return null
                serverTask =
                    apiClient.scoreChecklistItem(
                        taskID,
                        serverItemID,
                        suppressConnectionErrors = true,
                        expectedUserID = expectedUserID,
                        expectedServerOrigin = expectedServerOrigin,
                    )?.takeIf { it.id == taskID } ?: return null
                if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
                if (serverTask.checklist?.firstOrNull { it.id == serverItemID }?.completed !=
                    target.completed
                ) {
                    return null
                }
                val serverChecklist = checklistCompletionSnapshot(serverTask)
                currentTask = getLatestLocalTask(currentTask)
                if (!taskOutboxIdentityMatches(
                        currentTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return null
                }
                saveCreatedTask(
                    currentTask,
                    serverTask,
                    preservePendingActions = true,
                    preserveServerResponse = true,
                )
                restoreChecklistCompletionSnapshot(serverTask, serverChecklist)
                currentTask = getLatestLocalTask(serverTask)
            }
        }
        return Pair(currentTask, serverTask)
    }

    private suspend fun replayQueuedChecklistTargets(
        queuedTask: Task,
        initialServerTask: Task,
        expectedUserID: String,
        expectedServerOrigin: String,
    ): Pair<Task, Task>? {
        if (!taskOutboxIdentityMatches(queuedTask, expectedUserID, expectedServerOrigin)) {
            return null
        }
        var currentTask = getLatestLocalTask(queuedTask)
        var serverTask = initialServerTask
        val taskID = serverTask.id ?: return null
        val pendingItemIDs =
            currentTask.checklist.orEmpty().filter { it.pendingSync }.mapNotNull { it.id }
        for (itemID in pendingItemIDs) {
            currentTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    currentTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            if (currentTask.pendingDelete) return null
            val localItem = currentTask.checklist?.firstOrNull { it.id == itemID } ?: continue
            val serverItem = serverTask.checklist?.firstOrNull { it.id == itemID }
            if (serverItem == null) {
                val latestTask = getLatestLocalTask(currentTask)
                if (!taskOutboxIdentityMatches(
                        latestTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return null
                }
                latestTask.checklist?.firstOrNull { it.id == itemID }?.pendingSync = false
                latestTask.pendingChecklist = latestTask.checklist.orEmpty().any { it.pendingSync }
                saveCreatedTask(
                    latestTask,
                    serverTask,
                    preservePendingActions = true,
                    preserveServerResponse = true,
                )
                currentTask = getLatestLocalTask(serverTask)
                continue
            }
            if (serverItem.completed != localItem.completed) {
                if (!taskOutboxIdentityMatches(
                        currentTask,
                        expectedUserID,
                        expectedServerOrigin,
                    )
                ) {
                    return null
                }
                val serverItemID = serverItem.id ?: itemID
                serverTask =
                    apiClient.scoreChecklistItem(
                        taskID,
                        serverItemID,
                        suppressConnectionErrors = true,
                        expectedUserID = expectedUserID,
                        expectedServerOrigin = expectedServerOrigin,
                    )?.takeIf { it.id == taskID } ?: return null
                if (!outboxIdentityMatches(expectedUserID, expectedServerOrigin)) return null
                if (serverTask.checklist?.firstOrNull { it.id == serverItemID }?.completed !=
                    localItem.completed
                ) {
                    return null
                }
            }
            val latestTask = getLatestLocalTask(currentTask)
            if (!taskOutboxIdentityMatches(
                    latestTask,
                    expectedUserID,
                    expectedServerOrigin,
                )
            ) {
                return null
            }
            latestTask.checklist?.firstOrNull { it.id == itemID }?.pendingSync = false
            latestTask.pendingChecklist = latestTask.checklist.orEmpty().any { it.pendingSync }
            val checklistSnapshot = checklistCompletionSnapshot(serverTask)
            saveCreatedTask(
                latestTask,
                serverTask,
                preservePendingActions = true,
                preserveServerResponse = true,
            )
            restoreChecklistCompletionSnapshot(serverTask, checklistSnapshot)
            currentTask = getLatestLocalTask(serverTask)
        }
        return Pair(currentTask, serverTask)
    }

    override suspend fun unlinkAllTasks(
        challengeID: String?,
        keepOption: String
    ): Void? {
        return apiClient.unlinkAllTasks(challengeID, keepOption)
    }

    override fun getTasksForChallenge(challengeID: String?): Flow<List<Task>> {
        return localRepository.getTasksForChallenge(challengeID, currentUserID)
    }
}
