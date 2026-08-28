package com.habitrpg.android.habitica.data.implementation

import android.util.Log
import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.TaskServerState
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.OfflineTaskSyncScheduler
import com.habitrpg.android.habitica.data.sync.canQueueOfflineTodoCompletion
import com.habitrpg.android.habitica.data.sync.canQueueOfflineCreation
import com.habitrpg.android.habitica.data.sync.isQueuedOfflineTodoCompletion
import com.habitrpg.android.habitica.data.sync.offlineCreateAlias
import com.habitrpg.android.habitica.helpers.Analytics
import com.habitrpg.android.habitica.helpers.AppConfigManager
import com.habitrpg.android.habitica.helpers.EventCategory
import com.habitrpg.android.habitica.helpers.HitType
import com.habitrpg.android.habitica.interactors.ScoreTaskLocallyInteractor
import com.habitrpg.android.habitica.models.BaseMainObject
import com.habitrpg.android.habitica.models.responses.BulkTaskScoringData
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.OwnedItem
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.common.habitica.helpers.launchCatching
import com.habitrpg.shared.habitica.models.responses.TaskDirection
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.responses.TaskScoringResult
import com.habitrpg.shared.habitica.models.tasks.TaskType
import com.habitrpg.shared.habitica.models.tasks.TasksOrder
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.MainScope
import kotlinx.coroutines.flow.Flow
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
private val recentTaskIDReplacements = mutableMapOf<String, String>()
private const val OFFLINE_TASK_SYNC_LOG_TAG = "OfflineTaskSync"

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
    private var lastTaskAction: Long = 0

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
        )

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
    ): TaskScoringResult? {
        if (task.pendingDelete) return null
        val canQueueTodoCompletion = task.canQueueOfflineTodoCompletion(up)
        val isQueuedTaskCreation = task.pendingCreate && task.canQueueOfflineCreation()
        val wasQueuedTodoCompletion = task.isQueuedOfflineTodoCompletion()
        val localData =
            if (user != null && appConfigManager.enableLocalTaskScoring()) {
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
        if (isQueuedTaskCreation && task.type == TaskType.TODO) {
            queueTaskOperation(
                task,
                pendingCompletion = up,
                pendingReorder = task.pendingPosition,
                requiresCreation = true,
            )
            return null
        }
        val now = Date().time
        val id = task.id
        if (lastTaskAction > now - 500 && !force || id == null) {
            return null
        }

        lastTaskAction = now
        val res =
            this.apiClient.postTaskDirection(
                id,
                (if (up) TaskDirection.UP else TaskDirection.DOWN).text,
                suppressConnectionErrors = canQueueTodoCompletion
            )
        if (res == null) {
            if (canQueueTodoCompletion) {
                queueTaskOperation(
                    task,
                    pendingCompletion = true,
                    pendingReorder = false,
                    requiresCreation = false,
                )
            }
            return null
        }
        if (wasQueuedTodoCompletion) {
            clearQueuedTodoCompletion(task)
        }
        // There are cases where the user object is not set correctly. So the app refetches it as a fallback
        val thisUser =
            user ?: localRepository.getUser(authenticationHandler.currentUserID ?: "").firstOrNull()
                ?: return if (wasQueuedTodoCompletion) TaskScoringResult() else null
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
        if (res.lvl == 0) {
            // Team tasks that require approval have weird data that we should just ignore.
            return TaskScoringResult()
        }
        val result = TaskScoringResult(res, thisUser.stats)
        if (localData == null) {
            notifyFunc?.invoke(result)
        }
        handleTaskResponse(thisUser, res, task, up, localData?.delta ?: 0f)
        return result
    }

    private fun queueTaskOperation(
        task: Task,
        pendingCompletion: Boolean? = null,
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
        if (pendingCompletion != null && pendingTask.type == TaskType.TODO) {
            pendingTask.completeForUser(currentUserID, pendingCompletion)
            pendingTask.pendingScoreUp = pendingCompletion
        }
        localRepository.save(pendingTask)
        offlineTaskSyncScheduler.enqueue()
        Log.i(OFFLINE_TASK_SYNC_LOG_TAG, "Queued one local task operation for server sync.")
        return pendingTask
    }

    private suspend fun clearQueuedTodoCompletion(task: Task) {
        val completedTask = getLatestLocalTask(task)
        completedTask.completeForUser(currentUserID, true)
        completedTask.pendingScoreUp = false
        completedTask.hasErrored = false
        completedTask.isSaving = false
        localRepository.save(completedTask)
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
        this.localRepository.executeTransaction {
            val bgTask = localRepository.getLiveObject(task) ?: return@executeTransaction
            val bgUser = localRepository.getLiveObject(user) ?: return@executeTransaction
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
                bgTask.completeForUser(authenticationHandler.currentUserID ?: "", up)
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
                    it.where(Task::class.java).equalTo("id", taskId).findAll().forEach { sibling ->
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
                    it.where(OwnedItem::class.java).equalTo("itemType", type).equalTo("key", key)
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
        val task = localRepository.getTask(resolveTaskID(taskId)).firstOrNull() ?: return null
        return taskCheckedInternal(user, task, up, force, notifyFunc)
    }

    override suspend fun scoreChecklistItem(
        taskId: String,
        itemId: String
    ): Task? {
        return offlineTaskSyncMutex.withLock {
            val resolvedTaskID = resolveTaskID(taskId)
            val localTask = localRepository.getTaskCopy(resolvedTaskID).firstOrNull()
            if (localTask?.pendingCreate == true || localTask?.pendingDelete == true) {
                return@withLock null
            }
            scoreChecklistItemInternal(resolvedTaskID, itemId)
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

    override fun getTask(taskId: String) = localRepository.getTask(resolveTaskID(taskId))

    override fun getTaskCopy(taskId: String) = localRepository.getTaskCopy(resolveTaskID(taskId))

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
        val now = Date().time
        if (lastTaskAction > now - 500 && !force) {
            return null
        }
        lastTaskAction = now

        val canQueue = task.canQueueOfflineCreation()
        task.isSaving = true
        task.isCreating = true
        task.hasErrored = false
        task.pendingCreate = canQueue
        task.pendingPosition = false
        task.pendingScoreUp = false
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
            task.alias = task.offlineCreateAlias()
        }
        localRepository.save(task)

        val savedTask =
            if (task.isGroupTask) {
                apiClient.createGroupTask(task.group?.groupID ?: "", task)
            } else {
                apiClient.createTask(task, suppressConnectionErrors = canQueue)
            }
        val latestLocalTask =
            task.id?.let { localRepository.getTaskCopy(it).firstOrNull() } ?: task
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
            if (canQueue && !latestLocalTask.pendingDelete) {
                latestLocalTask.pendingPosition = true
            }
            localRepository.save(latestLocalTask)
            if (canQueue) {
                offlineTaskSyncScheduler.enqueue()
            }
        }
        return savedTask
    }

    private fun saveCreatedTask(
        localTask: Task,
        savedTask: Task,
        preservePendingActions: Boolean = false,
    ) {
        val localTaskID = localTask.id
        val shouldReplaceLocalTask = savedTask.id != localTaskID && localTaskID != null
        if (savedTask.ownerID.isBlank()) {
            savedTask.ownerID = localTask.ownerID
        }
        savedTask.dateCreated = savedTask.dateCreated ?: Date()
        savedTask.position = localTask.position
        savedTask.tags = localTask.tags
        savedTask.isCreating = false
        savedTask.isSaving = false
        savedTask.hasErrored = false
        savedTask.pendingCreate = false
        savedTask.pendingDelete = preservePendingActions && localTask.pendingDelete
        savedTask.pendingPosition = preservePendingActions && localTask.pendingPosition
        savedTask.pendingScoreUp = preservePendingActions && localTask.pendingScoreUp
        if (savedTask.pendingScoreUp && localTask.type == TaskType.TODO && localTask.completed) {
            savedTask.completeForUser(currentUserID, true)
        }
        if (shouldReplaceLocalTask && localTaskID != null) {
            savedTask.id?.let { replacementTaskID ->
                recentTaskIDReplacements.entries
                    .filter { it.value == localTaskID }
                    .forEach { it.setValue(replacementTaskID) }
                recentTaskIDReplacements[localTaskID] = replacementTaskID
            }
            localRepository.replaceTask(localTaskID, savedTask)
        } else {
            localRepository.save(savedTask)
        }
    }

    @Suppress("ReturnCount")
    override suspend fun updateTask(
        task: Task,
        force: Boolean
    ): Task? {
        return if (force) {
            updateTaskInternal(task, force)
        } else {
            offlineTaskSyncMutex.withLock {
                updateTaskInternal(getLatestLocalTask(task), force)
            }
        }
    }

    private suspend fun updateTaskInternal(
        task: Task,
        force: Boolean,
    ): Task? {
        val now = Date().time
        if ((lastTaskAction > now - 500 && !force) || !task.isValid) {
            return task
        }
        if (task.pendingDelete) return task
        if (task.pendingCreate && task.canQueueOfflineCreation()) {
            val pendingTask = localRepository.getUnmanagedCopy(task)
            pendingTask.isSaving = false
            pendingTask.hasErrored = false
            localRepository.save(pendingTask)
            offlineTaskSyncScheduler.enqueue()
            return pendingTask
        }
        lastTaskAction = now
        val id = task.id ?: return task
        val unmanagedTask = localRepository.getUnmanagedCopy(task)
        unmanagedTask.isSaving = true
        unmanagedTask.hasErrored = false
        localRepository.save(unmanagedTask)
        val savedTask = apiClient.updateTask(id, unmanagedTask)
        val latestLocalTask = localRepository.getTaskCopy(id).firstOrNull() ?: unmanagedTask
        savedTask?.position = latestLocalTask.position
        savedTask?.id = latestLocalTask.id
        savedTask?.ownerID = latestLocalTask.ownerID
        if (savedTask != null) {
            savedTask.tags = latestLocalTask.tags
            savedTask.pendingCreate = latestLocalTask.pendingCreate
            savedTask.pendingDelete = latestLocalTask.pendingDelete
            savedTask.pendingPosition = latestLocalTask.pendingPosition
            savedTask.pendingScoreUp = latestLocalTask.pendingScoreUp
            if (savedTask.pendingScoreUp && latestLocalTask.completed) {
                savedTask.completeForUser(currentUserID, true)
            }
            localRepository.save(savedTask)
        } else {
            latestLocalTask.hasErrored = true
            latestLocalTask.isSaving = false
            localRepository.save(latestLocalTask)
        }
        return savedTask
    }

    override suspend fun deleteTask(taskId: String): Boolean {
        return offlineTaskSyncMutex.withLock { deleteTaskInternal(taskId) }
    }

    private suspend fun deleteTaskInternal(taskId: String): Boolean {
        val resolvedTaskID = resolveTaskID(taskId)
        val localTask = localRepository.getTaskCopy(resolvedTaskID).firstOrNull()
        if (localTask?.canQueueOfflineCreation() == true) {
            val pendingDelete = localRepository.getUnmanagedCopy(localTask)
            pendingDelete.pendingDelete = true
            pendingDelete.pendingPosition = false
            pendingDelete.pendingScoreUp = false
            localRepository.save(pendingDelete)
            offlineTaskSyncScheduler.enqueue()
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
        val resolvedTaskID = resolveTaskID(taskID)
        val task = getTask(resolvedTaskID).firstOrNull()
        if (task?.pendingDelete == true) return emptyList()
        val canQueue = task?.canQueueOfflineCreation() == true
        if (task?.pendingCreate == true && canQueue) {
            queueTaskOperation(
                task,
                pendingReorder = true,
                newPosition = newPosition,
                requiresCreation = true,
            )
            return emptyList()
        }
        val positions = if (task?.isGroupTask == true) {
            apiClient.postGroupTaskNewPosition(resolvedTaskID, newPosition)
        } else {
            apiClient.postTaskNewPosition(
                resolvedTaskID,
                newPosition,
                suppressConnectionErrors = canQueue,
            )
        }
        if (positions == null) {
            if (canQueue && task != null) {
                queueTaskOperation(
                    task,
                    pendingReorder = true,
                    newPosition = newPosition,
                    requiresCreation = false,
                )
                return emptyList()
            }
            return null
        }
        localRepository.updateTaskPositions(positions)
        return positions
    }

    override fun getUnmanagedTask(taskid: String) =
        getTask(taskid).map { localRepository.getUnmanagedCopy(it) }

    override fun updateTaskInBackground(
        task: Task,
        assignChanges: Map<String, MutableList<String>>,
        onComplete: (suspend () -> Unit)?
    ) {
        MainScope().launchCatching {
            val updatedTask = updateTask(task) ?: return@launchCatching
            handleAssignmentChanges(updatedTask, assignChanges)
            awaitTaskPersisted(updatedTask)
            onComplete?.invoke()
        }
    }

    override fun createTaskInBackground(
        task: Task,
        assignChanges: Map<String, MutableList<String>>,
        onComplete: (suspend () -> Unit)?
    ) {
        MainScope().launchCatching {
            val createdTask = createTask(task) ?: return@launchCatching
            handleAssignmentChanges(createdTask, assignChanges)
            awaitTaskPersisted(createdTask)
            onComplete?.invoke()
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
        }.map { localRepository.getUnmanagedCopy(it) }

    override fun getTaskCopies(tasks: List<Task>): List<Task> =
        localRepository.getUnmanagedCopy(tasks)

    override suspend fun retrieveDailiesFromDate(date: Date): TaskList? {
        val formatter = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.US)
        return apiClient.getTasks("dailys", formatter.format(date))
    }

    override suspend fun syncErroredTasks(): List<Task>? {
        return offlineTaskSyncMutex.withLock {
            val tasks = localRepository.getErroredTasks(currentUserID).firstOrNull()
                ?: return@withLock null
            val unmanagedTasks =
                tasks.map { localRepository.getUnmanagedCopy(it) }
                    .filterNot { it.pendingDelete }
            val (queuedCreations, nonCreationTasks) =
                unmanagedTasks.partition { task ->
                    task.pendingCreate && task.canQueueOfflineCreation()
                }
            val (queuedActions, otherTasks) =
                nonCreationTasks.partition { task ->
                    (task.pendingPosition || task.pendingScoreUp) && task.canQueueOfflineCreation()
                }
            syncQueuedTaskCreations(queuedCreations).filterNotNull() +
                syncQueuedTaskActions(queuedActions).filterNotNull() +
                otherTasks.mapNotNull { task ->
                    if (task.isCreating) {
                        createTask(task, true)
                    } else {
                        updateTask(task, true)
                    }
                }
        }
    }

    override suspend fun syncPendingTaskCreations(): Boolean {
        return offlineTaskSyncMutex.withLock {
            if (currentUserID.isBlank()) return@withLock false
            val tasks = localRepository.getPendingTaskCreations(currentUserID).firstOrNull().orEmpty()
            val queuedTasks =
                tasks.map { localRepository.getUnmanagedCopy(it) }
                    .filter { task -> task.canQueueOfflineCreation() }
            val pendingDeletions =
                localRepository.getPendingTaskDeletions(currentUserID).firstOrNull().orEmpty()
                    .map { localRepository.getUnmanagedCopy(it) }
                    .filter { task -> task.canQueueOfflineCreation() }
            val pendingActions =
                localRepository.getPendingTaskActions(currentUserID).firstOrNull().orEmpty()
                    .map { localRepository.getUnmanagedCopy(it) }
                    .filter { task -> task.canQueueOfflineCreation() }
            Log.i(
                OFFLINE_TASK_SYNC_LOG_TAG,
                "Syncing ${queuedTasks.size} queued creations, " +
                    "${pendingDeletions.size} queued deletions, and " +
                    "${pendingActions.size} queued actions."
            )
            val deletionsSynced = syncQueuedTaskDeletions(pendingDeletions).all { it }
            val creationsSynced = syncQueuedTaskCreations(queuedTasks).all { task -> task != null }
            val actionsSynced = syncQueuedTaskActions(pendingActions).all { task -> task != null }
            Log.i(
                OFFLINE_TASK_SYNC_LOG_TAG,
                "Queued sync finished: creations=$creationsSynced, " +
                    "deletions=$deletionsSynced, actions=$actionsSynced."
            )
            deletionsSynced && creationsSynced && actionsSynced
        }
    }

    private suspend fun syncQueuedTaskDeletions(tasks: List<Task>): List<Boolean> {
        return tasks.map { task ->
            val localTaskID = task.id ?: return@map false
            val taskIdentifier =
                if (task.pendingCreate) task.offlineCreateAlias() ?: localTaskID else localTaskID
            val serverTaskID =
                if (task.pendingCreate) {
                    apiClient.getTask(taskIdentifier, suppressConnectionErrors = true)?.id
                } else {
                    localTaskID
                }
            var deleted = apiClient.deleteTask(taskIdentifier, suppressConnectionErrors = true)
            var serverMissing = false
            if (!deleted) {
                serverMissing =
                    apiClient.getTaskServerState(taskIdentifier) == TaskServerState.MISSING
            }
            if (!deleted && serverMissing && task.pendingCreate && taskIdentifier != localTaskID) {
                deleted = apiClient.deleteTask(localTaskID, suppressConnectionErrors = true)
                serverMissing =
                    !deleted &&
                    apiClient.getTaskServerState(localTaskID) == TaskServerState.MISSING
            }
            if (deleted || serverMissing) {
                localRepository.deleteTask(localTaskID)
                if (serverTaskID != null && serverTaskID != localTaskID) {
                    localRepository.deleteTask(serverTaskID)
                }
                true
            } else {
                false
            }
        }
    }

    private suspend fun syncQueuedTaskCreations(tasks: List<Task>): List<Task?> {
        if (tasks.isEmpty()) return emptyList()
        return tasks.map { task ->
            val taskID = task.id ?: return@map null
            val taskIdentifier = task.offlineCreateAlias() ?: taskID
            val onlineTask =
                apiClient.getTask(taskIdentifier, suppressConnectionErrors = true)
                    ?: apiClient.getTask(taskID, suppressConnectionErrors = true)
            var serverAlreadyCompleted = false
            val syncedTask =
                if (onlineTask != null) {
                    serverAlreadyCompleted = onlineTask.completed
                    val latestLocalTask = getLatestLocalTask(task)
                    saveCreatedTask(latestLocalTask, onlineTask, preservePendingActions = true)
                    onlineTask
                } else {
                    val aliasState = apiClient.getTaskServerState(taskIdentifier)
                    val originalIDState = apiClient.getTaskServerState(taskID)
                    if (aliasState != TaskServerState.MISSING ||
                        originalIDState != TaskServerState.MISSING
                    ) {
                        return@map null
                    }
                    val latestLocalTask = getLatestLocalTask(task)
                    if (latestLocalTask.pendingDelete) return@map latestLocalTask
                    val createdTask =
                        apiClient.createTask(latestLocalTask, suppressConnectionErrors = true)
                    serverAlreadyCompleted = createdTask?.completed == true
                    createdTask?.also {
                        val currentLocalTask = getLatestLocalTask(latestLocalTask)
                        saveCreatedTask(currentLocalTask, it, preservePendingActions = true)
                    }
                }
            if (syncedTask == null) {
                syncedTask
            } else if (syncedTask.pendingDelete) {
                syncedTask
            } else {
                replayQueuedTaskActions(
                    syncedTask,
                    syncedTask.position,
                    shouldReorder = syncedTask.pendingPosition,
                    shouldComplete = syncedTask.pendingScoreUp && !serverAlreadyCompleted,
                )
            }
        }
    }

    private suspend fun getLatestLocalTask(task: Task): Task {
        val taskID = task.id ?: return task
        return localRepository.getTaskCopy(resolveTaskID(taskID)).firstOrNull() ?: task
    }

    private fun resolveTaskID(taskID: String): String {
        recentTaskIDReplacements[taskID]?.let { cachedTaskID ->
            val resolvedCachedTaskID =
                localRepository.resolveTaskID(cachedTaskID, offlineCreateAlias(taskID))
            recentTaskIDReplacements[taskID] = resolvedCachedTaskID
            return resolvedCachedTaskID
        }
        val resolvedTaskID =
            localRepository.resolveTaskID(taskID, offlineCreateAlias(taskID))
        if (resolvedTaskID != taskID) {
            recentTaskIDReplacements[taskID] = resolvedTaskID
        }
        return resolvedTaskID
    }

    private suspend fun syncQueuedTaskActions(tasks: List<Task>): List<Task?> {
        return tasks.map { task ->
            val taskID = task.id ?: return@map null
            val onlineTask = apiClient.getTask(taskID, suppressConnectionErrors = true)
            if (onlineTask != null) {
                val latestLocalTask = getLatestLocalTask(task)
                if (latestLocalTask.pendingDelete) return@map latestLocalTask
                val shouldComplete =
                    latestLocalTask.pendingScoreUp &&
                        latestLocalTask.type == TaskType.TODO &&
                        latestLocalTask.completed &&
                        !onlineTask.completed
                val shouldReorder = latestLocalTask.pendingPosition
                saveCreatedTask(latestLocalTask, onlineTask, preservePendingActions = true)
                replayQueuedTaskActions(
                    onlineTask,
                    latestLocalTask.position,
                    shouldReorder,
                    shouldComplete,
                )
            } else {
                when (apiClient.getTaskServerState(taskID)) {
                    TaskServerState.MISSING -> {
                        val latestLocalTask = getLatestLocalTask(task)
                        if (latestLocalTask.pendingDelete) return@map latestLocalTask
                        val queuedCreation =
                            queueTaskOperation(
                                latestLocalTask,
                                pendingCompletion =
                                    true.takeIf {
                                        latestLocalTask.pendingScoreUp &&
                                            latestLocalTask.type == TaskType.TODO
                                    },
                                pendingReorder = latestLocalTask.pendingPosition,
                                requiresCreation = true,
                            )
                        syncQueuedTaskCreations(listOf(queuedCreation)).firstOrNull()
                    }
                    TaskServerState.PRESENT,
                    TaskServerState.UNKNOWN -> null
                }
            }
        }
    }

    private suspend fun replayQueuedTaskActions(
        syncedTask: Task,
        desiredPosition: Int,
        shouldReorder: Boolean,
        shouldComplete: Boolean,
    ): Task? {
        val taskID = syncedTask.id ?: return null
        val pendingTask = localRepository.getUnmanagedCopy(syncedTask)
        pendingTask.position = desiredPosition
        pendingTask.pendingCreate = false
        pendingTask.pendingPosition = shouldReorder
        pendingTask.pendingScoreUp = shouldComplete
        if (shouldComplete && pendingTask.type == TaskType.TODO) {
            pendingTask.completeForUser(currentUserID, true)
        }
        localRepository.save(pendingTask)

        if (shouldReorder) {
            val positions =
                apiClient.postTaskNewPosition(
                    taskID,
                    desiredPosition,
                    suppressConnectionErrors = true,
                ) ?: return null
            val currentTask = getLatestLocalTask(pendingTask)
            if (currentTask.pendingDelete) return currentTask
            if (currentTask.pendingPosition && currentTask.position != desiredPosition) {
                return null
            }
            localRepository.updateTaskPositions(positions)
            currentTask.pendingPosition = false
            localRepository.save(currentTask)
        }

        var currentTask = getLatestLocalTask(pendingTask)
        if (currentTask.pendingDelete) return currentTask

        if (currentTask.pendingScoreUp &&
            currentTask.type == TaskType.TODO &&
            currentTask.completed
        ) {
            if (taskChecked(null, currentTask, true, true, null) == null) return null
            currentTask = getLatestLocalTask(currentTask)
        }

        return if (currentTask.pendingDelete ||
            currentTask.pendingPosition ||
            currentTask.pendingScoreUp
        ) {
            null
        } else {
            currentTask
        }
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
