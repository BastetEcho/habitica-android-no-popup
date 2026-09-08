package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.google.gson.JsonObject
import com.google.gson.JsonArray
import com.habitrpg.android.habitica.data.sync.TodoOperationType
import com.habitrpg.android.habitica.data.sync.TodoOutbox
import com.habitrpg.android.habitica.data.sync.TodoTaskOrdering
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
import kotlinx.coroutines.withTimeoutOrNull
import java.text.SimpleDateFormat
import java.io.IOException
import java.util.Date
import java.util.Locale
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

@ExperimentalCoroutinesApi
class TaskRepositoryImpl(
    localRepository: TaskLocalRepository,
    apiClient: ApiClient,
    authenticationHandler: AuthenticationHandler,
    val appConfigManager: AppConfigManager,
    private val todoOutbox: TodoOutbox? = null
) : BaseRepositoryImpl<TaskLocalRepository>(localRepository, apiClient, authenticationHandler),
    TaskRepository {
    private var lastTaskAction: Long = 0

    override fun refreshLocalData() {
        val r = localRepository.realm
        if (r.isClosed) return
        try {
            r.refresh()
            restorePendingTodos()
            todoOutbox?.resume()
        } catch (_: IllegalStateException) {
        }
    }

    override fun getTasks(
        taskType: TaskType,
        userID: String?,
        includedGroupIDs: Array<String>
    ): Flow<List<Task>> {
        restorePendingTodos()
        todoOutbox?.resume()
        return this.localRepository.getTasks(
            taskType,
            userID ?: authenticationHandler.currentUserID ?: "",
            includedGroupIDs
        )
    }

    override fun saveTasks(
        userId: String,
        order: TasksOrder,
        tasks: TaskList
    ) {
        if (!isCurrentTaskRead(tasks)) return
        if (todoOutbox?.scope()?.userId == userId) mergePendingTodos(tasks)
        localRepository.saveTasks(userId, order, tasks)
        restorePendingTodos()
        todoOutbox?.resume()
    }

    override suspend fun retrieveTasks(
        userId: String,
        tasksOrder: TasksOrder
    ): TaskList? {
        val tasks = apiClient.getTasks() ?: return null
        if (!isCurrentTaskRead(tasks)) return null
        saveTasks(userId, tasksOrder, tasks)
        return tasks
    }

    override suspend fun retrieveCompletedTodos(userId: String?): TaskList? {
        val taskList = this.apiClient.getTasks("completedTodos") ?: return null
        if (!isCurrentTaskRead(taskList)) return null
        if (todoOutbox?.scope()?.userId == (userId ?: authenticationHandler.currentUserID)) {
            mergePendingTodos(taskList, completedOnly = true)
        }
        val tasks = taskList.tasks
        this.localRepository.saveCompletedTodos(
            userId ?: authenticationHandler.currentUserID ?: "",
            tasks.values
        )
        restorePendingTodos()
        return taskList
    }

    override suspend fun retrieveTasks(
        userId: String,
        tasksOrder: TasksOrder,
        dueDate: Date
    ): TaskList? {
        val formatter = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZZZ", Locale.US)
        val taskList = this.apiClient.getTasks("dailys", formatter.format(dueDate)) ?: return null
        if (!isCurrentTaskRead(taskList)) return null
        saveTasks(userId, tasksOrder, taskList)
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
        if (todoOutbox?.handles(task) == true) {
            val local = localRepository.getUnmanagedCopy(task)
            if (local.completed == up) return null
            local.completed = up
            todoOutbox.enqueue(local, TodoOperationType.SCORE, JsonObject().apply { addProperty("up", up) })
            restorePendingTodos()
            // The initiating view already plays its sound. Stats, quests, and callbacks wait for the server.
            return null
        }
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
        val now = Date().time
        val id = task.id
        if (lastTaskAction > now - 500 && !force || id == null) {
            return null
        }

        lastTaskAction = now
        val res =
            this.apiClient.postTaskDirection(
                id,
                (if (up) TaskDirection.UP else TaskDirection.DOWN).text
            ) ?: return null
        // There are cases where the user object is not set correctly. So the app refetches it as a fallback
        val thisUser =
            user ?: localRepository.getUser(authenticationHandler.currentUserID ?: "").firstOrNull()
                ?: return null
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
        val task = getTask(taskId).firstOrNull() ?: return null
        return taskChecked(user, task, up, force, notifyFunc)
    }

    override suspend fun scoreChecklistItem(
        taskId: String,
        itemId: String
    ): Task? {
        val localTask = pendingEligibleTask(taskId)
        if (localTask != null) {
            val item = localTask.checklist?.firstOrNull { it.id == itemId } ?: return localTask
            item.completed = !item.completed
            todoOutbox?.enqueue(localTask, TodoOperationType.CHECKLIST, JsonObject().apply {
                addProperty("itemId", itemId)
                addProperty("completed", item.completed)
            })
            restorePendingTodos()
            return localTask
        }
        val task = apiClient.scoreChecklistItem(taskId, itemId)
        val updatedItem: ChecklistItem? = task?.checklist?.lastOrNull { itemId == it.id }
        if (updatedItem != null) {
            localRepository.save(updatedItem)
        }
        return task
    }

    override fun getTask(taskId: String) = localRepository.getTask(todoOutbox?.resolve(taskId) ?: taskId)

    override fun getTaskCopy(taskId: String) = localRepository.getTaskCopy(todoOutbox?.resolve(taskId) ?: taskId)

    override suspend fun createTask(
        task: Task,
        force: Boolean
    ): Task? {
        if (todoOutbox?.handles(task) == true) {
            val local = localRepository.getUnmanagedCopy(task)
            todoOutbox.enqueue(local, TodoOperationType.CREATE)
            restorePendingTodos()
            return local
        }
        val now = Date().time
        if (lastTaskAction > now - 500 && !force) {
            return null
        }
        lastTaskAction = now

        task.isSaving = true
        task.isCreating = true
        task.hasErrored = false
        task.ownerID =
            if (task.isGroupTask) {
                task.group?.groupID ?: ""
            } else {
                authenticationHandler.currentUserID ?: ""
            }
        if (task.id == null) {
            task.id = UUID.randomUUID().toString()
        }
        localRepository.save(task)

        val savedTask =
            if (task.isGroupTask) {
                apiClient.createGroupTask(task.group?.groupID ?: "", task)
            } else {
                apiClient.createTask(task)
            }
        savedTask?.dateCreated = Date()
        if (savedTask != null) {
            savedTask.tags = task.tags
            localRepository.save(savedTask)
        } else {
            task.hasErrored = true
            task.isSaving = false
            localRepository.save(task)
        }
        return savedTask
    }

    @Suppress("ReturnCount")
    override suspend fun updateTask(
        task: Task,
        force: Boolean
    ): Task? {
        if (todoOutbox?.handles(task) == true) {
            val local = localRepository.getUnmanagedCopy(task)
            task.id?.let(::pendingEligibleTask)?.let { current ->
                local.completed = current.completed
                local.position = current.position
                local.value = current.value
            }
            todoOutbox.enqueue(local, TodoOperationType.UPDATE)
            restorePendingTodos()
            return local
        }
        val now = Date().time
        if ((lastTaskAction > now - 500 && !force) || !task.isValid) {
            return task
        }
        lastTaskAction = now
        val id = task.id ?: return task
        val unmanagedTask = localRepository.getUnmanagedCopy(task)
        unmanagedTask.isSaving = true
        unmanagedTask.hasErrored = false
        localRepository.save(unmanagedTask)
        val savedTask = apiClient.updateTask(id, unmanagedTask)
        savedTask?.position = task.position
        savedTask?.id = task.id
        savedTask?.ownerID = task.ownerID
        if (savedTask != null) {
            savedTask.tags = task.tags
            localRepository.save(savedTask)
        } else {
            unmanagedTask.hasErrored = true
            unmanagedTask.isSaving = false
            localRepository.save(unmanagedTask)
        }
        return savedTask
    }

    override suspend fun deleteTask(taskId: String): Void? {
        val localTask = pendingEligibleTask(taskId)
        if (localTask != null) {
            todoOutbox?.enqueue(localTask, TodoOperationType.DELETE)
            restorePendingTodos()
            return null
        }
        apiClient.deleteTask(taskId) ?: return null
        localRepository.deleteTask(taskId)
        return null
    }

    override fun saveTask(task: Task) {
        localRepository.save(task)
    }

    override suspend fun createTasks(newTasks: List<Task>): List<Task>? {
        if (newTasks.any { todoOutbox?.handles(it) == true }) {
            return newTasks.mapNotNull { createTask(it, force = true) }
        }
        return apiClient.createTasks(newTasks)
    }

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
        val localTask = pendingEligibleTask(taskID)
        if (localTask != null) {
            // Completed Todos are not members of the server's active Todo order.
            if (localTask.completed) return null
            val outbox = requireNotNull(todoOutbox)
            val owner = requireNotNull(outbox.scope()).userId
            val currentOrder = localRepository.realm.where(Task::class.java)
                .equalTo("ownerID", owner).equalTo("typeValue", TaskType.TODO.value)
                .equalTo("completed", false)
                .sort("position", io.realm.Sort.ASCENDING, "dateCreated", io.realm.Sort.DESCENDING)
                .findAll().filterNot { it.isGroupTask }.mapNotNull { it.id }.map(outbox::resolve).distinct()
            val projectedOrder = outbox.pendingOrder(currentOrder) ?: currentOrder
            val taskId = outbox.resolve(requireNotNull(localTask.id))
            val order = TodoTaskOrdering.move(projectedOrder, taskId, newPosition)
            localTask.position = order.indexOf(taskId)
            outbox.enqueue(localTask, TodoOperationType.MOVE, JsonObject().apply {
                addProperty("position", newPosition)
                add(TodoTaskOrdering.LOCAL_ORDER, JsonArray().apply { order.forEach { add(it) } })
            })
            restorePendingTodos()
            return order
        }
        val task = getTask(taskID).firstOrNull()
        val positions = if (task?.isGroupTask == true) {
            apiClient.postGroupTaskNewPosition(taskID, newPosition)
        } else {
            apiClient.postTaskNewPosition(taskID, newPosition)
        } ?: return null
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
        todoOutbox?.resume()
        val tasks = localRepository.getErroredTasks(currentUserID).firstOrNull()
        return tasks?.map { localRepository.getUnmanagedCopy(it) }?.mapNotNull {
            if (it.isCreating) {
                createTask(it, true)
            } else {
                updateTask(it, true)
            }
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

    /** Finds only the signed-in account's personal task and detaches it from Realm before queuing. */
    private fun pendingEligibleTask(taskId: String): Task? {
        val outbox = todoOutbox ?: return null
        val scope = outbox.scope() ?: return null
        val task = localRepository.realm.where(Task::class.java)
            .equalTo("id", outbox.resolve(taskId)).equalTo("ownerID", scope.userId).findFirst()
            ?: localRepository.realm.where(Task::class.java)
                .equalTo("id", taskId).equalTo("ownerID", scope.userId).findFirst()
            ?: return null
        return if (outbox.handles(task)) localRepository.getUnmanagedCopy(task) else null
    }

    /** Rejects reads that started before a newer local mutation or acknowledged server result. */
    private fun isCurrentTaskRead(tasks: TaskList): Boolean =
        tasks.readGeneration?.let { it == apiClient.taskReadGeneration } ?: true

    /** Adds pending records before cache reconciliation so refresh cannot discard local edits. */
    private fun mergePendingTodos(tasks: TaskList, completedOnly: Boolean = false) {
        todoOutbox?.projections()?.forEach { (localId, task) ->
            tasks.tasks.remove(localId)
            tasks.tasks.remove(todoOutbox.resolve(localId))
            if (task != null && (!completedOnly || task.completed)) {
                tasks.tasks[requireNotNull(task.id)] = task
            }
        }
    }

    /** Materializes durable local truth without converting the adapter's RealmResults to a List. */
    private fun restorePendingTodos(acknowledgingSequence: Long? = null) {
        val outbox = todoOutbox ?: return
        val scope = outbox.scope() ?: return
        val projections = outbox.projections(acknowledgingSequence)
        if (localRepository.isClosed) return
        localRepository.executeTransaction { realm ->
            projections.forEach { (localId, task) ->
                val serverId = outbox.resolve(localId)
                if (task == null || localId != serverId) {
                    realm.where(Task::class.java).equalTo("ownerID", scope.userId)
                        .equalTo("id", localId).findAll().deleteAllFromRealm()
                }
                if (task == null) {
                    realm.where(Task::class.java).equalTo("ownerID", scope.userId)
                        .equalTo("id", serverId).findAll().deleteAllFromRealm()
                } else realm.insertOrUpdate(task)
            }
            val personalTodos = realm.where(Task::class.java)
                .equalTo("ownerID", scope.userId).equalTo("typeValue", TaskType.TODO.value)
                .equalTo("completed", false)
                .sort("position", io.realm.Sort.ASCENDING, "dateCreated", io.realm.Sort.DESCENDING)
                .findAll().filterNot { it.isGroupTask }
            val order = outbox.pendingOrder(personalTodos.mapNotNull { it.id }, acknowledgingSequence)
            if (order != null) {
                val positions = order.withIndex().associate { it.value to it.index }
                personalTodos.forEach { task -> positions[task.id]?.let { task.position = it } }
            }
        }
    }

    /** Applies checkpointed replies silently, then reconciles canonical server account data. */
    override suspend fun syncPendingTodos(): Boolean {
        val outbox = todoOutbox ?: return true
        val scope = outbox.scope() ?: return true
        restorePendingTodos()
        val drained = outbox.replay { operation, reply ->
            val local = pendingEligibleTask(operation.taskId)
            reply.snapshot?.let { snapshot ->
                val serverTask = outbox.codec.restore(snapshot)
                local?.let { serverTask.position = it.position; serverTask.tags = it.tags }
                localRepository.save(serverTask)
                if (operation.taskId != serverTask.id) {
                    localRepository.executeTransaction { realm ->
                        realm.where(Task::class.java).equalTo("ownerID", scope.userId)
                            .equalTo("id", operation.taskId).findAll().deleteAllFromRealm()
                    }
                }
            }
            reply.order?.let { localRepository.updateTaskPositions(it) }
            if (reply.deleted) {
                reply.serverId?.let { localRepository.deleteTask(it) }
            }
            if (reply.refreshUser) {
                // Persisted receipt remains pending if this read fails; never apply additive quest/drop effects twice.
                val user = apiClient.todoRemoteApi.forAccount(scope.server, scope.userId).user()
                if (outbox.scope() != scope || user.id != scope.userId) {
                    throw IOException("Todo scoring account changed")
                }
                localRepository.save(user)
            }
            restorePendingTodos(operation.sequence)
        }
        return drained
    }
}
