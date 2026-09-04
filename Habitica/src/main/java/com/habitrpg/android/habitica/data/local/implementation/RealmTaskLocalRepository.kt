package com.habitrpg.android.habitica.data.local.implementation

import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.isQueuedOfflineTodoCompletion
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.RemindersItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.shared.habitica.models.tasks.TaskType
import com.habitrpg.shared.habitica.models.tasks.TasksOrder
import io.realm.Realm
import io.realm.RealmResults
import io.realm.Sort
import io.realm.kotlin.toFlow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull

class RealmTaskLocalRepository(realm: Realm) :
    RealmBaseLocalRepository(realm),
    TaskLocalRepository {
    override fun getTasks(
        taskType: TaskType,
        userID: String,
        includedGroupIDs: Array<String>
    ): Flow<List<Task>> {
        if (realm.isClosed) return emptyFlow()
        return findTasks(taskType, userID)
            .toFlow()
            .filter { it.isLoaded }
    }

    private fun findTasks(
        taskType: TaskType,
        ownerID: String
    ): RealmResults<Task> {
        return realm.where(Task::class.java)
            .equalTo("typeValue", taskType.value)
            .equalTo("ownerID", ownerID)
            .equalTo("pendingDelete", false)
            .equalTo("pendingDeleteAfterScore", false)
            .sort("position", Sort.ASCENDING, "dateCreated", Sort.DESCENDING)
            .findAll()
    }

    override fun getTasks(userId: String): Flow<List<Task>> {
        if (realm.isClosed) return emptyFlow()
        return realm.where(Task::class.java).equalTo("ownerID", userId)
            .equalTo("pendingDelete", false)
            .equalTo("pendingDeleteAfterScore", false)
            .sort("position", Sort.ASCENDING, "dateCreated", Sort.DESCENDING)
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun getTasksIncludingPending(userId: String): Flow<List<Task>> {
        if (realm.isClosed) return emptyFlow()
        return realm.where(Task::class.java).equalTo("ownerID", userId)
            .sort("position", Sort.ASCENDING, "dateCreated", Sort.DESCENDING)
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun saveTasks(
        ownerID: String,
        tasksOrder: TasksOrder,
        tasks: TaskList
    ) {
        val sortedTasks = mutableListOf<Task>()
        sortedTasks.addAll(sortTasks(tasks.tasks, tasksOrder.habits))
        sortedTasks.addAll(sortTasks(tasks.tasks, tasksOrder.dailys))
        sortedTasks.addAll(sortTasks(tasks.tasks, tasksOrder.todos))
        sortedTasks.addAll(sortTasks(tasks.tasks, tasksOrder.rewards))
        for (task in tasks.tasks.values) {
            task.position = (sortedTasks.lastOrNull { it.type == task.type }?.position ?: -1) + 1
            sortedTasks.add(task)
        }
        sortedTasks.forEach { task ->
            if (task.ownerID.isBlank()) {
                task.ownerID = ownerID
            }
        }
        removeOldTasks(ownerID, sortedTasks)
        executeTransaction { realm1 ->
            sortedTasks.forEach { incomingTask ->
                val currentTask =
                    incomingTask.combinedID?.let { combinedID ->
                        realm1.where(Task::class.java).equalTo("combinedID", combinedID).findFirst()
                    }
                val pendingAliasTask =
                    incomingTask.alias?.takeIf { it.isNotBlank() }?.let { alias ->
                        realm1.where(Task::class.java)
                            .equalTo("ownerID", incomingTask.ownerID)
                            .equalTo("alias", alias)
                            .equalTo("pendingCreate", true)
                            .findFirst()
                    }
                if (pendingAliasTask != null &&
                    pendingAliasTask.combinedID != incomingTask.combinedID
                ) {
                    return@forEach
                }
                if (currentTask?.hasPendingWriteMutation() != true) {
                    incomingTask.preservePendingScoreReceiptFrom(currentTask)
                    realm1.insertOrUpdate(incomingTask)
                }
            }
        }
        removeOrphanedTaskChildren()
    }

    override fun saveCompletedTodos(
        userId: String,
        tasks: MutableCollection<Task>
    ) {
        tasks.forEach { task ->
            if (task.ownerID.isBlank()) {
                task.ownerID = userId
            }
        }
        removeCompletedTodos(userId, tasks)
        executeTransaction { realm1 ->
            tasks.forEach { incomingTask ->
                val currentTask =
                    incomingTask.combinedID?.let { combinedID ->
                        realm1.where(Task::class.java).equalTo("combinedID", combinedID).findFirst()
                    }
                val pendingAliasTask =
                    incomingTask.alias?.takeIf { it.isNotBlank() }?.let { alias ->
                        realm1.where(Task::class.java)
                            .equalTo("ownerID", incomingTask.ownerID)
                            .equalTo("alias", alias)
                            .equalTo("pendingCreate", true)
                            .findFirst()
                    }
                if (pendingAliasTask != null &&
                    pendingAliasTask.combinedID != incomingTask.combinedID
                ) {
                    return@forEach
                }
                if (currentTask?.hasPendingWriteMutation() != true) {
                    incomingTask.preservePendingScoreReceiptFrom(currentTask)
                    realm1.insertOrUpdate(incomingTask)
                }
            }
        }
        removeOrphanedTaskChildren()
    }

    private fun removeOrphanedTaskChildren() {
        executeTransaction { transactionRealm ->
            val tasks = transactionRealm.where(Task::class.java).findAll()
            val checklistIDs = tasks.flatMap { it.checklist.orEmpty() }.mapNotNull { it.id }.toSet()
            val reminderIDs = tasks.flatMap { it.reminders.orEmpty() }.mapNotNull { it.id }.toSet()
            transactionRealm.where(ChecklistItem::class.java).findAll().toList()
                .filter { it.id !in checklistIDs }
                .forEach { it.deleteFromRealm() }
            transactionRealm.where(RemindersItem::class.java).findAll().toList()
                .filter { it.id !in reminderIDs }
                .forEach { it.deleteFromRealm() }
        }
    }

    private fun sortTasks(
        taskMap: MutableMap<String, Task>,
        taskOrder: List<String>
    ): List<Task> {
        val taskList = ArrayList<Task>()
        var position = 0
        for (taskId in taskOrder) {
            val task = taskMap[taskId]
            if (task != null) {
                task.position = position
                taskList.add(task)
                position++
                taskMap.remove(taskId)
            }
        }
        return taskList
    }

    private fun removeOldTasks(
        ownerID: String,
        onlineTaskList: List<Task>
    ) {
        if (realm.isClosed) return
        val onlineTaskIDs = onlineTaskList.mapNotNull { it.id }.toSet()
        executeTransaction { transactionRealm ->
            transactionRealm.where(Task::class.java)
                .equalTo("ownerID", ownerID)
                .beginGroup()
                .beginGroup()
                .equalTo("typeValue", TaskType.TODO.value)
                .equalTo("completed", false)
                .endGroup()
                .or()
                .notEqualTo("typeValue", TaskType.TODO.value)
                .endGroup()
                .findAll()
                .toList()
                .filterNot { task -> task.hasPendingOutboxMutation() || task.id in onlineTaskIDs }
                .forEach { task -> task.deleteFromRealm() }
        }
    }

    private fun removeCompletedTodos(
        userID: String,
        onlineTaskList: MutableCollection<Task>
    ) {
        val onlineTaskIDs = onlineTaskList.mapNotNull { it.id }.toSet()
        executeTransaction { transactionRealm ->
            transactionRealm.where(Task::class.java)
                .equalTo("ownerID", userID)
                .equalTo("typeValue", TaskType.TODO.value)
                .equalTo("completed", true)
                .findAll()
                .toList()
                .filterNot { task ->
                    task.hasPendingOutboxMutation() ||
                        task.isQueuedOfflineTodoCompletion() ||
                        task.id in onlineTaskIDs
                }
                .forEach { task -> task.deleteFromRealm() }
        }
    }

    override fun deleteTask(taskID: String) {
        deleteTaskInternal(taskID, null)
    }

    override fun deleteTask(
        taskID: String,
        ownerID: String,
    ) {
        deleteTaskInternal(taskID, ownerID)
    }

    private fun deleteTaskInternal(
        taskID: String,
        ownerID: String?,
    ) {
        executeTransaction { transactionRealm ->
            val query = transactionRealm.where(Task::class.java).equalTo("id", taskID)
            ownerID?.let { query.equalTo("ownerID", it) }
            query.findFirst()?.deleteFromRealm()
        }
    }

    override fun replaceTask(
        taskID: String,
        task: Task,
    ) {
        replaceTaskInternal(taskID, null, task)
    }

    override fun replaceTask(
        taskID: String,
        ownerID: String,
        task: Task,
    ) {
        replaceTaskInternal(taskID, ownerID, task)
    }

    private fun replaceTaskInternal(
        taskID: String,
        ownerID: String?,
        task: Task,
    ) {
        executeTransaction { transactionRealm ->
            val query = transactionRealm.where(Task::class.java).equalTo("id", taskID)
            ownerID?.let { query.equalTo("ownerID", it) }
            query.findFirst()?.deleteFromRealm()
            transactionRealm.insertOrUpdate(task)
        }
    }

    override fun resolveTaskID(
        taskID: String,
        alias: String,
    ): String {
        return resolveTaskIDInternal(taskID, alias, null)
    }

    override fun resolveTaskID(
        taskID: String,
        alias: String,
        ownerID: String,
    ): String {
        return resolveTaskIDInternal(taskID, alias, ownerID)
    }

    private fun resolveTaskIDInternal(
        taskID: String,
        alias: String,
        ownerID: String?,
    ): String {
        if (realm.isClosed) return taskID
        val directQuery = realm.where(Task::class.java).equalTo("id", taskID)
        ownerID?.let { directQuery.equalTo("ownerID", it) }
        val directTask = directQuery.findFirst()
        if (directTask?.id != null) return directTask.id.orEmpty()
        val aliasQuery = realm.where(Task::class.java).equalTo("alias", alias)
        ownerID?.let { aliasQuery.equalTo("ownerID", it) }
        return aliasQuery.findFirst()?.id ?: taskID
    }

    override fun getTask(taskId: String): Flow<Task> {
        return getTaskInternal(taskId, null)
    }

    override fun getTask(
        taskId: String,
        ownerID: String,
    ): Flow<Task> {
        return getTaskInternal(taskId, ownerID)
    }

    private fun getTaskInternal(
        taskId: String,
        ownerID: String?,
    ): Flow<Task> {
        if (realm.isClosed) {
            return emptyFlow()
        }
        val query = realm.where(Task::class.java).equalTo("id", taskId)
        ownerID?.let { query.equalTo("ownerID", it) }
        return query.findAll().toFlow()
            .filter { realmObject -> realmObject.isLoaded && realmObject.isNotEmpty() }.mapNotNull { it.first() }
    }

    override fun getTaskCopy(taskId: String): Flow<Task> {
        return getTaskCopyInternal(taskId, null)
    }

    override fun getTaskCopy(
        taskId: String,
        ownerID: String,
    ): Flow<Task> {
        return getTaskCopyInternal(taskId, ownerID)
    }

    private fun getTaskCopyInternal(
        taskId: String,
        ownerID: String?,
    ): Flow<Task> {
        return getTaskInternal(taskId, ownerID)
            .map { task ->
                return@map if (task.isManaged && task.isValid) {
                    realm.copyFromRealm(task)
                } else {
                    task
                }
            }
    }

    override fun markTaskCompleted(
        taskId: String,
        isCompleted: Boolean
    ) {
        val task = realm.where(Task::class.java).equalTo("id", taskId).findFirst()
        executeTransaction { task?.completed = isCompleted }
    }

    override fun swapTaskPosition(
        firstPosition: Int,
        secondPosition: Int
    ) {
        val firstTask =
            realm.where(Task::class.java)
                .equalTo("position", firstPosition)
                .equalTo("pendingDelete", false)
                .equalTo("pendingDeleteAfterScore", false)
                .findFirst()
        val secondTask =
            realm.where(Task::class.java)
                .equalTo("position", secondPosition)
                .equalTo("pendingDelete", false)
                .equalTo("pendingDeleteAfterScore", false)
                .findFirst()
        if (firstTask != null && secondTask != null && firstTask.isValid && secondTask.isValid) {
            executeTransaction {
                firstTask.position = secondPosition
                secondTask.position = firstPosition
            }
        }
    }

    override fun moveTaskToPosition(
        taskID: String,
        ownerID: String,
        serverOrigin: String,
        taskType: TaskType,
        newPosition: Int,
    ) {
        if (realm.isClosed) return
        executeTransaction { transactionRealm ->
            val orderedTasks =
                transactionRealm.where(Task::class.java)
                    .equalTo("ownerID", ownerID)
                    .equalTo("typeValue", taskType.value)
                    .equalTo("pendingDelete", false)
                    .equalTo("pendingDeleteAfterScore", false)
                    .beginGroup()
                    .isNull("outboxServerOrigin")
                    .or()
                    .equalTo("outboxServerOrigin", "")
                    .or()
                    .equalTo("outboxServerOrigin", serverOrigin)
                    .endGroup()
                    .sort("position", Sort.ASCENDING, "dateCreated", Sort.DESCENDING)
                    .findAll()
                    .filter { task -> task.isInLocalMoveScope(ownerID, serverOrigin, taskType) }
                    .toMutableList()
            val targetTask = orderedTasks.firstOrNull { it.id == taskID } ?: return@executeTransaction
            if (targetTask.outboxServerOrigin?.takeIf(String::isNotBlank) != null &&
                targetTask.outboxServerOrigin != serverOrigin
            ) {
                return@executeTransaction
            }
            orderedTasks.remove(targetTask)
            orderedTasks.add(newPosition.coerceIn(0, orderedTasks.size), targetTask)
            orderedTasks.forEachIndexed { position, task -> task.position = position }
            targetTask.pendingPosition = true
            targetTask.outboxServerOrigin = serverOrigin
            targetTask.isSaving = false
            targetTask.hasErrored = false
        }
    }

    override fun getTaskAtPosition(
        taskType: String,
        position: Int
    ): Flow<Task> {
        return realm.where(Task::class.java).equalTo("typeValue", taskType)
            .equalTo("position", position)
            .equalTo("pendingDelete", false)
            .equalTo("pendingDeleteAfterScore", false)
            .findAll()
            .toFlow()
            .filter { realmObject -> realmObject.isLoaded && realmObject.isNotEmpty() }.mapNotNull { it.first() }
    }

    override fun updateIsdue(daily: TaskList): TaskList {
        val tasks =
            realm.where(Task::class.java).equalTo("typeValue", TaskType.DAILY.value).findAll()
        realm.beginTransaction()
        tasks.filter { daily.tasks.containsKey(it.id) }
            .forEach { it.isDue = daily.tasks[it.id]?.isDue }
        realm.commitTransaction()
        return daily
    }

    override fun updateTaskPositions(taskOrder: List<String>) {
        updateTaskPositionsInternal(taskOrder, null)
    }

    override fun updateTaskPositions(
        taskOrder: List<String>,
        ownerID: String,
    ) {
        updateTaskPositionsInternal(taskOrder, ownerID)
    }

    private fun updateTaskPositionsInternal(
        taskOrder: List<String>,
        ownerID: String?,
    ) {
        if (taskOrder.isNotEmpty()) {
            executeTransaction { transactionRealm ->
                val query =
                    transactionRealm.where(Task::class.java)
                        .`in`("id", taskOrder.toTypedArray())
                ownerID?.let { query.equalTo("ownerID", it) }
                query
                    .findAll()
                    .filterNot { it.pendingPosition }
                    .forEach { it.position = taskOrder.indexOf(it.id) }
            }
        }
    }

    override fun getErroredTasks(userID: String): Flow<List<Task>> {
        return realm.where(Task::class.java)
            .equalTo("ownerID", userID)
            .equalTo("hasErrored", true)
            .sort("position")
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun getPendingTaskCreations(userID: String): Flow<List<Task>> {
        return realm.where(Task::class.java)
            .equalTo("ownerID", userID)
            .equalTo("pendingCreate", true)
            .equalTo("pendingDelete", false)
            .sort("position")
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun getPendingTaskDeletions(userID: String): Flow<List<Task>> {
        return realm.where(Task::class.java)
            .equalTo("ownerID", userID)
            .equalTo("pendingDelete", true)
            .sort("position")
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun getPendingTaskActions(userID: String): Flow<List<Task>> {
        return realm.where(Task::class.java)
            .equalTo("ownerID", userID)
            .equalTo("pendingCreate", false)
            .equalTo("pendingDelete", false)
            .beginGroup()
            .equalTo("pendingUpdate", true)
            .or()
            .equalTo("pendingPosition", true)
            .or()
            .equalTo("pendingScoreUp", true)
            .or()
            .equalTo("pendingScoreDown", true)
            .or()
            .isNotNull("pendingScoreSnapshot")
            .or()
            .equalTo("pendingScoreRefresh", true)
            .or()
            .equalTo("pendingDeleteAfterScore", true)
            .or()
            .equalTo("pendingChecklist", true)
            .endGroup()
            .sort("position")
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun getUser(userID: String): Flow<User> {
        return realm.where(User::class.java)
            .equalTo("id", userID)
            .findAll()
            .toFlow()
            .filter { realmObject -> realmObject.isLoaded && realmObject.isValid && !realmObject.isEmpty() }.mapNotNull { users -> users.first() }
    }

    override fun getTasksForChallenge(
        challengeID: String?,
        userID: String?
    ): Flow<List<Task>> {
        return realm.where(Task::class.java)
            .equalTo("challengeID", challengeID)
            .equalTo("ownerID", userID)
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }

    override fun getPendingTaskScoreRefreshes(userID: String): Flow<List<Task>> {
        if (realm.isClosed) return emptyFlow()
        return realm.where(Task::class.java)
            .equalTo("ownerID", userID)
            .equalTo("pendingScoreRefresh", true)
            .findAll()
            .toFlow()
            .filter { it.isLoaded }
    }
}

private fun Task.hasPendingOutboxMutation(): Boolean {
    return pendingCreate || pendingDelete || pendingDeleteAfterScore || pendingUpdate || pendingPosition ||
        pendingScoreUp || pendingScoreDown || pendingScoreSnapshot != null || pendingChecklist ||
        pendingScoreRefresh
}

private fun Task.hasPendingWriteMutation(): Boolean {
    return pendingCreate || pendingDelete || pendingDeleteAfterScore || pendingUpdate || pendingPosition ||
        pendingScoreUp || pendingScoreDown || pendingScoreSnapshot != null || pendingChecklist
}

/** Preserves a receipt-only score refresh and the server identity that owns it. */
internal fun Task.preservePendingScoreReceiptFrom(currentTask: Task?) {
    pendingScoreRefresh = currentTask?.pendingScoreRefresh == true
    outboxServerOrigin = currentTask?.outboxServerOrigin.takeIf { pendingScoreRefresh }
}

/** Keeps a local reorder from touching retained rows that belong to another server. */
internal fun Task.isInLocalMoveScope(
    ownerID: String,
    serverOrigin: String,
    taskType: TaskType,
): Boolean {
    return this.ownerID == ownerID && type == taskType && !pendingDelete && !pendingDeleteAfterScore &&
        (outboxServerOrigin.isNullOrBlank() || outboxServerOrigin == serverOrigin)
}
