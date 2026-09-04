package com.habitrpg.android.habitica.data.sync

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.UserRepository
import com.habitrpg.android.habitica.helpers.NotificationsManager
import com.habitrpg.android.habitica.helpers.TaskAlarmManager
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.android.habitica.widget.glance.work.WidgetSnapshotPublisher
import com.habitrpg.common.habitica.api.HostConfig
import com.habitrpg.shared.habitica.models.tasks.TaskType
import dagger.hilt.EntryPoint
import dagger.hilt.InstallIn
import dagger.hilt.android.EntryPointAccessors
import dagger.hilt.android.qualifiers.ApplicationContext
import dagger.hilt.components.SingletonComponent
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

private const val OFFLINE_TASK_ALIAS_PREFIX = "android_"

internal fun offlineCreateAlias(taskID: String): String = "$OFFLINE_TASK_ALIAS_PREFIX$taskID"

internal fun isOfflineCreateAlias(identifier: String): Boolean {
    return identifier.startsWith(OFFLINE_TASK_ALIAS_PREFIX)
}

/** Selects whether a sync request follows running work or intentionally restarts it. */
internal fun offlineTaskExistingWorkPolicy(replaceExisting: Boolean): ExistingWorkPolicy {
    return if (replaceExisting) {
        ExistingWorkPolicy.REPLACE
    } else {
        ExistingWorkPolicy.APPEND_OR_REPLACE
    }
}

/** Confirms that queued work still belongs to the active account and API server. */
internal fun offlineTaskIdentityMatches(
    expectedUserID: String,
    expectedServerOrigin: String,
    currentUserID: String,
    currentServerOrigin: String,
): Boolean {
    return expectedUserID.isNotBlank() && expectedServerOrigin.isNotBlank() &&
        expectedUserID == currentUserID && expectedServerOrigin == currentServerOrigin
}

internal fun Task.offlineCreateAlias(): String? {
    return alias?.takeIf { it.isNotBlank() }
}

internal fun Task.canQueueOfflineCreation(): Boolean {
    return !isGroupTask &&
        challengeID.isNullOrBlank() &&
        type in setOf(TaskType.HABIT, TaskType.DAILY, TaskType.TODO, TaskType.REWARD)
}

internal fun Task.canQueueOfflineTodoOperation(): Boolean {
    return !isGroupTask &&
        challengeID.isNullOrBlank() &&
        type == TaskType.TODO
}

internal fun Task.canQueueOfflineTodoScore(): Boolean {
    return canQueueOfflineTodoOperation() && !pendingCreate
}

internal fun Task.hasPendingTodoScore(): Boolean {
    return pendingScoreUp || pendingScoreDown || pendingScoreSnapshot != null
}

internal fun Task.isQueuedOfflineTodoCompletion(): Boolean {
    return canQueueOfflineTodoOperation() && hasPendingTodoScore()
}

internal fun Task.hasQueuedOfflineTodoOperation(): Boolean {
    return canQueueOfflineTodoOperation() &&
        (
            pendingCreate || pendingDelete || pendingDeleteAfterScore || pendingUpdate ||
                pendingPosition || pendingChecklist || hasPendingTodoScore() || pendingScoreRefresh
        )
}

class OfflineTaskSyncScheduler
    @Inject
    constructor(
        @ApplicationContext private val context: Context,
        private val authenticationHandler: AuthenticationHandler,
        private val hostConfig: HostConfig,
    ) {
        fun enqueue(
            expectedUserID: String? = authenticationHandler.currentUserID,
            replaceExisting: Boolean = false,
        ) {
            OfflineTaskSyncWorker.enqueue(
                context,
                expectedUserID,
                hostConfig.serverOrigin(),
                replaceExisting,
            )
        }
    }

class OfflineTaskSyncWorker(
    context: Context,
    params: WorkerParameters,
) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        return withContext(Dispatchers.Main.immediate) {
            doWorkOnMainThread()
        }
    }

    private suspend fun doWorkOnMainThread(): Result {
        val entryPoint =
            EntryPointAccessors.fromApplication(
                applicationContext,
                OfflineTaskSyncEntryPoint::class.java,
            )
        val repository = entryPoint.taskRepository()
        val userRepository = entryPoint.userRepository()
        val authenticationHandler = entryPoint.authenticationHandler()
        val hostConfig = entryPoint.hostConfig()
        val notificationsManager = entryPoint.notificationsManager()
        val taskAlarmManager =
            TaskAlarmManager(applicationContext, repository, authenticationHandler)
        val expectedUserID =
            inputData.getString(EXPECTED_USER_ID_KEY).orEmpty().ifBlank {
                repository.currentUserIDForSync()
            }
        val expectedServerOrigin =
            inputData.getString(EXPECTED_SERVER_ORIGIN_KEY).orEmpty().ifBlank {
                hostConfig.serverOrigin()
            }
        fun identityMatches(): Boolean {
            val authentication = hostConfig.authenticationSnapshot()
            return offlineTaskIdentityMatches(
                expectedUserID,
                expectedServerOrigin,
                repository.currentUserIDForSync(),
                authentication.serverOrigin,
            ) && authentication.userID == expectedUserID && authentication.apiKey.isNotBlank()
        }
        var todosBeforeSync = emptyList<Task>()
        return try {
            if (!identityMatches()) {
                return Result.success()
            }
            todosBeforeSync =
                repository.getTodoCopiesForSync(expectedUserID, expectedServerOrigin)
            for (task in todosBeforeSync.filter { it.pendingDelete || it.pendingDeleteAfterScore }) {
                if (!identityMatches()) return Result.success()
                taskAlarmManager.removeAlarmsForTaskNow(task)
                if (!identityMatches()) return Result.success()
                notificationsManager.dismissTaskNotification(applicationContext, task)
            }
            var syncComplete =
                repository.syncPendingTaskCreations(expectedUserID, expectedServerOrigin)
            if (!identityMatches()) {
                return Result.success()
            }
            val scoreRefreshes =
                repository.getPendingTodoScoreRefreshes(expectedUserID, expectedServerOrigin)
            var refreshComplete = true
            if (scoreRefreshes.isNotEmpty()) {
                val refreshedUser =
                    userRepository.retrieveUser(
                        withTasks = true,
                        forced = true,
                        expectedUserID = expectedUserID,
                        suppressConnectionErrors = true,
                        expectedServerOrigin = expectedServerOrigin,
                    )
                if (!identityMatches()) {
                    return Result.success()
                }
                if (refreshedUser?.id != expectedUserID) {
                    refreshComplete = false
                } else {
                    val refreshedTasks =
                        repository.refreshPendingTodoScoreTasks(
                            scoreRefreshes,
                            expectedUserID,
                            expectedServerOrigin,
                        )
                    val refreshedTaskIDs = refreshedTasks.mapNotNull { it.id }.toSet()
                    refreshedTasks.forEach { task ->
                        if (!identityMatches()) {
                            return Result.success()
                        }
                        if (task.pendingDeleteAfterScore || task.completed ||
                            task.missingDuringScoreRefresh
                        ) {
                            taskAlarmManager.removeAlarmsForTaskNow(task)
                            notificationsManager.dismissTaskNotification(applicationContext, task)
                        }
                    }
                    if (!identityMatches()) {
                        return Result.success()
                    }
                    repository.clearPendingTodoScoreRefreshes(
                        expectedUserID,
                        refreshedTasks,
                        expectedServerOrigin,
                    )
                    refreshComplete =
                        scoreRefreshes.mapNotNull { it.id }.toSet() == refreshedTaskIDs
                    if (refreshComplete && identityMatches()) {
                        syncComplete =
                            repository.syncPendingTaskCreations(
                                expectedUserID,
                                expectedServerOrigin,
                            )
                        if (identityMatches()) {
                            refreshComplete =
                                repository.getPendingTodoScoreRefreshes(
                                    expectedUserID,
                                    expectedServerOrigin,
                                ).isEmpty()
                        }
                    }
                }
            }
            if (!identityMatches()) {
                Result.success()
            } else {
                reconcileTodoSideEffects(
                    repository,
                    authenticationHandler,
                    notificationsManager,
                    taskAlarmManager,
                    expectedUserID,
                    expectedServerOrigin,
                    hostConfig,
                    todosBeforeSync,
                )
                if (syncComplete && refreshComplete) Result.success() else Result.retry()
            }
        } catch (exception: CancellationException) {
            throw exception
        } catch (_: Exception) {
            if (identityMatches()) {
                runCatching {
                    reconcileTodoSideEffects(
                        repository,
                        authenticationHandler,
                        notificationsManager,
                        taskAlarmManager,
                        expectedUserID,
                        expectedServerOrigin,
                        hostConfig,
                        todosBeforeSync,
                    )
                }
            }
            Result.retry()
        } finally {
            runCatching { repository.close() }
            runCatching { userRepository.close() }
        }
    }

    /** Repairs alarms, notifications, and widgets from durable local truth after a sync attempt. */
    private suspend fun reconcileTodoSideEffects(
        repository: TaskRepository,
        authenticationHandler: AuthenticationHandler,
        notificationsManager: NotificationsManager,
        taskAlarmManager: TaskAlarmManager,
        expectedUserID: String,
        expectedServerOrigin: String,
        hostConfig: HostConfig,
        todosBeforeSync: List<Task>,
    ) {
        fun identityMatches(): Boolean {
            val authentication = hostConfig.authenticationSnapshot()
            return offlineTaskIdentityMatches(
                expectedUserID,
                expectedServerOrigin,
                repository.currentUserIDForSync(),
                authentication.serverOrigin,
            ) && authentication.userID == expectedUserID && authentication.apiKey.isNotBlank() &&
                authenticationHandler.currentUserID == expectedUserID
        }
        if (!identityMatches()) return
        val finalTodos = repository.getTodoCopiesForSync(expectedUserID, expectedServerOrigin)
        if (!identityMatches()) return

        val finalTaskIDs = finalTodos.mapNotNull { it.id }.toSet()
        val preservedReminders =
            finalTodos.filterNot { it.pendingDelete || it.pendingDeleteAfterScore }
                .flatMap { it.reminders.orEmpty() }
        finalTodos.forEach { task ->
            if (!identityMatches()) return
            val scoreInFlight = task.hasPendingTodoScore() || task.pendingScoreRefresh
            when {
                task.pendingDelete || task.pendingDeleteAfterScore -> {
                    taskAlarmManager.removeAlarmsForTaskNow(task)
                    if (!identityMatches()) return
                    notificationsManager.dismissTaskNotification(applicationContext, task)
                }
                task.completed && !scoreInFlight -> {
                    taskAlarmManager.removeAlarmsForTaskNow(task)
                    if (!identityMatches()) return
                    notificationsManager.dismissTaskNotification(applicationContext, task)
                }
                !task.completed && !scoreInFlight -> taskAlarmManager.setAlarmsForTaskNow(task)
            }
        }
        todosBeforeSync.forEach { originalTask ->
            if (!identityMatches()) return
            taskAlarmManager.cancelRemovedRemindersAlarms(
                originalTask.reminders.orEmpty(),
                preservedReminders,
            )
            if (originalTask.id !in finalTaskIDs) {
                if (!identityMatches()) return
                notificationsManager.dismissTaskNotification(applicationContext, originalTask)
            }
        }
        if (identityMatches()) {
            WidgetSnapshotPublisher.publishAll(applicationContext)
        }
    }

    @EntryPoint
    @InstallIn(SingletonComponent::class)
    interface OfflineTaskSyncEntryPoint {
        fun taskRepository(): TaskRepository

        fun userRepository(): UserRepository

        fun authenticationHandler(): AuthenticationHandler

        fun hostConfig(): HostConfig

        fun notificationsManager(): NotificationsManager
    }

    companion object {
        private const val WORK_NAME = "offline_task_creation_sync"
        private const val EXPECTED_USER_ID_KEY = "expected_user_id"
        private const val EXPECTED_SERVER_ORIGIN_KEY = "expected_server_origin"

        fun enqueue(
            context: Context,
            expectedUserID: String?,
            expectedServerOrigin: String,
            replaceExisting: Boolean = false,
        ) {
            if (expectedUserID.isNullOrBlank() || expectedServerOrigin.isBlank()) return
            val constraints =
                Constraints
                    .Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build()
            val request =
                OneTimeWorkRequestBuilder<OfflineTaskSyncWorker>()
                    .setInputData(
                        workDataOf(
                            EXPECTED_USER_ID_KEY to expectedUserID.orEmpty(),
                            EXPECTED_SERVER_ORIGIN_KEY to expectedServerOrigin,
                        )
                    )
                    .setConstraints(constraints)
                    .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 15, TimeUnit.SECONDS)
                    .build()
            WorkManager.getInstance(context).enqueueUniqueWork(
                "$WORK_NAME:$expectedUserID",
                offlineTaskExistingWorkPolicy(replaceExisting),
                request,
            )
        }
    }
}
