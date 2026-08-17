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
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.shared.habitica.models.tasks.TaskType
import dagger.hilt.EntryPoint
import dagger.hilt.InstallIn
import dagger.hilt.android.EntryPointAccessors
import dagger.hilt.android.qualifiers.ApplicationContext
import dagger.hilt.components.SingletonComponent
import java.util.concurrent.TimeUnit
import javax.inject.Inject

internal fun Task.canQueueOfflineCreation(): Boolean {
    return !isGroupTask &&
        challengeID.isNullOrBlank() &&
        type in setOf(TaskType.HABIT, TaskType.DAILY, TaskType.TODO, TaskType.REWARD)
}

class OfflineTaskSyncScheduler
    @Inject
    constructor(
        @ApplicationContext private val context: Context,
    ) {
        fun enqueue() {
            OfflineTaskSyncWorker.enqueue(context)
        }
    }

class OfflineTaskSyncWorker(
    context: Context,
    params: WorkerParameters,
) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val repository =
            EntryPointAccessors
                .fromApplication(
                    applicationContext,
                    OfflineTaskSyncEntryPoint::class.java,
                ).taskRepository()
        return try {
            if (repository.syncPendingTaskCreations()) Result.success() else Result.retry()
        } finally {
            repository.close()
        }
    }

    @EntryPoint
    @InstallIn(SingletonComponent::class)
    interface OfflineTaskSyncEntryPoint {
        fun taskRepository(): TaskRepository
    }

    companion object {
        private const val WORK_NAME = "offline_task_creation_sync"

        fun enqueue(context: Context) {
            val constraints =
                Constraints
                    .Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build()
            val request =
                OneTimeWorkRequestBuilder<OfflineTaskSyncWorker>()
                    .setConstraints(constraints)
                    .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 15, TimeUnit.SECONDS)
                    .build()
            WorkManager.getInstance(context).enqueueUniqueWork(
                WORK_NAME,
                ExistingWorkPolicy.APPEND_OR_REPLACE,
                request,
            )
        }
    }
}
