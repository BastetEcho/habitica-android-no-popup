package com.habitrpg.android.habitica.data.sync

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import dagger.hilt.android.qualifiers.ApplicationContext
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton

/** Schedules durable, connected-only replay of the personal Todo outbox. */
@Singleton
class TodoSyncScheduler @Inject constructor(
    @ApplicationContext private val context: Context,
) {
    /** Enqueues a follow-up drain without losing requests accepted during an active drain. */
    fun schedule() {
        val request = OneTimeWorkRequestBuilder<TodoOutboxWorker>()
            .setConstraints(
                Constraints.Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build(),
            )
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            "todo-outbox-v1",
            ExistingWorkPolicy.APPEND_OR_REPLACE,
            request,
        )
    }
}
