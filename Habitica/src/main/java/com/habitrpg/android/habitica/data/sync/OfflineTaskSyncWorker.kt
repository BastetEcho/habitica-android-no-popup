package com.habitrpg.android.habitica.data.sync

import android.content.Context
import androidx.annotation.Keep
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters

/** Retains the legacy worker name so queued requests retire without replaying the old outbox. */
@Keep
class OfflineTaskSyncWorker(
    context: Context,
    params: WorkerParameters,
) : CoroutineWorker(context, params) {
    /** Completes scheduled legacy work while leaving its task data untouched for recovery. */
    override suspend fun doWork(): Result = Result.success()
}
