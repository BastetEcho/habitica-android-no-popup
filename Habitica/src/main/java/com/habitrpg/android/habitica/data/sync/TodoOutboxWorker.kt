package com.habitrpg.android.habitica.data.sync

import android.content.Context
import androidx.hilt.work.HiltWorker
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.habitrpg.android.habitica.data.TaskRepository
import dagger.assisted.Assisted
import dagger.assisted.AssistedInject
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import javax.inject.Provider

/** Replays personal Todo operations without invoking foreground feedback or audio. */
@HiltWorker
class TodoOutboxWorker @AssistedInject constructor(
    @Assisted context: Context,
    @Assisted params: WorkerParameters,
    private val repositoryProvider: Provider<TaskRepository>,
) : CoroutineWorker(context, params) {
    /** Creates, drains, and closes the thread-confined repository on the main dispatcher. */
    override suspend fun doWork(): Result = withContext(Dispatchers.Main.immediate) {
        try {
            val repository = repositoryProvider.get()
            try {
                if (repository.syncPendingTodos()) Result.success() else Result.retry()
            } finally {
                repository.close()
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            Result.retry()
        }
    }
}
