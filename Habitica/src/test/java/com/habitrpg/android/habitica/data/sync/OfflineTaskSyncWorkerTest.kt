package com.habitrpg.android.habitica.data.sync

import android.content.Context
import androidx.work.ListenableWorker
import androidx.work.WorkerParameters
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.Called
import io.mockk.clearMocks
import io.mockk.mockk
import io.mockk.verify

class OfflineTaskSyncWorkerTest : WordSpec({
    "legacy offline work" should {
        "finish without accessing application services or queued request data" {
            val context = mockk<Context>()
            val params = mockk<WorkerParameters>(relaxed = true)
            val worker = OfflineTaskSyncWorker(context, params)
            clearMocks(context, params, answers = false)

            worker.doWork() shouldBe ListenableWorker.Result.success()

            verify { context wasNot Called }
            verify { params wasNot Called }
        }
    }
})
