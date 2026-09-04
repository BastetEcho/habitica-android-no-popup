package com.habitrpg.android.habitica.data.sync

import androidx.work.ExistingWorkPolicy
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe

class OfflineTaskSyncWorkerTest : WordSpec({
    "offline task sync scheduling" should {
        "append normal requests but replace explicit restarts" {
            offlineTaskExistingWorkPolicy(false) shouldBe ExistingWorkPolicy.APPEND_OR_REPLACE
            offlineTaskExistingWorkPolicy(true) shouldBe ExistingWorkPolicy.REPLACE
        }
    }
})
