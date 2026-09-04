package com.habitrpg.android.habitica.data.local.implementation

import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe

class RealmTaskLocalRepositoryOverlayTest : WordSpec({
    "server refresh score receipt overlay" should {
        "preserve the receipt server origin" {
            val current =
                Task().apply {
                    pendingScoreRefresh = true
                    outboxServerOrigin = "https://example.com/api/v4/"
                }
            val incoming = Task()

            incoming.preservePendingScoreReceiptFrom(current)

            incoming.pendingScoreRefresh shouldBe true
            incoming.outboxServerOrigin shouldBe "https://example.com/api/v4/"
        }

        "clear a stale origin when no receipt remains" {
            val incoming =
                Task().apply {
                    pendingScoreRefresh = true
                    outboxServerOrigin = "https://stale.example/api/v4/"
                }

            incoming.preservePendingScoreReceiptFrom(Task())

            incoming.pendingScoreRefresh shouldBe false
            incoming.outboxServerOrigin shouldBe null
        }
    }

    "local reorder scope" should {
        "exclude a same-owner task retained from another server" {
            val task =
                Task().apply {
                    ownerID = "expected-user"
                    type = TaskType.TODO
                    outboxServerOrigin = "https://old.example/api/v4/"
                }

            task.isInLocalMoveScope(
                "expected-user",
                "https://new.example/api/v4/",
                TaskType.TODO,
            ) shouldBe false
            task.isInLocalMoveScope(
                "expected-user",
                "https://old.example/api/v4/",
                TaskType.TODO,
            ) shouldBe true
        }
    }
})
