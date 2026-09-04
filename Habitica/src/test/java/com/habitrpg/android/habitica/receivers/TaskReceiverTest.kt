package com.habitrpg.android.habitica.receivers

import com.habitrpg.android.habitica.models.tasks.Task
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe

class TaskReceiverTest : WordSpec({
    "task reminder account gate" should {
        "show a reminder only for the currently authenticated owner" {
            val task = Task().apply { ownerID = "expected-user" }

            task.isReminderVisibleFor("expected-user") shouldBe true
            task.isReminderVisibleFor("other-user") shouldBe false
            task.isReminderVisibleFor("") shouldBe false
            task.isReminderVisibleFor(null) shouldBe false
        }
    }
})
