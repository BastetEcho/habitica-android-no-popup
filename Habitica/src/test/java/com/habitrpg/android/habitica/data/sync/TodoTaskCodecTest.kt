package com.habitrpg.android.habitica.data.sync

import com.habitrpg.android.habitica.models.Tag
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.RemindersItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.realm.RealmList
import java.util.Date

/** Tests offline snapshot completeness separately from the intentionally restricted API body. */
class TodoTaskCodecTest : WordSpec({
    "Todo snapshots" should {
        "round trip all supported personal Todo fields and clear transient error presentation" {
            val codec = TodoTaskCodec()
            val task = codecFixture()

            val restored = codec.restore(codec.snapshot(task))

            restored.id shouldBe task.id
            restored.ownerID shouldBe "test-user"
            restored.combinedID shouldBe task.combinedID
            restored.type shouldBe TaskType.TODO
            restored.text shouldBe task.text
            restored.notes shouldBe task.notes
            restored.priority shouldBe task.priority
            restored.attributeValue shouldBe task.attributeValue
            restored.value shouldBe task.value
            restored.completed shouldBe true
            restored.position shouldBe task.position
            restored.dateCreated shouldBe task.dateCreated
            restored.dueDate shouldBe task.dueDate
            restored.updatedAt shouldBe task.updatedAt
            restored.challengeID shouldBe task.challengeID
            restored.challengeBroken shouldBe task.challengeBroken
            restored.tags?.single()?.let {
                it.id shouldBe "fixture-tag"
                it.name shouldBe "Fixture label"
                it.userId shouldBe "test-user"
                it.group shouldBe "fixture-group"
            }
            restored.tags?.size shouldBe 1
            restored.checklist?.map { listOf(it.id, it.text, it.completed, it.position) } shouldBe
                listOf(listOf("fixture-check", "Fixture checklist", true, 3))
            restored.reminders?.single()?.let {
                it.id shouldBe "fixture-reminder"
                it.startDate shouldBe "2026-01-01T12:00:00Z"
                it.time shouldBe "2026-01-02T12:00:00Z"
                it.type shouldBe "todo"
            }
            restored.reminders?.size shouldBe 1
            restored.isSaving shouldBe false
            restored.isCreating shouldBe false
            restored.hasErrored shouldBe false
        }

        "restore nullable fields and missing collections without inventing task state" {
            val codec = TodoTaskCodec()
            val task = Task().apply {
                id = "fixture-task"
                ownerID = "test-user"
                type = TaskType.TODO
                text = "Fixture task"
                notes = null
                attributeValue = null
                tags = null
                checklist = null
                reminders = null
            }

            val restored = codec.restore(codec.snapshot(task))

            restored.notes shouldBe null
            restored.attributeValue shouldBe null
            restored.dueDate shouldBe null
            restored.dateCreated shouldBe null
            restored.updatedAt shouldBe null
            restored.completed shouldBe false
            restored.tags?.size shouldBe 0
            restored.checklist?.size shouldBe 0
            restored.reminders?.size shouldBe 0
        }

        "retain immutable snapshot values when the live task is edited later" {
            val codec = TodoTaskCodec()
            val task = codecFixture()
            val snapshot = codec.snapshot(task)
            task.text = "Later fixture edit"
            task.completed = false
            task.checklist?.first()?.completed = false
            task.tags?.first()?.name = "Later fixture tag"

            val restored = codec.restore(snapshot)

            restored.text shouldBe "Fixture task"
            restored.completed shouldBe true
            restored.checklist?.single()?.completed shouldBe true
            restored.tags?.single()?.name shouldBe "Fixture label"
        }
    }

    "Todo request bodies" should {
        "exclude completion and score value from edits while retaining editable task content" {
            val codec = TodoTaskCodec()
            val task = codecFixture()

            val request = codec.request(task, create = false)

            request.has("completed") shouldBe false
            request.has("value") shouldBe false
            request.get("_id").asString shouldBe task.id
            request.get("text").asString shouldBe task.text
            request.get("notes").asString shouldBe task.notes
            request.get("type").asString shouldBe "todo"
            request.get("priority").asFloat shouldBe task.priority
            request.get("attribute").asString shouldBe task.attributeValue
            request.get("date").asString.isNotBlank() shouldBe true
            request.getAsJsonArray("tags").map { it.asString } shouldBe listOf("fixture-tag")
            request.getAsJsonArray("checklist").single().asJsonObject.let {
                it.get("id").asString shouldBe "fixture-check"
                it.get("text").asString shouldBe "Fixture checklist"
                it.get("completed").asBoolean shouldBe true
            }
            request.getAsJsonArray("reminders").single().asJsonObject.let {
                it.get("id").asString shouldBe "fixture-reminder"
                it.get("startDate").asString shouldBe "2026-01-01T12:00:00Z"
                it.get("time").asString shouldBe "2026-01-02T12:00:00Z"
            }
            task.completed shouldBe true
            task.value shouldBe 7.5
        }

        "create an incomplete server task so a queued score still runs the complete scoring operation" {
            val task = codecFixture()

            val request = TodoTaskCodec().request(task, create = true)

            request.get("completed").asBoolean shouldBe false
            request.has("value") shouldBe false
            request.get("_id").asString shouldBe task.id
            task.completed shouldBe true
        }

        "encode due-date removal explicitly in an edit" {
            val task = codecFixture().apply { dueDate = null }

            TodoTaskCodec().request(task, create = false).get("date").asString shouldBe ""
        }
    }
})

/** Builds a rich unmanaged fixture using only synthetic task and account data. */
private fun codecFixture(): Task = Task().apply {
    id = "fixture-task"
    ownerID = "test-user"
    type = TaskType.TODO
    text = "Fixture task"
    notes = "Fixture notes"
    priority = 1.5f
    attributeValue = "int"
    value = 7.5
    completed = true
    position = 4
    dateCreated = Date(1700000000000)
    dueDate = Date(1700100000000)
    updatedAt = Date(1700200000000)
    challengeID = "fixture-challenge"
    challengeBroken = "fixture-status"
    tags = RealmList(Tag().apply {
        id = "fixture-tag"
        name = "Fixture label"
        userId = "test-user"
        group = "fixture-group"
    })
    checklist = RealmList(ChecklistItem("fixture-check", "Fixture checklist", true).apply { position = 3 })
    reminders = RealmList(RemindersItem().apply {
        id = "fixture-reminder"
        startDate = "2026-01-01T12:00:00Z"
        time = "2026-01-02T12:00:00Z"
        type = "todo"
    })
    isSaving = true
    isCreating = true
    hasErrored = true
}
