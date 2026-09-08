package com.habitrpg.android.habitica.data.sync

import com.google.gson.Gson
import com.google.gson.JsonObject
import com.habitrpg.android.habitica.api.GSonFactoryCreator
import com.habitrpg.android.habitica.models.Tag
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.RemindersItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.realm.RealmList
import java.util.Date

/** Keeps durable local presentation separate from the immutable request body sent to Habitica. */
class TodoTaskCodec {
    private val gson = Gson()
    private val wireGson = GSonFactoryCreator.createGson()

    /** Captures an unmanaged personal Todo, including fields absent from API edit bodies. */
    fun snapshot(task: Task): String = gson.toJson(
        Snapshot(
            id = requireNotNull(task.id), owner = task.ownerID, text = task.text,
            notes = task.notes, priority = task.priority, attribute = task.attributeValue,
            value = task.value, completed = task.completed, position = task.position,
            created = task.dateCreated?.time, due = task.dueDate?.time,
            updated = task.updatedAt?.time, challenge = task.challengeID,
            challengeBroken = task.challengeBroken,
            tags = task.tags.orEmpty().map { TagValue(it.id, it.name, it.userId, it.group) },
            checklist = task.checklist.orEmpty().map {
                CheckValue(requireNotNull(it.id), it.text, it.completed, it.position)
            },
            reminders = task.reminders.orEmpty().map {
                ReminderValue(it.id, it.startDate, it.time, it.type)
            }
        )
    )

    /** Restores only plain task data; Realm owns the eventual queryable collection. */
    fun restore(snapshot: String): Task {
        val value = gson.fromJson(snapshot, Snapshot::class.java)
        return Task().apply {
            id = value.id
            ownerID = value.owner
            type = TaskType.TODO
            text = value.text
            notes = value.notes
            priority = value.priority
            attributeValue = value.attribute
            this.value = value.value
            completed = value.completed
            position = value.position
            dateCreated = value.created?.let(::Date)
            dueDate = value.due?.let(::Date)
            updatedAt = value.updated?.let(::Date)
            challengeID = value.challenge
            challengeBroken = value.challengeBroken
            tags = RealmList<Tag>().apply {
                value.tags.forEach { tag ->
                    add(Tag().apply { id = tag.id; name = tag.name; userId = tag.owner; group = tag.group })
                }
            }
            checklist = RealmList<ChecklistItem>().apply {
                value.checklist.forEach { item ->
                    add(ChecklistItem(item.id, item.text, item.completed).apply { position = item.position })
                }
            }
            reminders = RealmList<RemindersItem>().apply {
                value.reminders.forEach { item ->
                    add(RemindersItem().apply {
                        id = item.id; startDate = item.start; time = item.time; type = item.type
                    })
                }
            }
            isSaving = false
            isCreating = false
            hasErrored = false
        }
    }

    /** Builds the same task API body as upstream without letting an edit bypass scoring. */
    fun request(task: Task, create: Boolean): JsonObject =
        wireGson.toJsonTree(task, Task::class.java).asJsonObject.apply {
            remove("completed")
            remove("value")
            if (create) addProperty("completed", false)
        }

    private data class Snapshot(
        val id: String, val owner: String, val text: String, val notes: String?,
        val priority: Float, val attribute: String?, val value: Double,
        val completed: Boolean, val position: Int, val created: Long?, val due: Long?,
        val updated: Long?, val challenge: String?, val challengeBroken: String?,
        val tags: List<TagValue>, val checklist: List<CheckValue>, val reminders: List<ReminderValue>
    )

    private data class TagValue(val id: String, val name: String, val owner: String?, val group: String?)
    private data class CheckValue(val id: String, val text: String?, val completed: Boolean, val position: Int)
    private data class ReminderValue(val id: String?, val start: String?, val time: String?, val type: String?)
}
