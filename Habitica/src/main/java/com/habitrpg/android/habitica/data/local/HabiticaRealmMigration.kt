package com.habitrpg.android.habitica.data.local

import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.realm.DynamicRealm
import io.realm.RealmMigration

class HabiticaRealmMigration : RealmMigration {
    override fun migrate(
        realm: DynamicRealm,
        oldVersion: Long,
        newVersion: Long,
    ) {
        if (oldVersion >= newVersion) return
        val taskSchema = realm.schema.get("Task") ?: return
        if (oldVersion < 2L) {
            listOf("pendingCreate", "pendingDelete", "pendingPosition", "pendingScoreUp")
                .filterNot(taskSchema::hasField)
                .forEach { fieldName ->
                    taskSchema.addField(fieldName, Boolean::class.javaPrimitiveType!!)
                }
            if (!taskSchema.hasField("alias")) {
                taskSchema.addField("alias", String::class.java)
            }

            realm.where("Task").findAll().forEach { task ->
                val pendingCreate = task.getBoolean("isCreating")
                val pendingScoreUp =
                    task.getString("typeValue") == TaskType.TODO.value &&
                        task.getBoolean("completed") &&
                        task.getBoolean("hasErrored") &&
                        task.getBoolean("isSaving")
                task.setBoolean("pendingCreate", pendingCreate)
                task.setBoolean("pendingDelete", false)
                task.setBoolean("pendingPosition", pendingCreate)
                task.setBoolean("pendingScoreUp", pendingScoreUp)
            }
        }

        if (oldVersion < 3L) {
            listOf(
                "pendingDeleteAfterScore",
                "pendingUpdate",
                "pendingScoreDown",
                "pendingScoreRefresh",
                "pendingChecklist",
            )
                .filterNot(taskSchema::hasField)
                .forEach { fieldName ->
                    taskSchema.addField(fieldName, Boolean::class.javaPrimitiveType!!)
                }
            if (!taskSchema.hasField("pendingEditFields")) {
                taskSchema.addField("pendingEditFields", String::class.java)
            }
            val checklistSchema = realm.schema.get("ChecklistItem")
            if (checklistSchema != null && !checklistSchema.hasField("pendingSync")) {
                checklistSchema.addField("pendingSync", Boolean::class.javaPrimitiveType!!)
            }
            realm.where("Task").equalTo("pendingScoreUp", true).findAll().forEach { task ->
                task.setBoolean("completed", false)
            }
        }
        if (oldVersion < OUTBOX_SCHEMA_VERSION) {
            if (!taskSchema.hasField("pendingScoreSnapshot")) {
                taskSchema.addField("pendingScoreSnapshot", String::class.java)
            }
            if (!taskSchema.hasField("outboxServerOrigin")) {
                taskSchema.addField("outboxServerOrigin", String::class.java)
            }
        }
    }

    override fun equals(other: Any?): Boolean = other is HabiticaRealmMigration

    override fun hashCode(): Int = javaClass.hashCode()

    companion object {
        const val OUTBOX_SCHEMA_VERSION = 5L
    }
}
