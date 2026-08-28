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
        if (oldVersion >= newVersion || oldVersion >= OUTBOX_SCHEMA_VERSION) return
        val taskSchema = realm.schema.get("Task") ?: return
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

    override fun equals(other: Any?): Boolean = other is HabiticaRealmMigration

    override fun hashCode(): Int = javaClass.hashCode()

    companion object {
        const val OUTBOX_SCHEMA_VERSION = 2L
    }
}
