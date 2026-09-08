package com.habitrpg.android.habitica.models.tasks

class TaskList {
    var tasks: MutableMap<String, Task> = mutableMapOf()

    /** Process-local API read fence; null denotes a locally constructed, non-network list. */
    @Transient
    var readGeneration: Long? = null
}
