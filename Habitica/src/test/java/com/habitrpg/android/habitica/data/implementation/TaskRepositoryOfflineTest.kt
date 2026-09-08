package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.TodoOperation
import com.habitrpg.android.habitica.data.sync.TodoOperationType
import com.habitrpg.android.habitica.data.sync.TodoOutbox
import com.habitrpg.android.habitica.data.sync.TodoOutboxStore
import com.habitrpg.android.habitica.data.sync.TodoProjection
import com.habitrpg.android.habitica.data.sync.TodoRemoteApi
import com.habitrpg.android.habitica.data.sync.TodoScope
import com.habitrpg.android.habitica.data.sync.TodoTaskCodec
import com.habitrpg.android.habitica.helpers.AppConfigManager
import com.habitrpg.android.habitica.models.BaseObject
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.responses.TaskScoringResult
import com.habitrpg.shared.habitica.models.tasks.TaskType
import com.habitrpg.shared.habitica.models.tasks.TasksOrder
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.realm.Realm
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.Sort
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import java.io.IOException
import java.util.Date

/** Tests repository/outbox integration; SQLite persistence and server replay have separate suites. */
@OptIn(ExperimentalCoroutinesApi::class)
class TaskRepositoryOfflineTest : WordSpec({
    "pending personal Todos" should {
        "persist local creation before returning without calling the foreground API" {
            val fixture = OfflineRepositoryFixture()
            val created = fixture.repository.createTask(offlineTodo(), true)

            created?.id shouldBe OFFLINE_ID
            fixture.pending.single().type shouldBe TodoOperationType.CREATE
            fixture.rows.getValue(OFFLINE_ID).ownerID shouldBe OFFLINE_SCOPE.userId
            fixture.rows.getValue(OFFLINE_ID).hasErrored shouldBe false
            fixture.rows.getValue(OFFLINE_ID).isSaving shouldBe false
            fixture.rows.getValue(OFFLINE_ID).isCreating shouldBe false
            coVerify(exactly = 0) { fixture.api.createTask(any()) }
        }

        "include an offline create in the snapshot before destructive server-cache reconciliation" {
            val fixture = OfflineRepositoryFixture()
            fixture.repository.createTask(offlineTodo(), true)
            val emptyServer = TaskList()
            coEvery { fixture.api.getTasks() } returns emptyServer

            fixture.repository.retrieveTasks(OFFLINE_SCOPE.userId, TasksOrder())

            fixture.lastSavedSnapshot.keys shouldBe setOf(OFFLINE_ID)
            fixture.rows.getValue(OFFLINE_ID).text shouldBe "offline fixture"
            fixture.pending.single().type shouldBe TodoOperationType.CREATE
        }

        "restore the pending task after replacing the repository and losing its Realm projection" {
            val fixture = OfflineRepositoryFixture()
            fixture.repository.createTask(offlineTodo(), true)
            fixture.rows.clear()
            fixture.repository = fixture.newRepository()

            fixture.repository.refreshLocalData()

            fixture.rows.getValue(OFFLINE_ID).ownerID shouldBe OFFLINE_SCOPE.userId
            fixture.pending.size shouldBe 1
            verify { fixture.realm.refresh() }
        }

        "preserve pending Todos during a dated Daily refresh as well as a full refresh" {
            val fixture = OfflineRepositoryFixture()
            fixture.repository.createTask(offlineTodo(), true)
            val daily = offlineTodo("dated-daily").apply { type = TaskType.DAILY; isDue = true }
            coEvery { fixture.api.getTasks("dailys", any()) } returns taskList(daily)

            fixture.repository.retrieveTasks(OFFLINE_SCOPE.userId, TasksOrder(), Date(0))

            fixture.lastSavedSnapshot.containsKey(OFFLINE_ID) shouldBe true
            fixture.rows.getValue(OFFLINE_ID).text shouldBe "offline fixture"
            fixture.rows.getValue("dated-daily").isDue shouldBe true
            fixture.pending.single().type shouldBe TodoOperationType.CREATE
        }

        "reject pre-acknowledgement responses after a create has left the durable queue" {
            val fixture = OfflineRepositoryFixture()
            fixture.repository.createTask(offlineTodo(), true)
            val beforeAcknowledgement = fixture.api.taskReadGeneration
            val staleMissing = TaskList().apply { readGeneration = beforeAcknowledgement }
            val staleOldValue = taskList(offlineTodo().apply { text = "stale fixture" }).apply {
                readGeneration = beforeAcknowledgement
            }
            coEvery { fixture.remote.create(any()) } returns offlineTodo().apply { text = "acknowledged fixture" }

            fixture.repository.syncPendingTodos() shouldBe true
            fixture.pending.isEmpty() shouldBe true
            (fixture.api.taskReadGeneration > beforeAcknowledgement) shouldBe true
            fixture.rows.getValue(OFFLINE_ID).text shouldBe "acknowledged fixture"

            fixture.repository.saveTasks(OFFLINE_SCOPE.userId, TasksOrder(), staleMissing)
            fixture.repository.saveTasks(OFFLINE_SCOPE.userId, TasksOrder(), staleOldValue)

            fixture.rows.getValue(OFFLINE_ID).text shouldBe "acknowledged fixture"
            fixture.pending.isEmpty() shouldBe true
        }

        "project completion without changing user rewards or invoking scoring callbacks" {
            val fixture = OfflineRepositoryFixture()
            val task = offlineTodo()
            fixture.rows[OFFLINE_ID] = task
            val user = mockk<User>()
            val callback = mockk<(TaskScoringResult) -> Unit>(relaxed = true)

            fixture.repository.taskChecked(user, task, true, true, callback) shouldBe null

            fixture.rows.getValue(OFFLINE_ID).completed shouldBe true
            fixture.rows.getValue(OFFLINE_ID).value shouldBe task.value
            fixture.pending.single().type shouldBe TodoOperationType.SCORE
            verify { user wasNot Called }
            verify(exactly = 0) { callback.invoke(any()) }
            verify(exactly = 0) { fixture.config.enableLocalTaskScoring() }
            coVerify(exactly = 0) { fixture.api.postTaskDirection(any(), any()) }
        }

        "reuse a score receipt when authoritative user refresh fails without replaying rewards" {
            val fixture = OfflineRepositoryFixture()
            val task = offlineTodo()
            fixture.rows[OFFLINE_ID] = task
            val callback = mockk<(TaskScoringResult) -> Unit>(relaxed = true)
            fixture.repository.taskChecked(null, task, true, true, callback)
            coEvery { fixture.remote.find(OFFLINE_ID) } returns offlineTodo()
            coEvery { fixture.remote.score(OFFLINE_ID, true) } returns TaskDirectionData().apply { delta = 1.0f }
            val serverUser = User().apply { id = OFFLINE_SCOPE.userId }
            coEvery { fixture.remote.user() } throws IOException("Fictional interrupted refresh") andThen serverUser

            fixture.repository.syncPendingTodos() shouldBe false
            fixture.pending.single().response.isNullOrBlank() shouldBe false
            fixture.savedUsers.isEmpty() shouldBe true
            fixture.repository.syncPendingTodos() shouldBe true

            fixture.pending.isEmpty() shouldBe true
            fixture.rows.getValue(OFFLINE_ID).completed shouldBe true
            fixture.rows.getValue(OFFLINE_ID).value shouldBe 5.0
            fixture.savedUsers shouldBe listOf(serverUser)
            coVerify(exactly = 1) { fixture.remote.score(OFFLINE_ID, true) }
            coVerify(exactly = 2) { fixture.remote.user() }
            coVerify(exactly = 0) { fixture.api.postTaskDirection(any(), any()) }
            coVerify(exactly = 0) { fixture.api.retrieveUser(any()) }
            verify(exactly = 0) { callback.invoke(any()) }
        }

        "keep pending completion out of the active query after an incomplete server snapshot" {
            val fixture = OfflineRepositoryFixture()
            val task = offlineTodo()
            fixture.rows[OFFLINE_ID] = task
            fixture.repository.taskChecked(null, task, true, true, null)

            fixture.repository.saveTasks(OFFLINE_SCOPE.userId, TasksOrder(), taskList(offlineTodo()))
            val result = fixture.repository.getTasks(TaskType.TODO, OFFLINE_SCOPE.userId, emptyArray()).first()

            (result === fixture.lastQueryResult) shouldBe true
            fixture.lastSavedSnapshot.getValue(OFFLINE_ID).completed shouldBe true
            (result as RealmResults<Task>).where().equalTo("completed", false).findAll().isEmpty() shouldBe true
            result.where().equalTo("completed", true).findAll().size shouldBe 1
        }

        "exclude a locally uncompleted task from a stale completed-Todo response" {
            val fixture = OfflineRepositoryFixture()
            val task = offlineTodo().apply { completed = true }
            fixture.rows[OFFLINE_ID] = task
            fixture.repository.taskChecked(null, task, false, true, null)
            coEvery { fixture.api.getTasks("completedTodos") } returns taskList(offlineTodo().apply { completed = true })

            val result = fixture.repository.retrieveCompletedTodos(OFFLINE_SCOPE.userId)

            result?.tasks?.isEmpty() shouldBe true
            fixture.rows.getValue(OFFLINE_ID).completed shouldBe false
        }

        "keep a pending deletion removed when the server still includes its task" {
            val fixture = OfflineRepositoryFixture()
            fixture.rows[OFFLINE_ID] = offlineTodo()
            fixture.repository.deleteTask(OFFLINE_ID)

            fixture.repository.saveTasks(OFFLINE_SCOPE.userId, TasksOrder(), taskList(offlineTodo()))

            fixture.rows.containsKey(OFFLINE_ID) shouldBe false
            fixture.lastSavedSnapshot.containsKey(OFFLINE_ID) shouldBe false
            fixture.pending.single().type shouldBe TodoOperationType.DELETE
            coVerify(exactly = 0) { fixture.api.deleteTask(any()) }
        }

        "preserve the original RealmResults object for every task type while a Todo is pending" {
            val fixture = OfflineRepositoryFixture()
            fixture.repository.createTask(offlineTodo(), true)
            for (type in listOf(TaskType.TODO, TaskType.DAILY, TaskType.HABIT, TaskType.REWARD)) {
                val result = fixture.repository.getTasks(type, OFFLINE_SCOPE.userId, emptyArray()).first()
                (result === fixture.lastQueryResult) shouldBe true
                (result is RealmResults<*>) shouldBe true
            }
        }

        "leave due and gray Daily records unchanged while materializing pending Todos" {
            val fixture = OfflineRepositoryFixture()
            val due = offlineTodo("daily-due").apply { type = TaskType.DAILY; isDue = true }
            val gray = offlineTodo("daily-gray").apply { type = TaskType.DAILY; isDue = false }
            fixture.rows[requireNotNull(due.id)] = due
            fixture.rows[requireNotNull(gray.id)] = gray
            fixture.repository.createTask(offlineTodo(), true)

            val result = fixture.repository.getTasks(TaskType.DAILY, OFFLINE_SCOPE.userId, emptyArray()).first()

            (result === fixture.lastQueryResult) shouldBe true
            (result as RealmResults<Task>).where().equalTo("isDue", true).findAll().single().id shouldBe due.id
            result.where().equalTo("isDue", false).findAll().single().id shouldBe gray.id
            (fixture.rows[due.id] === due) shouldBe true
            (fixture.rows[gray.id] === gray) shouldBe true
        }

        "keep the full moved order after refreshing an old server order" {
            val fixture = OfflineRepositoryFixture()
            listOf(OFFLINE_ID, "second", "third").forEachIndexed { index, id ->
                fixture.rows[id] = offlineTodo(id).apply { position = index }
            }
            fixture.repository.updateTaskPosition(TaskType.TODO, OFFLINE_ID, 2)
            fixture.rows.values.sortedBy { it.position }.map { it.id } shouldBe listOf("second", "third", OFFLINE_ID)
            val stale = taskList(
                offlineTodo().apply { position = 0 },
                offlineTodo("second").apply { position = 1 },
                offlineTodo("third").apply { position = 2 },
            )

            fixture.repository.saveTasks(OFFLINE_SCOPE.userId, TasksOrder(), stale)

            fixture.rows.values.sortedBy { it.position }.map { it.id } shouldBe listOf("second", "third", OFFLINE_ID)
            fixture.pending.single().type shouldBe TodoOperationType.MOVE
            coVerify(exactly = 0) { fixture.api.postTaskNewPosition(any(), any()) }
        }

        "exclude cached completed Todos from active drag positions" {
            val fixture = OfflineRepositoryFixture()
            fixture.rows["completed"] = offlineTodo("completed").apply { completed = true; position = 0 }
            fixture.rows[OFFLINE_ID] = offlineTodo().apply { position = 0 }
            fixture.rows["second"] = offlineTodo("second").apply { position = 1 }

            fixture.repository.updateTaskPosition(TaskType.TODO, OFFLINE_ID, 1)

            fixture.rows.values.filterNot { it.completed }.sortedBy { it.position }.map { it.id } shouldBe
                listOf("second", OFFLINE_ID)
            fixture.rows.getValue("completed").completed shouldBe true
            fixture.rows.getValue("completed").position shouldBe 0
            fixture.pending.single().type shouldBe TodoOperationType.MOVE
        }

        "ignore attempts to reorder completed Todos without changing cached rows or queue" {
            val fixture = OfflineRepositoryFixture()
            val completed = offlineTodo().apply { completed = true; position = 0 }
            fixture.rows[OFFLINE_ID] = completed
            fixture.rows["active"] = offlineTodo("active").apply { position = 0 }

            fixture.repository.updateTaskPosition(TaskType.TODO, OFFLINE_ID, 1) shouldBe null

            (fixture.rows[OFFLINE_ID] === completed) shouldBe true
            fixture.rows.getValue(OFFLINE_ID).position shouldBe 0
            fixture.rows.getValue("active").position shouldBe 0
            fixture.pending.isEmpty() shouldBe true
            coVerify(exactly = 0) { fixture.api.postTaskNewPosition(any(), any()) }
        }
    }
})

private val OFFLINE_SCOPE = TodoScope("https://example.com", "fixture-user")
private const val OFFLINE_ID = "11111111-1111-4111-8111-111111111111"

/** Builds an unmanaged fictional Todo with a stable identity. */
private fun offlineTodo(id: String = OFFLINE_ID): Task = Task().apply {
    this.id = id
    ownerID = OFFLINE_SCOPE.userId
    type = TaskType.TODO
    text = "offline fixture"
    value = 4.0
}

/** Builds a server-response fixture without any device/account data. */
private fun taskList(vararg tasks: Task): TaskList = TaskList().apply {
    tasks.forEach { this.tasks[requireNotNull(it.id)] = it }
}

/** Models the Realm boundary while exercising the real repository and outbox coordinator. */
@OptIn(ExperimentalCoroutinesApi::class)
private class OfflineRepositoryFixture {
    val rows = linkedMapOf<String, Task>()
    val savedUsers = mutableListOf<User>()
    val pending = mutableListOf<TodoOperation>()
    private val projections = linkedMapOf<String, TodoProjection>()
    private val codec = TodoTaskCodec()
    private val local = mockk<TaskLocalRepository>(relaxed = true)
    val realm = mockk<Realm>(relaxed = true)
    val api = mockk<ApiClient>(relaxed = true)
    val remote = mockk<TodoRemoteApi>()
    val config = mockk<AppConfigManager>(relaxed = true)
    private val authentication = mockk<AuthenticationHandler>()
    private val store = mockk<TodoOutboxStore>()
    private val outbox: TodoOutbox
    private var readGeneration = 0L
    var lastSavedSnapshot = emptyMap<String, Task>()
    lateinit var lastQueryResult: RealmResults<Task>
    var repository: TaskRepositoryImpl

    init {
        every { api.taskReadGeneration } answers { readGeneration }
        every { api.invalidateTaskReads() } answers { readGeneration++; Unit }
        every { api.todoRemoteApi } returns remote
        every { remote.forAccount(OFFLINE_SCOPE.server, OFFLINE_SCOPE.userId) } returns remote
        every { authentication.currentUserID } returns OFFLINE_SCOPE.userId
        every { local.realm } returns realm
        every { local.isClosed } returns false
        every { realm.isClosed } returns false
        every { local.executeTransaction(any()) } answers { firstArg<(Realm) -> Unit>().invoke(realm) }
        every { local.getUnmanagedCopy(any<Task>()) } answers { codec.restore(codec.snapshot(firstArg())) }
        every { realm.insertOrUpdate(any<Task>()) } answers {
            val task = firstArg<Task>()
            rows[requireNotNull(task.id)] = task
        }
        every { local.save(any<BaseObject>()) } answers {
            when (val saved = firstArg<BaseObject>()) {
                is Task -> rows[requireNotNull(saved.id)] = saved
                is User -> savedUsers.add(saved)
                else -> error("Unexpected saved fixture type")
            }
            Unit
        }
        every { realm.where(Task::class.java) } answers { query() }
        every { local.getTask(any()) } answers { rows[firstArg<String>()]?.let { flowOf(it) } ?: emptyFlow() }
        every { local.getTaskCopy(any()) } answers { rows[firstArg<String>()]?.let { flowOf(it) } ?: emptyFlow() }
        every { local.getTasks(any<String>()) } answers { flowOf(rows.values.toList()) }
        every { local.getTasks(any<TaskType>(), any(), any()) } answers {
            lastQueryResult = query().equalTo("typeValue", firstArg<TaskType>().value)
                .equalTo("ownerID", secondArg<String>()).findAll()
            flowOf(lastQueryResult)
        }
        every { local.saveTasks(any(), any(), any()) } answers {
            val owner = firstArg<String>()
            val tasks = thirdArg<TaskList>().tasks
            lastSavedSnapshot = tasks.toMap()
            rows.entries.removeAll { it.value.ownerID == owner }
            rows.putAll(tasks)
        }
        every { local.saveCompletedTodos(any(), any()) } answers {
            val owner = firstArg<String>()
            rows.entries.removeAll { it.value.ownerID == owner && it.value.completed }
            secondArg<MutableCollection<Task>>().forEach { rows[requireNotNull(it.id)] = it }
        }
        every { store.resolve(any(), any()) } answers { secondArg() }
        every { store.projections(any()) } answers { projections.values.toList() }
        every { store.operations(any()) } answers { pending.toList() }
        every { store.hasPending(any()) } answers { pending.isNotEmpty() }
        every { store.enqueue(any(), any(), any(), any(), any(), any()) } answers {
            val id = secondArg<String>()
            val operation = TodoOperation(pending.size.toLong() + 1, firstArg(), id, thirdArg(), arg(3))
            pending.add(operation)
            projections[id] = TodoProjection(id, arg(4), arg(5))
            operation
        }
        every { store.markAttempted(any()) } answers {
            val index = pending.indexOfFirst { it.sequence == firstArg<Long>() }
            if (index >= 0) pending[index] = pending[index].copy(attempted = true)
        }
        every { store.recordResponse(any(), any(), any()) } answers {
            val index = pending.indexOfFirst { it.sequence == firstArg<Long>() }
            if (index >= 0) pending[index] = pending[index].copy(response = secondArg())
        }
        every { store.acknowledge(any()) } answers {
            val operation = pending.first { it.sequence == firstArg<Long>() }
            pending.remove(operation)
            if (pending.none { it.taskId == operation.taskId }) projections.remove(operation.taskId)
            Unit
        }
        outbox = TodoOutbox(
            store,
            { OFFLINE_SCOPE },
            { remote },
            {},
            invalidateReads = api::invalidateTaskReads,
        )
        repository = newRepository()
    }

    /** Recreates repository state while retaining the durable-journal fixture. */
    fun newRepository(): TaskRepositoryImpl = TaskRepositoryImpl(local, api, authentication, config, outbox)

    /** Implements only the selection operations used at this repository's mocked Realm boundary. */
    private fun query(initial: Map<String, Any?> = emptyMap()): RealmQuery<Task> {
        val conditions = initial.toMutableMap()
        val query = mockk<RealmQuery<Task>>()
        every { query.equalTo(any<String>(), any<String>()) } answers {
            conditions[firstArg()] = secondArg<String>()
            query
        }
        every { query.equalTo(any<String>(), any<Boolean>()) } answers {
            conditions[firstArg()] = secondArg<Boolean>()
            query
        }
        every { query.sort(any<String>(), any<Sort>(), any<String>(), any<Sort>()) } returns query
        every { query.findAll() } answers { results(conditions.toMap()) }
        every { query.findFirst() } answers { results(conditions.toMap()).firstOrNull() }
        return query
    }

    /** Supplies queryable identity and projection mutation, not a replacement production filter. */
    private fun results(conditions: Map<String, Any?>): RealmResults<Task> {
        val result = mockk<RealmResults<Task>>()
        /** Selects fictional rows through the limited predicates exercised by these tests. */
        fun selected(): List<Task> = rows.values.filter { task ->
            conditions.all { (field, value) ->
                when (field) {
                    "id" -> task.id == value
                    "ownerID" -> task.ownerID == value
                    "typeValue" -> task.type?.value == value
                    "completed" -> task.completed == value
                    "isDue" -> task.isDue == value
                    else -> error("Unsupported fixture predicate: $field")
                }
            }
        }.sortedBy { it.position }
        every { result.where() } answers { query(conditions) }
        every { result.size } answers { selected().size }
        every { result.isEmpty() } answers { selected().isEmpty() }
        every { result.iterator() } answers { selected().toMutableList().iterator() }
        every { result[any<Int>()] } answers { selected()[firstArg()] }
        every { result.deleteAllFromRealm() } answers {
            selected().forEach { rows.remove(it.id) }
            true
        }
        return result
    }
}
