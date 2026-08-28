package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.TaskServerState
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.OfflineTaskSyncScheduler
import com.habitrpg.android.habitica.data.sync.offlineCreateAlias
import com.habitrpg.android.habitica.models.BaseObject
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskGroupPlan
import com.habitrpg.android.habitica.models.user.Stats
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.android.habitica.utils.TaskSerializer
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifyOrder
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.realm.Realm
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import java.util.UUID

class OfflineTaskCreationTest : WordSpec({
    lateinit var repository: TaskRepository
    val localRepository = mockk<TaskLocalRepository>()
    val apiClient = mockk<ApiClient>()
    lateinit var offlineTaskSyncScheduler: OfflineTaskSyncScheduler
    var authenticatedUserID = ""

    beforeEach {
        clearTaskIDReplacements()
        authenticatedUserID = ""
        val transactionSlot = slot<((Realm) -> Unit)>()
        every { localRepository.executeTransaction(transaction = capture(transactionSlot)) } answers {
            transactionSlot.captured(mockk(relaxed = true))
        }
        val authenticationHandler = mockk<AuthenticationHandler>()
        every { authenticationHandler.currentUserID } answers { authenticatedUserID }
        offlineTaskSyncScheduler = mockk(relaxed = true)
        repository =
            TaskRepositoryImpl(
                localRepository,
                apiClient,
                authenticationHandler,
                mockk(relaxed = true),
                offlineTaskSyncScheduler
            )
        val liveObjectSlot = slot<BaseObject>()
        every { localRepository.getLiveObject(capture(liveObjectSlot)) } answers {
            liveObjectSlot.captured
        }
        every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
        every { localRepository.getTaskCopy(any()) } returns emptyFlow()
        every { localRepository.resolveTaskID(any(), any()) } answers { firstArg() }
        every { localRepository.getPendingTaskCreations(any()) } returns flowOf(emptyList())
        every { localRepository.getPendingTaskActions(any()) } returns flowOf(emptyList())
        every { localRepository.getPendingTaskDeletions(any()) } returns flowOf(emptyList())
        every { localRepository.updateTaskPositions(any()) } returns Unit
        coEvery { apiClient.getTask(any(), true) } returns null
        coEvery { apiClient.getTaskServerState(any()) } returns TaskServerState.MISSING
        coEvery { apiClient.postTaskNewPosition(any(), any(), any()) } returns emptyList()
    }
    afterEach { clearAllMocks() }

    "offline task creation" should {
        "keep personal tasks locally without displaying an error" {
            val task = Task().apply { type = TaskType.TODO }
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.createTask(task, true) } returns null

            repository.createTask(task)

            task.isCreating shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingCreate shouldBe true
            task.pendingPosition shouldBe true
            verify(exactly = 2) { localRepository.save(task) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }

        "keep unsupported creations retryable without scheduling background work" {
            val task =
                Task().apply {
                    type = TaskType.TODO
                    group = TaskGroupPlan().apply { groupID = "group-id" }
                }
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.createGroupTask("group-id", task) } returns null

            repository.createTask(task)

            task.isCreating shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe true
            verify(exactly = 2) { localRepository.save(task) }
            verify(exactly = 0) { offlineTaskSyncScheduler.enqueue() }
        }

        "preserve the owner if authentication is temporarily unavailable" {
            val task =
                Task().apply {
                    ownerID = "local-user"
                    type = TaskType.TODO
                }
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.createTask(task, true) } returns null

            repository.createTask(task)

            task.ownerID shouldBe "local-user"
        }

        "merge an action queued while foreground CREATE is in flight" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            val latestLocalTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                    completed = true
                    pendingCreate = true
                    pendingScoreUp = true
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = task.type
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.getTaskCopy("local-task-id") } returns flowOf(latestLocalTask)
            every { localRepository.replaceTask("local-task-id", serverTask) } returns Unit
            coEvery { apiClient.createTask(task, true) } returns serverTask

            repository.createTask(task) shouldBe serverTask

            serverTask.pendingCreate shouldBe false
            serverTask.pendingScoreUp shouldBe true
            serverTask.completed shouldBe true
        }

        "resolve a waiting action after CREATE replaces the local ID" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = task.type
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.replaceTask("local-task-id", serverTask) } returns Unit
            every { localRepository.getTask("server-task-id") } returns flowOf(serverTask)
            coEvery { apiClient.createTask(task, true) } returns serverTask
            coEvery {
                apiClient.postTaskNewPosition("server-task-id", 4, true)
            } returns listOf("server-task-id")

            repository.createTask(task) shouldBe serverTask
            repository.updateTaskPosition(TaskType.TODO, "local-task-id", 4) shouldBe
                listOf("server-task-id")

            coVerify(exactly = 1) {
                apiClient.postTaskNewPosition("server-task-id", 4, true)
            }
        }

        "keep pending work retryable if authentication is temporarily unavailable" {
            repository.syncPendingTaskCreations() shouldBe false

            verify(exactly = 0) { localRepository.getPendingTaskCreations(any()) }
        }

        "sync only queued personal creations" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = UUID.randomUUID().toString()
                    type = TaskType.REWARD
                    isCreating = true
                    hasErrored = true
                    pendingCreate = true
                    pendingPosition = true
                }
            every { localRepository.getPendingTaskCreations("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(task, true) } returns
                Task().apply {
                    id = task.id
                    type = task.type
                }

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.createTask(task, true) }
        }

        "not create when the server state is unknown" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    pendingCreate = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty()) } returns
                TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations() shouldBe false

            coVerify(exactly = 0) { apiClient.createTask(any(), any()) }
        }

        "preserve a queued check-off while CREATE is in flight" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    pendingScoreUp = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(task, true) } returns null

            repository.syncPendingTaskCreations() shouldBe false

            task.completed shouldBe true
            task.isCreating shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingScoreUp shouldBe true
        }

        "reconcile an already-created server task without creating a duplicate" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = UUID.randomUUID().toString()
                    ownerID = "local-user"
                    type = TaskType.DAILY
                    isCreating = true
                    hasErrored = true
                    pendingCreate = true
                    pendingPosition = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = task.type
                }
            every { localRepository.getPendingTaskCreations("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns onlineTask

            repository.syncPendingTaskCreations() shouldBe true

            onlineTask.ownerID shouldBe task.ownerID
            onlineTask.isCreating shouldBe false
            onlineTask.hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.createTask(any(), any()) }
            verify(atLeast = 1) { localRepository.save(onlineTask) }
        }

        "reconcile before the foreground retry path can create a duplicate" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = UUID.randomUUID().toString()
                    ownerID = "local-user"
                    type = TaskType.TODO
                    isCreating = true
                    hasErrored = true
                    pendingCreate = true
                    pendingPosition = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = task.type
                }
            every { localRepository.getErroredTasks("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(onlineTask) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns onlineTask

            repository.syncErroredTasks().orEmpty().single() shouldBe onlineTask

            coVerify(exactly = 0) { apiClient.createTask(any(), any()) }
            verify(atLeast = 1) { localRepository.save(onlineTask) }
        }

        "queue a check-off behind a pending task creation" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit

            repository.taskChecked(null, task, true, false, null) shouldBe null

            task.completed shouldBe true
            task.isCreating shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingScoreUp shouldBe true
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }

        "queue a reorder behind a pending task creation" {
            val task =
                Task().apply {
                    id = "local-task-id"
                    type = TaskType.TODO
                    position = 3
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                }
            every { localRepository.getTask("local-task-id") } returns flowOf(task)
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit

            repository.updateTaskPosition(TaskType.TODO, "local-task-id", 3) shouldBe emptyList()

            coVerify(exactly = 0) { apiClient.postTaskNewPosition(any(), any(), any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }

        "queue create reconciliation when a reorder cannot reach the server" {
            val task =
                Task().apply {
                    id = "legacy-task-id"
                    type = TaskType.TODO
                    position = 4
                }
            every { localRepository.getTask("legacy-task-id") } returns flowOf(task)
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit
            coEvery {
                apiClient.postTaskNewPosition("legacy-task-id", 4, true)
            } returns null

            repository.updateTaskPosition(TaskType.TODO, "legacy-task-id", 4) shouldBe emptyList()

            task.isCreating shouldBe false
            task.isSaving shouldBe false
            task.pendingPosition shouldBe true
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }

        "create before replaying a queued reorder and check-off" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 2
                    completed = true
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    pendingScoreUp = true
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            val user = User().apply { stats = Stats() }
            val createPayload = slot<Task>()
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.replaceTask("local-task-id", serverTask) } returns Unit
            every { localRepository.updateTaskPositions(listOf("server-task-id")) } returns Unit
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(capture(createPayload), true) } returns serverTask
            coEvery {
                apiClient.postTaskNewPosition("server-task-id", 2, true)
            } returns listOf("server-task-id")
            coEvery {
                apiClient.postTaskDirection("server-task-id", "up", true)
            } returns TaskDirectionData()

            repository.syncPendingTaskCreations() shouldBe true

            createPayload.captured.completed shouldBe true
            createPayload.captured.pendingScoreUp shouldBe true
            coVerifyOrder {
                apiClient.createTask(any(), true)
                apiClient.postTaskNewPosition("server-task-id", 2, true)
                apiClient.postTaskDirection("server-task-id", "up", true)
            }
        }

        "restore CREATE when a queued action targets a server-missing task" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "legacy-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 5
                    pendingPosition = true
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.replaceTask("legacy-task-id", serverTask) } returns Unit
            every { localRepository.updateTaskPositions(listOf("server-task-id")) } returns Unit
            coEvery { apiClient.getTask("legacy-task-id", true) } returns null
            coEvery { apiClient.getTaskServerState("legacy-task-id") } returns
                TaskServerState.MISSING
            coEvery { apiClient.getTask("android_legacy-task-id", true) } returns null
            coEvery { apiClient.getTaskServerState("android_legacy-task-id") } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(any(), true) } returns serverTask
            coEvery {
                apiClient.postTaskNewPosition("server-task-id", 5, true)
            } returns listOf("server-task-id")

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.createTask(any(), true) }
            coVerify(exactly = 1) {
                apiClient.postTaskNewPosition("server-task-id", 5, true)
            }
        }

        "reconcile an acknowledged check-off without scoring twice" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 1
                    completed = true
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    pendingScoreUp = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.updateTaskPositions(listOf("task-id")) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true) } returns onlineTask
            coEvery { apiClient.postTaskNewPosition("task-id", 1, true) } returns
                listOf("task-id")

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 0) { apiClient.createTask(any(), any()) }
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any()) }
        }
    }

    "offline To Do completion" should {
        "stay completed locally and schedule a silent retry" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.postTaskDirection("todo-id", "up", true) } returns null

            repository.taskChecked(null, task, true, false, null) shouldBe null

            task.completed shouldBe true
            task.hasErrored shouldBe false
            task.isSaving shouldBe false
            task.pendingScoreUp shouldBe true
            verify(exactly = 1) { localRepository.save(task) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }

        "reconcile an already-completed server task without scoring it twice" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                    pendingScoreUp = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = TaskType.TODO
                    completed = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(onlineTask) } returns Unit
            coEvery { apiClient.getTask("todo-id", true) } returns onlineTask

            repository.syncPendingTaskCreations() shouldBe true

            onlineTask.ownerID shouldBe task.ownerID
            onlineTask.hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.postTaskNewPosition(any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any()) }
            verify(atLeast = 1) { localRepository.save(onlineTask) }
        }

        "submit a persisted completion when connectivity returns" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                    pendingScoreUp = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = TaskType.TODO
                }
            val user = User().apply { stats = Stats() }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery { apiClient.getTask("todo-id", true) } returns onlineTask
            coEvery { apiClient.postTaskDirection("todo-id", "up", true) } returns
                TaskDirectionData()

            repository.syncPendingTaskCreations() shouldBe true

            onlineTask.completed shouldBe true
            onlineTask.hasErrored shouldBe false
            coVerify(exactly = 1) { apiClient.postTaskDirection("todo-id", "up", true) }
        }

        "replay reorder before check-off when both are queued" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 6
                    completed = true
                    pendingPosition = true
                    pendingScoreUp = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                }
            val user = User().apply { stats = Stats() }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            every { localRepository.updateTaskPositions(listOf("todo-id")) } returns Unit
            coEvery { apiClient.getTask("todo-id", true) } returns onlineTask
            coEvery { apiClient.postTaskNewPosition("todo-id", 6, true) } returns
                listOf("todo-id")
            coEvery { apiClient.postTaskDirection("todo-id", "up", true) } returns
                TaskDirectionData()

            repository.syncPendingTaskCreations() shouldBe true

            coVerifyOrder {
                apiClient.postTaskNewPosition("todo-id", 6, true)
                apiClient.postTaskDirection("todo-id", "up", true)
            }
        }

        "queue create reconciliation when a check-off cannot reach the server" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "legacy-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.postTaskDirection("legacy-task-id", "up", true) } returns null

            repository.taskChecked(null, task, true, false, null) shouldBe null

            task.isCreating shouldBe false
            task.completed shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingScoreUp shouldBe true
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }
    }

    "offline task serialization" should {
        "send a deterministic alias and leave queued SCORE_UP incomplete" {
            val task =
                Task().apply {
                    id = "local-task-id"
                    type = TaskType.TODO
                    completed = true
                    pendingCreate = true
                    pendingScoreUp = true
                }

            val json =
                TaskSerializer().serialize(
                    task,
                    Task::class.java,
                    mockk(relaxed = true),
                ).asJsonObject

            json.get("alias").asString shouldBe "android_local-task-id"
            json.get("completed").asBoolean shouldBe false
            task.completed shouldBe true
            task.pendingScoreUp shouldBe true
        }
    }

    "outbox state" should {
        "resolve a pre-replacement ID from its persisted alias" {
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    alias = "android_local-task-id"
                }
            every {
                localRepository.resolveTaskID("local-task-id", "android_local-task-id")
            } returns "server-task-id"
            every { localRepository.getTask("server-task-id") } returns flowOf(serverTask)

            repository.getTask("local-task-id").first() shouldBe serverTask
        }

        "survive a concurrent successful task edit" {
            val task =
                Task().apply {
                    id = "task-id"
                    type = TaskType.TODO
                }
            val latestLocalTask =
                Task().apply {
                    id = task.id
                    type = task.type
                    completed = true
                    pendingPosition = true
                    pendingScoreUp = true
                }
            val serverTask =
                Task().apply {
                    id = task.id
                    type = task.type
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.getTaskCopy("task-id") } returns flowOf(latestLocalTask)
            coEvery { apiClient.updateTask("task-id", any()) } returns serverTask

            repository.updateTask(task, force = true) shouldBe serverTask

            serverTask.pendingPosition shouldBe true
            serverTask.pendingScoreUp shouldBe true
            serverTask.completed shouldBe true
        }
    }

    "task deletion" should {
        "hide a personal task immediately and queue its deletion" {
            val task =
                Task().apply {
                    id = "local-task-id"
                    type = TaskType.TODO
                }
            every { localRepository.getTaskCopy("local-task-id") } returns flowOf(task)
            every { localRepository.save(task) } returns Unit

            repository.deleteTask("local-task-id") shouldBe true

            task.pendingDelete shouldBe true
            coVerify(exactly = 0) { apiClient.deleteTask(any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue() }
        }

        "delete an acknowledged pending CREATE by its deterministic alias" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    pendingDelete = true
                }
            val serverTask = Task().apply { id = "server-task-id" }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("local-task-id") } returns Unit
            every { localRepository.deleteTask("server-task-id") } returns Unit
            coEvery { apiClient.getTask("android_local-task-id", true) } returns serverTask
            coEvery { apiClient.deleteTask("android_local-task-id", true) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.deleteTask("android_local-task-id", true) }
            verify(exactly = 1) { localRepository.deleteTask("local-task-id") }
            verify(exactly = 1) { localRepository.deleteTask("server-task-id") }
        }

        "fall back to the original ID for a pre-alias pending CREATE" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    pendingDelete = true
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("local-task-id") } returns Unit
            coEvery { apiClient.deleteTask("android_local-task-id", true) } returns false
            coEvery { apiClient.getTaskServerState("android_local-task-id") } returns
                TaskServerState.MISSING
            coEvery { apiClient.deleteTask("local-task-id", true) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.deleteTask("local-task-id", true) }
            verify(exactly = 1) { localRepository.deleteTask("local-task-id") }
        }

        "remove a synchronized task after the server accepts deletion" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("server-task-id") } returns Unit
            coEvery { apiClient.deleteTask("server-task-id", true) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.deleteTask("server-task-id", true) }
            verify(exactly = 1) { localRepository.deleteTask("server-task-id") }
        }

        "keep a deletion queued while server state is unknown" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery { apiClient.deleteTask("server-task-id", true) } returns false
            coEvery { apiClient.getTaskServerState("server-task-id") } returns
                TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations() shouldBe false

            verify(exactly = 0) { localRepository.deleteTask(any()) }
        }

        "remove a legacy local task after the server confirms it is missing" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "legacy-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("legacy-task-id") } returns Unit
            coEvery { apiClient.deleteTask("legacy-task-id", true) } returns false
            coEvery { apiClient.getTaskServerState("legacy-task-id") } returns
                TaskServerState.MISSING

            repository.syncPendingTaskCreations() shouldBe true

            verify(exactly = 1) { localRepository.deleteTask("legacy-task-id") }
        }
    }
})
