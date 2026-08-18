package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.OfflineTaskSyncScheduler
import com.habitrpg.android.habitica.models.BaseObject
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskGroupPlan
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.Stats
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.realm.Realm
import kotlinx.coroutines.flow.flowOf
import java.util.UUID

class OfflineTaskCreationTest : WordSpec({
    lateinit var repository: TaskRepository
    val localRepository = mockk<TaskLocalRepository>()
    val apiClient = mockk<ApiClient>()
    lateinit var offlineTaskSyncScheduler: OfflineTaskSyncScheduler
    var authenticatedUserID = ""

    beforeEach {
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
        every { localRepository.getPendingTodoCompletions(any()) } returns flowOf(emptyList())
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
                }
            every { localRepository.getPendingTaskCreations("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery { apiClient.getTasks(true) } returns null
            coEvery { apiClient.createTask(task, true) } returns
                Task().apply {
                    id = task.id
                    type = task.type
                }

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.createTask(task, true) }
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
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = task.type
                }
            val onlineTasks =
                TaskList().apply {
                    tasks[onlineTask.id.orEmpty()] = onlineTask
                }
            every { localRepository.getPendingTaskCreations("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(onlineTask) } returns Unit
            coEvery { apiClient.getTasks(true) } returns onlineTasks

            repository.syncPendingTaskCreations() shouldBe true

            onlineTask.ownerID shouldBe task.ownerID
            onlineTask.isCreating shouldBe false
            onlineTask.hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.createTask(any(), any()) }
            verify(exactly = 1) { localRepository.save(onlineTask) }
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
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = task.type
                }
            val onlineTasks =
                TaskList().apply {
                    tasks[onlineTask.id.orEmpty()] = onlineTask
                }
            every { localRepository.getErroredTasks("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(onlineTask) } returns Unit
            coEvery { apiClient.getTasks(true) } returns onlineTasks

            repository.syncErroredTasks().orEmpty().single() shouldBe onlineTask

            coVerify(exactly = 0) { apiClient.createTask(any(), any()) }
            verify(exactly = 1) { localRepository.save(onlineTask) }
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
            task.hasErrored shouldBe true
            task.isSaving shouldBe true
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
                    hasErrored = true
                    isSaving = true
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = TaskType.TODO
                    completed = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTodoCompletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(onlineTask) } returns Unit
            coEvery { apiClient.getTask("todo-id", true) } returns onlineTask

            repository.syncPendingTaskCreations() shouldBe true

            onlineTask.ownerID shouldBe task.ownerID
            onlineTask.hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any()) }
            verify(exactly = 1) { localRepository.save(onlineTask) }
        }

        "submit a persisted completion when connectivity returns" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                    hasErrored = true
                    isSaving = true
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
            every { localRepository.getPendingTodoCompletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.getTask("todo-id", true) } returns onlineTask
            coEvery { apiClient.postTaskDirection("todo-id", "up", true) } returns
                TaskDirectionData()

            repository.syncPendingTaskCreations() shouldBe true

            task.completed shouldBe true
            task.hasErrored shouldBe false
            coVerify(exactly = 1) { apiClient.postTaskDirection("todo-id", "up", true) }
        }
    }
})
