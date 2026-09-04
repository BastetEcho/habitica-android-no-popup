package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.OfflineTaskSyncScheduler
import com.habitrpg.android.habitica.models.BaseObject
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskGroupPlan
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.Stats
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.common.habitica.api.HostConfig
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.tasks.TaskType
import com.habitrpg.shared.habitica.models.tasks.TasksOrder
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.common.ExperimentalKotest
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.realm.Realm
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalKotest::class)
class TaskRepositoryImplTest : WordSpec({
    lateinit var repository: TaskRepository
    val localRepository = mockk<TaskLocalRepository>()
    val apiClient = mockk<ApiClient>()
    lateinit var offlineTaskSyncScheduler: OfflineTaskSyncScheduler
    var authenticatedUserID = ""
    beforeEach {
        val slot = slot<((Realm) -> Unit)>()
        every { localRepository.executeTransaction(transaction = capture(slot)) } answers {
            slot.captured(mockk(relaxed = true))
        }
        val authenticationHandler = mockk<AuthenticationHandler>()
        authenticatedUserID = ""
        every { authenticationHandler.currentUserID } answers { authenticatedUserID }
        every { apiClient.hostConfig } returns
            HostConfig("https://example.com", "", "fake-api-key", "test-user")
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
        every { localRepository.resolveTaskID(any(), any()) } answers { firstArg() }
        every { localRepository.resolveTaskID(any(), any(), any()) } answers { firstArg() }
        every { localRepository.getTaskCopy(any()) } returns emptyFlow()
        every { localRepository.getTaskCopy(any(), any()) } returns emptyFlow()
        every { localRepository.getPendingTaskScoreRefreshes(any()) } returns flowOf(emptyList())
        every { localRepository.getTask(any(), any()) } returns emptyFlow()
        every { localRepository.deleteTask(any(), any()) } returns Unit
        every { localRepository.replaceTask(any(), any(), any()) } returns Unit
        every { localRepository.save(any<Task>()) } returns Unit
    }
    "retrieveTasks" should {
        "save tasks locally" {
            val list = TaskList()
            coEvery { apiClient.getTasks() } returns list
            every { localRepository.saveTasks("", any(), any()) } returns Unit
            val order = TasksOrder()
            repository.retrieveTasks("", order)
            verify { localRepository.saveTasks("", order, list) }
        }
    }
    "group-owned task routing" should {
        lateinit var groupTask: Task
        beforeEach {
            authenticatedUserID = "member-user"
            groupTask =
                Task().apply {
                    id = "group-task-id"
                    ownerID = "group-id"
                    type = TaskType.TODO
                    group = TaskGroupPlan().apply { groupID = "group-id" }
                }
        }

        "look up a group task without filtering by the signed-in user" {
            every { localRepository.getTask("group-task-id") } returns flowOf(groupTask)

            repository.getTask("group-task-id").first() shouldBe groupTask

            verify(exactly = 1) { localRepository.getTask("group-task-id") }
            verify(exactly = 0) {
                localRepository.getTask("group-task-id", authenticatedUserID)
            }
        }

        "score a group checklist item through the existing online path" {
            val item = ChecklistItem("item-id", "item", true)
            val response =
                Task().apply {
                    id = groupTask.id
                    ownerID = groupTask.ownerID
                    type = groupTask.type
                    group = groupTask.group
                    checklist?.add(item)
                }
            every { localRepository.getTaskCopy("group-task-id") } returns flowOf(groupTask)
            every { localRepository.save(item) } returns Unit
            coEvery {
                apiClient.scoreChecklistItem("group-task-id", "item-id", false, null, null)
            } returns response

            repository.scoreChecklistItem("group-task-id", "item-id") shouldBe response

            coVerify(exactly = 1) {
                apiClient.scoreChecklistItem("group-task-id", "item-id", false, null, null)
            }
            verify(exactly = 1) { localRepository.save(item) }
        }

        "reorder a group task through the group endpoint" {
            val positions = listOf("other-task-id", "group-task-id")
            every { localRepository.getTask("group-task-id") } returns flowOf(groupTask)
            every { localRepository.updateTaskPositions(positions) } returns Unit
            coEvery {
                apiClient.postGroupTaskNewPosition("group-task-id", 1)
            } returns positions

            repository.updateTaskPosition(TaskType.TODO, "group-task-id", 1) shouldBe positions

            coVerify(exactly = 1) {
                apiClient.postGroupTaskNewPosition("group-task-id", 1)
            }
            coVerify(exactly = 0) {
                apiClient.postTaskNewPosition(any(), any(), any(), any(), any())
            }
            verify(exactly = 1) { localRepository.updateTaskPositions(positions) }
            verify(exactly = 0) {
                localRepository.updateTaskPositions(positions, groupTask.ownerID)
            }
        }
    }
    "taskChecked" should {
        val task = Task()
        task.id = UUID.randomUUID().toString()
        lateinit var user: User
        beforeEach {
            user = spyk(User())
            user.stats = Stats()
        }
        "debounce" {
            coEvery {
                apiClient.postTaskDirection(any(), "up", false, null, null)
            } returns TaskDirectionData()
            repository.taskChecked(user, task, true, false, null)
            repository.taskChecked(user, task, true, false, null)
            coVerify(exactly = 1) {
                apiClient.postTaskDirection(any(), any(), false, null, null)
            }
        }
        "get user if not passed" {
            coEvery {
                apiClient.postTaskDirection(any(), "up", false, null, null)
            } returns TaskDirectionData()
            coEvery { localRepository.getUser("") } returns flowOf(user)
            repository.taskChecked(null, task, true, false, null)
            eventually(5000.milliseconds) {
                localRepository.getUser("")
            }
        }
        "builds task result correctly" {
            val data = TaskDirectionData()
            data.lvl = 10
            data.hp = 20.0
            data.mp = 30.0
            data.gp = 40.0
            user.stats?.lvl = 10
            user.stats?.hp = 8.0
            user.stats?.mp = 4.0
            coEvery { apiClient.postTaskDirection(any(), "up", false, null, null) } returns data
            val result = repository.taskChecked(user, task, true, true, null)
            result?.level shouldBe 10
            result?.healthDelta shouldBe 12.0
            result?.manaDelta shouldBe 26.0
            result?.hasLeveledUp shouldBe false
        }
        "set hasLeveledUp correctly" {
            val data = TaskDirectionData()
            data.lvl = 11
            user.stats?.lvl = 10
            coEvery { apiClient.postTaskDirection(any(), "up", false, null, null) } returns data
            val result = repository.taskChecked(user, task, true, true, null)
            result?.level shouldBe 11
            result?.hasLeveledUp shouldBe true
        }
        "handle stats not being there" {
            val data = TaskDirectionData()
            data.lvl = 1
            user.stats = null
            coEvery { apiClient.postTaskDirection(any(), "up", false, null, null) } returns data
            repository.taskChecked(user, task, true, true, null)
        }
        "update daily streak" {
            val data = TaskDirectionData()
            data.delta = 1.0f
            data.lvl = 1
            task.type = TaskType.DAILY
            task.value = 0.0
            coEvery { apiClient.postTaskDirection(any(), "up", false, null, null) } returns data
            repository.taskChecked(user, task, true, true, null)
            task.streak shouldBe 1
            task.completed shouldBe true
        }
        "update habit counter" {
            val data = TaskDirectionData()
            data.delta = 1.0f
            data.lvl = 1
            task.type = TaskType.HABIT
            task.value = 0.0
            coEvery { apiClient.postTaskDirection(any(), "up", false, null, null) } returns data
            repository.taskChecked(user, task, true, true, null)
            task.counterUp shouldBe 1

            data.delta = -10.0f
            coEvery { apiClient.postTaskDirection(any(), "down", false, null, null) } returns data
            repository.taskChecked(user, task, false, true, null)
            task.counterUp shouldBe 1
            task.counterDown shouldBe 1
        }
    }
    afterEach { clearAllMocks() }
})
