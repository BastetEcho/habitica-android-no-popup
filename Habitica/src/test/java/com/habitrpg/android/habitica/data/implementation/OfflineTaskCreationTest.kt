package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.TaskServerState
import com.habitrpg.android.habitica.data.local.TaskLocalRepository
import com.habitrpg.android.habitica.data.sync.OfflineTaskSyncScheduler
import com.habitrpg.android.habitica.data.sync.offlineCreateAlias
import com.habitrpg.android.habitica.models.BaseObject
import com.habitrpg.android.habitica.models.tasks.ChecklistItem
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.android.habitica.models.tasks.TaskGroupPlan
import com.habitrpg.android.habitica.models.user.Stats
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.android.habitica.utils.TaskSerializer
import com.habitrpg.common.habitica.api.HostConfig
import com.habitrpg.shared.habitica.models.responses.TaskDirectionData
import com.habitrpg.shared.habitica.models.tasks.TaskType
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.clearMocks
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
import java.util.Date
import java.util.UUID

class OfflineTaskCreationTest : WordSpec({
    lateinit var repository: TaskRepository
    val localRepository = mockk<TaskLocalRepository>()
    val apiClient = mockk<ApiClient>()
    val hostConfig = HostConfig("https://example.com/api/v4/", "", "fake-api-key", "test-user")
    val expectedServerOrigin = hostConfig.serverOrigin()
    lateinit var offlineTaskSyncScheduler: OfflineTaskSyncScheduler
    var authenticatedUserID = "test-user"

    beforeEach {
        clearTaskIDReplacements()
        authenticatedUserID = "test-user"
        hostConfig.updateServerAddress("https://example.com/api/v4/")
        hostConfig.updateAuthentication("test-user", "fake-api-key")
        val transactionSlot = slot<((Realm) -> Unit)>()
        every { localRepository.executeTransaction(transaction = capture(transactionSlot)) } answers {
            transactionSlot.captured(mockk(relaxed = true))
        }
        val authenticationHandler = mockk<AuthenticationHandler>()
        every { authenticationHandler.currentUserID } answers { authenticatedUserID }
        every { apiClient.hostConfig } returns hostConfig
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
        every { localRepository.save(any<Task>()) } returns Unit
        every { localRepository.getTaskCopy(any()) } returns emptyFlow()
        every { localRepository.getTaskCopy(any(), any()) } returns emptyFlow()
        every { localRepository.getTask(any(), any()) } returns emptyFlow()
        every { localRepository.deleteTask(any(), any()) } returns Unit
        every { localRepository.replaceTask(any(), any(), any()) } returns Unit
        every { localRepository.resolveTaskID(any(), any()) } answers { firstArg() }
        every { localRepository.resolveTaskID(any(), any(), any()) } answers { firstArg() }
        every { localRepository.getPendingTaskCreations(any()) } returns flowOf(emptyList())
        every { localRepository.getPendingTaskActions(any()) } returns flowOf(emptyList())
        every { localRepository.getPendingTaskScoreRefreshes(any()) } returns flowOf(emptyList())
        every { localRepository.getPendingTaskDeletions(any()) } returns flowOf(emptyList())
        every { localRepository.updateTaskPositions(any()) } returns Unit
        every { localRepository.updateTaskPositions(any(), any()) } returns Unit
        every {
            localRepository.moveTaskToPosition(
                any(),
                "test-user",
                expectedServerOrigin,
                any(),
                any(),
            )
        } returns Unit
        coEvery {
            apiClient.getTask(any(), true, "test-user", expectedServerOrigin)
        } returns null
        coEvery {
            apiClient.getTaskServerState(any(), "test-user", expectedServerOrigin)
        } returns TaskServerState.MISSING
        coEvery {
            apiClient.postTaskNewPosition(
                any(),
                any(),
                true,
                "test-user",
                expectedServerOrigin,
            )
        } returns emptyList()
    }
    afterEach { clearAllMocks() }

    "offline task creation" should {
        "keep personal tasks locally without displaying an error" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.createTask(task, true, any(), any()) } returns null

            repository.createTask(task)

            task.isCreating shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingCreate shouldBe true
            task.pendingPosition shouldBe true
            verify(exactly = 2) { localRepository.save(task) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "stabilize checklist identity before saving and queue a tap by that ID" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(
                        ChecklistItem(null, "first", false).apply {
                            id = null
                        }
                    )
                    checklist?.add(
                        ChecklistItem(null, "second", false).apply {
                            id = ""
                        }
                    )
                }
            var firstSavedIDs: List<String?>? = null
            var firstSavedPositions: List<Int>? = null
            every { localRepository.save(any<Task>()) } answers {
                val savedTask = firstArg<Task>()
                if (firstSavedIDs == null) {
                    firstSavedIDs = savedTask.checklist.orEmpty().map { it.id }
                    firstSavedPositions = savedTask.checklist.orEmpty().map { it.position }
                }
            }
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } returns flowOf(task)
            every { localRepository.getTaskCopy(any()) } returns flowOf(task)
            coEvery {
                apiClient.createTask(any(), true, authenticatedUserID, any())
            } returns null

            repository.createTask(task)

            firstSavedIDs?.all { !it.isNullOrBlank() } shouldBe true
            firstSavedIDs?.distinct()?.size shouldBe 2
            firstSavedPositions shouldBe listOf(0, 1)
            val firstItemID = requireNotNull(firstSavedIDs?.first())

            repository.scoreChecklistItem(requireNotNull(task.id), firstItemID)

            task.checklist.orEmpty().map { it.id } shouldBe firstSavedIDs
            task.checklist.orEmpty().map { it.position } shouldBe firstSavedPositions
            task.checklist.orEmpty().first().completed shouldBe true
            task.checklist.orEmpty().first().pendingSync shouldBe true
            task.pendingChecklist shouldBe true
            coVerify(exactly = 0) {
                apiClient.scoreChecklistItem(any(), any(), any(), any(), any())
            }
            verify(exactly = 2) { offlineTaskSyncScheduler.enqueue(any(), any()) }
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
            verify(exactly = 0) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "preserve the owner if authentication is temporarily unavailable" {
            authenticatedUserID = ""
            val task =
                Task().apply {
                    ownerID = "local-user"
                    type = TaskType.TODO
                }
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.createTask(task, true, any(), any()) } returns null

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
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = task.ownerID
                    type = task.type
                    completed = false
                    pendingCreate = true
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = task.type
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.getTaskCopy("local-task-id", authenticatedUserID) } returns flowOf(latestLocalTask)
            every {
                localRepository.replaceTask("local-task-id", authenticatedUserID, serverTask)
            } returns Unit
            coEvery { apiClient.createTask(task, true, any(), any()) } returns serverTask

            repository.createTask(task) shouldBe serverTask

            serverTask.pendingCreate shouldBe false
            serverTask.pendingScoreUp shouldBe true
            serverTask.completed shouldBe false
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
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = task.type
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every {
                localRepository.replaceTask("local-task-id", authenticatedUserID, serverTask)
            } returns Unit
            every {
                localRepository.resolveTaskID("local-task-id", "android_local-task-id")
            } returns "server-task-id"
            every {
                localRepository.getTask("server-task-id", authenticatedUserID)
            } returns flowOf(serverTask)
            coEvery {
                apiClient.getTask(
                    "server-task-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns serverTask
            every {
                localRepository.moveTaskToPosition(
                    "server-task-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                    TaskType.TODO,
                    4,
                )
            } answers {
                serverTask.position = 4
                serverTask.pendingPosition = true
            }
            coEvery { apiClient.createTask(task, true, any(), any()) } returns serverTask
            coEvery {
                apiClient.postTaskNewPosition("server-task-id", 4, true, any(), any())
            } returns listOf("server-task-id")

            repository.createTask(task) shouldBe serverTask
            repository.updateTaskPosition(TaskType.TODO, "local-task-id", 4) shouldBe
                listOf("server-task-id")

            coVerify(exactly = 1) {
                apiClient.postTaskNewPosition("server-task-id", 4, true, any(), any())
            }
            verify(exactly = 1) {
                localRepository.updateTaskPositions(
                    listOf("server-task-id"),
                    authenticatedUserID,
                )
            }
            verify(exactly = 0) {
                localRepository.updateTaskPositions(listOf("server-task-id"))
            }
        }

        "keep pending work retryable if authentication is temporarily unavailable" {
            authenticatedUserID = ""

            repository.syncPendingTaskCreations() shouldBe false

            verify(exactly = 0) { localRepository.getPendingTaskCreations(any()) }
        }

        "sync only queued personal creations" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = UUID.randomUUID().toString()
                    alias = offlineCreateAlias(requireNotNull(id))
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    hasErrored = true
                    pendingCreate = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskCreations("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty(), any(), any()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(task, true, any(), any()) } returns
                Task().apply {
                    id = task.id
                    alias = task.alias
                    ownerID = authenticatedUserID
                    type = task.type
                }

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.createTask(task, true, any(), any()) }
        }

        "not create when the server state is unknown" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    pendingCreate = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty(), any(), any()) } returns
                TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations() shouldBe false

            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
        }

        "preserve a queued check-off while CREATE is in flight" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = false
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty(), any(), any()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(task, true, any(), any()) } returns null

            repository.syncPendingTaskCreations() shouldBe false

            task.completed shouldBe false
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
                    alias = offlineCreateAlias(requireNotNull(id))
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    hasErrored = true
                    pendingCreate = true
                    pendingPosition = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    alias = task.alias
                    type = task.type
                }
            val savedTasks = mutableListOf<Task>()
            every { localRepository.getPendingTaskCreations("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(any<Task>()) } answers {
                savedTasks += firstArg<Task>()
            }
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns onlineTask

            repository.syncPendingTaskCreations() shouldBe true

            savedTasks.last().ownerID shouldBe task.ownerID
            savedTasks.last().isCreating shouldBe false
            savedTasks.last().hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            verify(atLeast = 1) { localRepository.save(any<Task>()) }
        }

        "persist a legacy alias before reconciling a lost CREATE response" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val recoveredServerTask =
                Task().apply {
                    id = "server-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            var aliasProbeCount = 0
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } answers {
                flowOf(listOf(storedTask).filter { it.pendingCreate })
            }
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers { storedTask = firstArg() }
            every {
                localRepository.replaceTask("local-task-id", authenticatedUserID, any())
            } answers {
                storedTask = thirdArg()
            }
            coEvery {
                apiClient.getTask(
                    offlineCreateAlias("local-task-id"),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } answers {
                storedTask.alias shouldBe offlineCreateAlias("local-task-id")
                aliasProbeCount += 1
                recoveredServerTask.takeIf { aliasProbeCount > 1 }
            }
            coEvery {
                apiClient.getTask("local-task-id", true, authenticatedUserID, any())
            } returns null
            coEvery {
                apiClient.getTaskServerState(
                    offlineCreateAlias("local-task-id"),
                    authenticatedUserID,
                    any(),
                )
            } returns TaskServerState.MISSING
            coEvery {
                apiClient.getTaskServerState("local-task-id", authenticatedUserID, any())
            } returns TaskServerState.MISSING
            coEvery {
                apiClient.createTask(any(), true, authenticatedUserID, any())
            } returns null

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            storedTask.alias shouldBe offlineCreateAlias("local-task-id")
            storedTask.pendingCreate shouldBe true

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            storedTask.id shouldBe "server-task-id"
            storedTask.pendingCreate shouldBe false
            coVerify(exactly = 1) {
                apiClient.createTask(any(), true, authenticatedUserID, any())
            }
            coVerify(exactly = 2) {
                apiClient.getTask(
                    offlineCreateAlias("local-task-id"),
                    true,
                    authenticatedUserID,
                    any(),
                )
            }
        }

        "send an edit made after a lost CREATE response" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "edited after create"
                    pendingCreate = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    alias = offlineCreateAlias("local-task-id")
                    type = TaskType.TODO
                    text = "original create"
                }
            val updatePayload = slot<Task>()
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } answers {
                flowOf(listOf(storedTask).filter { it.pendingCreate })
            }
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            every {
                localRepository.replaceTask("local-task-id", authenticatedUserID, any())
            } answers {
                storedTask = thirdArg()
            }
            coEvery {
                apiClient.getTask(
                    offlineCreateAlias("local-task-id"),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } returns serverTask
            coEvery {
                apiClient.updateTask(
                    "server-task-id",
                    capture(updatePayload),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } answers {
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = updatePayload.captured.text
                }
            }

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            updatePayload.captured.text shouldBe "edited after create"
            storedTask.id shouldBe "server-task-id"
            storedTask.text shouldBe "edited after create"
            storedTask.pendingCreate shouldBe false
            storedTask.pendingUpdate shouldBe false
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 1) {
                apiClient.updateTask("server-task-id", any(), true, authenticatedUserID, any())
            }
        }

        "restore edited create fields before scoring after a lost CREATE response" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 2.0f
                    pendingCreate = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    alias = offlineCreateAlias("local-task-id")
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                }
            val updatePayload = slot<Task>()
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } answers {
                flowOf(listOf(storedTask).filter { it.pendingCreate })
            }
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            every {
                localRepository.replaceTask("local-task-id", authenticatedUserID, any())
            } answers {
                storedTask = thirdArg()
            }
            coEvery {
                apiClient.getTask(
                    offlineCreateAlias("local-task-id"),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } returns serverTask
            coEvery {
                apiClient.updateTask(
                    "server-task-id",
                    capture(updatePayload),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } answers {
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = updatePayload.captured.priority
                }
            }
            coEvery {
                apiClient.postTaskDirection(
                    "server-task-id",
                    "up",
                    true,
                    authenticatedUserID,
                    any(),
                )
            } returns TaskDirectionData().apply { lvl = 1 }

            repository.taskChecked(null, storedTask, true, false, null)
            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false
            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            updatePayload.captured.priority shouldBe 2.0f
            storedTask.pendingCreate shouldBe false
            storedTask.pendingScoreRefresh shouldBe true
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerifyOrder {
                apiClient.updateTask("server-task-id", any(), true, authenticatedUserID, any())
                apiClient.postTaskDirection(
                    "server-task-id",
                    "up",
                    true,
                    authenticatedUserID,
                    any(),
                )
            }
        }

        "reconcile before the foreground retry path can create a duplicate" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = UUID.randomUUID().toString()
                    alias = offlineCreateAlias(requireNotNull(id))
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    hasErrored = true
                    pendingCreate = true
                    pendingPosition = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    alias = task.alias
                    type = task.type
                }
            every { localRepository.getErroredTasks("test-user") } returns flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(onlineTask) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns onlineTask

            repository.syncErroredTasks().orEmpty().single() shouldBe onlineTask

            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            verify(atLeast = 1) { localRepository.save(onlineTask) }
        }

        "queue a check-off behind a pending task creation" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit

            repository.taskChecked(null, task, true, false, null) shouldBe null

            task.completed shouldBe false
            task.isCreating shouldBe true
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingScoreUp shouldBe true
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any(), any(), any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "queue a reorder behind a pending task creation" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 3
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every {
                localRepository.getTask("local-task-id", authenticatedUserID)
            } returns flowOf(task)
            every {
                localRepository.moveTaskToPosition(
                    "local-task-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                    TaskType.TODO,
                    3,
                )
            } answers {
                task.position = 3
                task.pendingPosition = true
            }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit

            repository.updateTaskPosition(TaskType.TODO, "local-task-id", 3) shouldBe emptyList()

            coVerify(exactly = 0) { apiClient.postTaskNewPosition(any(), any(), any(), any(), any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "queue a reorder behind a pending score" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 1
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every {
                localRepository.getTask("todo-id", authenticatedUserID)
            } returns flowOf(task)
            every {
                localRepository.moveTaskToPosition(
                    "todo-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                    TaskType.TODO,
                    4,
                )
            } answers {
                task.position = 4
                task.pendingPosition = true
            }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit

            repository.updateTaskPosition(TaskType.TODO, "todo-id", 4) shouldBe emptyList()

            task.position shouldBe 4
            task.pendingPosition shouldBe true
            task.pendingScoreUp shouldBe true
            coVerify(exactly = 0) { apiClient.postTaskNewPosition(any(), any(), any(), any(), any()) }
        }

        "queue create reconciliation when a reorder cannot reach the server" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "legacy-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 4
                }
            every {
                localRepository.getTask("legacy-task-id", authenticatedUserID)
            } returns flowOf(task)
            every {
                localRepository.moveTaskToPosition(
                    "legacy-task-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                    TaskType.TODO,
                    4,
                )
            } answers {
                task.position = 4
                task.pendingPosition = true
            }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit
            coEvery {
                apiClient.postTaskNewPosition("legacy-task-id", 4, true, any(), any())
            } returns null

            repository.updateTaskPosition(TaskType.TODO, "legacy-task-id", 4) shouldBe emptyList()

            task.isCreating shouldBe false
            task.isSaving shouldBe false
            task.pendingPosition shouldBe true
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "create before replaying a queued reorder and check-off" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    position = 2
                    completed = false
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            var storedTask = task
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                }
            val user = User().apply { stats = Stats() }
            var createPayloadCompleted = true
            var createPayloadPendingScore = false
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } answers {
                flowOf(listOf(storedTask).filter { it.pendingCreate })
            }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            every {
                localRepository.replaceTask("local-task-id", authenticatedUserID, any())
            } answers {
                storedTask = thirdArg()
            }
            every { localRepository.updateTaskPositions(listOf("server-task-id")) } returns Unit
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns null
            coEvery { apiClient.getTaskServerState(task.offlineCreateAlias().orEmpty(), any(), any()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(any(), true, any(), any()) } answers {
                val payload = firstArg<Task>()
                createPayloadCompleted = payload.completed
                createPayloadPendingScore = payload.pendingScoreUp
                serverTask
            }
            coEvery {
                apiClient.postTaskNewPosition("server-task-id", 2, true, any(), any())
            } returns listOf("server-task-id")
            coEvery {
                apiClient.postTaskDirection("server-task-id", "up", true, any(), any())
            } returns TaskDirectionData().apply { lvl = 1 }

            repository.syncPendingTaskCreations() shouldBe true

            createPayloadCompleted shouldBe false
            createPayloadPendingScore shouldBe true
            coVerifyOrder {
                apiClient.createTask(any(), true, any(), any())
                apiClient.postTaskNewPosition("server-task-id", 2, true, any(), any())
                apiClient.postTaskDirection("server-task-id", "up", true, any(), any())
            }
        }

        "restore CREATE when a queued action targets a server-missing task" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "legacy-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    position = 5
                    pendingPosition = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    alias = offlineCreateAlias("legacy-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } answers {
                flowOf(listOf(storedTask))
            }
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            every {
                localRepository.replaceTask("legacy-task-id", authenticatedUserID, any())
            } answers {
                storedTask = thirdArg()
            }
            every { localRepository.updateTaskPositions(listOf("server-task-id")) } returns Unit
            coEvery { apiClient.getTask("legacy-task-id", true, any(), any()) } returns null
            coEvery { apiClient.getTaskServerState("legacy-task-id", any(), any()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.getTask("android_legacy-task-id", true, any(), any()) } returns null
            coEvery { apiClient.getTaskServerState("android_legacy-task-id", any(), any()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.createTask(any(), true, any(), any()) } returns serverTask
            coEvery {
                apiClient.postTaskNewPosition("server-task-id", 5, true, any(), any())
            } returns listOf("server-task-id")

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.createTask(any(), true, any(), any()) }
            coVerify(exactly = 1) {
                apiClient.postTaskNewPosition("server-task-id", 5, true, any(), any())
            }
        }

        "reconcile an acknowledged check-off without scoring twice" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "task-id"
                    alias = offlineCreateAlias("task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    position = 1
                    completed = false
                    isCreating = true
                    pendingCreate = true
                    pendingPosition = true
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    alias = task.alias
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.updateTaskPositions(listOf("task-id")) } returns Unit
            coEvery { apiClient.getTask(task.offlineCreateAlias().orEmpty(), true, any(), any()) } returns onlineTask
            coEvery { apiClient.postTaskNewPosition("task-id", 1, true, any(), any()) } returns
                listOf("task-id")

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any(), any(), any()) }
        }
    }

    "offline To Do completion" should {
        "leave confirmed state untouched and schedule a silent retry" {
            authenticatedUserID = "test-user"
            val user = User().apply { stats = Stats().apply { gp = 12.0 } }
            var callbackInvoked = false
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.postTaskDirection("todo-id", "up", true, any(), any()) } returns null

            repository.taskChecked(user, task, true, false) { callbackInvoked = true } shouldBe null

            task.completed shouldBe false
            user.stats?.gp shouldBe 12.0
            callbackInvoked shouldBe false
            task.hasErrored shouldBe false
            task.isSaving shouldBe false
            task.pendingScoreUp shouldBe true
            verify(exactly = 1) { localRepository.save(task) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "reconcile an already-completed server task without scoring it twice" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = false
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    type = TaskType.TODO
                    text = "todo"
                    completed = true
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns onlineTask

            repository.syncPendingTaskCreations() shouldBe true

            onlineTask.ownerID shouldBe task.ownerID
            task.completed shouldBe true
            task.pendingScoreUp shouldBe false
            task.pendingScoreRefresh shouldBe true
            onlineTask.hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.postTaskNewPosition(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.postTaskDirection(any(), any(), any(), any(), any()) }
            verify(atLeast = 1) { localRepository.save(task) }
        }

        "submit a persisted completion when connectivity returns" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = false
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = TaskType.TODO
                    text = "todo"
                }
            val user = User().apply { stats = Stats() }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } returns onlineTask
            coEvery {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            } returns
                TaskDirectionData().apply { lvl = 1 }

            repository.syncPendingTaskCreations() shouldBe true

            task.completed shouldBe true
            task.pendingScoreUp shouldBe false
            task.pendingScoreRefresh shouldBe true
            coVerify(exactly = 1) {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            }
        }

        "replay reorder before check-off when both are queued" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    position = 6
                    completed = false
                    pendingPosition = true
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val onlineTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                    text = "todo"
                }
            val user = User().apply { stats = Stats() }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.getUser(authenticatedUserID) } returns flowOf(user)
            every { localRepository.updateTaskPositions(listOf("todo-id")) } returns Unit
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns onlineTask
            coEvery { apiClient.postTaskNewPosition("todo-id", 6, true, any(), any()) } returns
                listOf("todo-id")
            coEvery { apiClient.postTaskDirection("todo-id", "up", true, any(), any()) } returns
                TaskDirectionData().apply { lvl = 1 }

            repository.syncPendingTaskCreations() shouldBe true

            coVerifyOrder {
                apiClient.postTaskNewPosition("todo-id", 6, true, any(), any())
                apiClient.postTaskDirection("todo-id", "up", true, any(), any())
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
            coEvery { apiClient.postTaskDirection("legacy-task-id", "up", true, any(), any()) } returns null

            repository.taskChecked(null, task, true, false, null) shouldBe null

            task.isCreating shouldBe false
            task.completed shouldBe false
            task.isSaving shouldBe false
            task.hasErrored shouldBe false
            task.pendingScoreUp shouldBe true
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "queue score down without changing confirmed completion" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = true
                }
            every { localRepository.getUnmanagedCopy(task) } returns task
            every { localRepository.save(task) } returns Unit
            coEvery { apiClient.postTaskDirection("todo-id", "down", true, any(), any()) } returns null

            repository.taskChecked(null, task, false, false, null) shouldBe null

            task.completed shouldBe true
            task.pendingScoreUp shouldBe false
            task.pendingScoreDown shouldBe true
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "replay score down through the full server operation" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = true
                    pendingScoreDown = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(storedTask))
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns serverTask
            coEvery { apiClient.postTaskDirection("todo-id", "down", true, any(), any()) } returns
                TaskDirectionData().apply { lvl = 1 }

            repository.syncPendingTaskCreations() shouldBe true

            storedTask.completed shouldBe false
            storedTask.pendingScoreDown shouldBe false
            storedTask.pendingScoreRefresh shouldBe true
            coVerify(exactly = 1) { apiClient.postTaskDirection("todo-id", "down", true, any(), any()) }
        }

        "keep a score receipt durable before deleting" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    pendingScoreUp = true
                    pendingDeleteAfterScore = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(storedTask))
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            every { localRepository.deleteTask("todo-id", authenticatedUserID) } returns Unit
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns serverTask
            coEvery { apiClient.postTaskDirection("todo-id", "up", true, any(), any()) } returns
                TaskDirectionData().apply { lvl = 1 }
            coEvery { apiClient.deleteTask("todo-id", true, any(), any()) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            storedTask.pendingScoreRefresh shouldBe true
            storedTask.pendingDeleteAfterScore shouldBe true
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }

            repository.clearPendingTodoScoreRefreshes(
                authenticatedUserID,
                listOf(storedTask),
            )
            repository.syncPendingTaskCreations() shouldBe true

            coVerifyOrder {
                apiClient.postTaskDirection("todo-id", "up", true, any(), any())
                apiClient.deleteTask("todo-id", true, any(), any())
            }
            verify(exactly = 1) {
                localRepository.deleteTask("todo-id", authenticatedUserID)
            }
        }
    }

    "offline To Do edits" should {
        "keep the submitted edit snapshot and queue it silently" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "old text"
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = "new text"
                }
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns null

            repository.updateTask(editedTask)?.text shouldBe "new text"

            storedTask.text shouldBe "new text"
            storedTask.pendingUpdate shouldBe true
            storedTask.pendingEditFields shouldBe "text"
            storedTask.hasErrored shouldBe false
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "preserve newer local fields that a stale form did not edit" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "old text"
                    notes = "newer notes"
                }
            val editBaseline =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = "old text"
                    notes = "old notes"
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = "new text"
                    notes = "old notes"
                }
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns null

            repository.updateTask(editedTask, editBaseline = editBaseline)

            storedTask.text shouldBe "new text"
            storedTask.notes shouldBe "newer notes"
            storedTask.pendingEditFields shouldBe "text"
        }

        "queue an edit made after a score without sending it out of order" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "old text"
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = "new text"
                    pendingScoreUp = true
                }
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }

            repository.updateTask(editedTask)

            storedTask.text shouldBe "new text"
            storedTask.pendingUpdate shouldBe true
            storedTask.pendingScoreUp shouldBe true
            coVerify(exactly = 0) { apiClient.getTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
        }

        "merge edited fields onto fresh server-derived state" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "old text"
                    value = 3.0
                    completed = false
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = "new text"
                    value = -100.0
                    completed = true
                }
            val serverTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = TaskType.TODO
                    text = "server text"
                    value = 42.0
                    completed = false
                }
            val responseTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = TaskType.TODO
                    text = "new text"
                    value = 42.0
                }
            val payload = slot<Task>()
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns serverTask
            coEvery { apiClient.updateTask("todo-id", capture(payload), true, any(), any()) } returns responseTask

            repository.updateTask(editedTask) shouldBe responseTask

            payload.captured.text shouldBe "new text"
            payload.captured.value shouldBe 42.0
            payload.captured.completed shouldBe false
            responseTask.pendingUpdate shouldBe false
            responseTask.pendingEditFields shouldBe null
        }

        "coalesce repeated edits while preserving every dirty field" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "old text"
                }
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns null

            repository.updateTask(
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "new text"
                }
            )
            repository.updateTask(
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "new text"
                    dueDate = Date(1234)
                }
            )

            storedTask.text shouldBe "new text"
            storedTask.dueDate shouldBe Date(1234)
            storedTask.pendingEditFields shouldBe "text,dueDate"
            verify(exactly = 2) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }
    }

    "offline checklist scoring" should {
        "persist the desired checklist target when disconnected" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.scoreChecklistItem("todo-id", "item-id", true, any(), any()) } returns null

            repository.scoreChecklistItem("todo-id", "item-id") shouldBe storedTask

            storedTask.checklist?.single()?.completed shouldBe true
            storedTask.checklist?.single()?.pendingSync shouldBe true
            storedTask.pendingChecklist shouldBe true
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "acknowledge a foreground checklist target already present on the server" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(ChecklistItem("item-id", "item", true))
                }
            every {
                localRepository.getTaskCopy("todo-id", authenticatedUserID)
            } answers { flowOf(storedTask) }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery {
                apiClient.getTask(
                    "todo-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns serverTask

            repository.scoreChecklistItem("todo-id", "item-id") shouldBe storedTask

            storedTask.checklist?.single()?.completed shouldBe true
            storedTask.checklist?.single()?.pendingSync shouldBe false
            storedTask.pendingChecklist shouldBe false
            coVerify(exactly = 0) {
                apiClient.scoreChecklistItem(any(), any(), any(), any(), any())
            }
            verify(atLeast = 2) { localRepository.save(any<Task>()) }
        }

        "keep a foreground checklist target queued when the toggle response is missing" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            every {
                localRepository.getTaskCopy("todo-id", authenticatedUserID)
            } answers { flowOf(storedTask) }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery {
                apiClient.getTask(
                    "todo-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns serverTask
            coEvery {
                apiClient.scoreChecklistItem(
                    "todo-id",
                    "item-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns null

            repository.scoreChecklistItem("todo-id", "item-id") shouldBe storedTask

            storedTask.checklist?.single()?.completed shouldBe true
            storedTask.checklist?.single()?.pendingSync shouldBe true
            storedTask.pendingChecklist shouldBe true
            coVerify(exactly = 1) {
                apiClient.scoreChecklistItem(
                    "todo-id",
                    "item-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            }
        }

        "queue a checklist target behind a pending score" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }

            repository.scoreChecklistItem("todo-id", "item-id") shouldBe storedTask

            storedTask.checklist?.single()?.completed shouldBe true
            storedTask.pendingChecklist shouldBe true
            storedTask.pendingScoreUp shouldBe true
            coVerify(exactly = 0) { apiClient.scoreChecklistItem(any(), any(), any(), any(), any()) }
        }

        "clear an acknowledged checklist target without toggling twice" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingChecklist = true
                    outboxServerOrigin = expectedServerOrigin
                    checklist?.add(
                        ChecklistItem("item-id", "item", true).apply { pendingSync = true }
                    )
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    checklist?.add(ChecklistItem("item-id", "item", true))
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(storedTask))
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns serverTask

            repository.syncPendingTaskCreations() shouldBe true

            storedTask.pendingChecklist shouldBe false
            storedTask.checklist?.single()?.pendingSync shouldBe false
            coVerify(exactly = 0) { apiClient.scoreChecklistItem(any(), any(), any(), any(), any()) }
        }
    }

    "chronological To Do replay" should {
        "replay an edit captured before a score before scoring" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                }
            val editBaseline =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = storedTask.priority
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = 2.0f
                }
            val serverTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = 1.0f
                }
            var syncing = false
            val updatePayload = slot<Task>()
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } answers {
                flowOf(listOf(storedTask))
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } answers { serverTask.takeIf { syncing } }
            coEvery {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            } answers {
                TaskDirectionData().apply { lvl = 1 }.takeIf { syncing }
            }
            coEvery {
                apiClient.updateTask(
                    "todo-id",
                    capture(updatePayload),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } answers {
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = updatePayload.captured.priority
                }
            }

            repository.updateTask(editedTask, editBaseline = editBaseline)
            repository.taskChecked(null, storedTask, true, false, null)

            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false
            storedTask.pendingUpdate shouldBe false
            syncing = true
            clearMocks(apiClient, answers = false, recordedCalls = true)

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            updatePayload.captured.priority shouldBe 2.0f
            storedTask.pendingScoreRefresh shouldBe true
            coVerifyOrder {
                apiClient.updateTask("todo-id", any(), true, authenticatedUserID, any())
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            }
        }

        "preserve server checklist state through edit checklist and score replay" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            val editBaseline =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = storedTask.priority
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = 2.0f
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            val serverAfterEdit =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 2.0f
                    checklist?.add(ChecklistItem("item-id", "item", false))
                }
            val serverAfterChecklist =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 2.0f
                    checklist?.add(ChecklistItem("item-id", "item", true))
                }
            var syncing = false
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } answers {
                flowOf(listOf(storedTask))
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } answers { serverTask.takeIf { syncing } }
            coEvery {
                apiClient.updateTask("todo-id", any(), true, authenticatedUserID, any())
            } returns serverAfterEdit
            coEvery {
                apiClient.scoreChecklistItem("todo-id", "item-id", true, authenticatedUserID, any())
            } returns serverAfterChecklist
            coEvery {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            } returns TaskDirectionData().apply { lvl = 1 }

            repository.updateTask(editedTask, editBaseline = editBaseline)
            repository.scoreChecklistItem("todo-id", "item-id")
            repository.taskChecked(null, storedTask, true, false, null)
            repository.scoreChecklistItem("todo-id", "item-id")

            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false
            storedTask.pendingChecklist shouldBe true
            storedTask.checklist?.single()?.completed shouldBe false
            syncing = true
            clearMocks(apiClient, answers = false, recordedCalls = true)

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            serverAfterEdit.checklist?.single()?.completed shouldBe false
            serverAfterChecklist.checklist?.single()?.completed shouldBe true
            storedTask.checklist?.single()?.completed shouldBe false
            storedTask.pendingChecklist shouldBe true
            storedTask.pendingScoreRefresh shouldBe true
            coVerifyOrder {
                apiClient.updateTask("todo-id", any(), true, authenticatedUserID, any())
                apiClient.scoreChecklistItem("todo-id", "item-id", true, authenticatedUserID, any())
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            }
        }

        "replay a score before an edit made after the score boundary" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                }
            val editBaseline =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = storedTask.priority
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = 2.0f
                }
            var serverTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = 1.0f
                }
            val updatePayload = slot<Task>()
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } answers {
                flowOf(listOf(storedTask))
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            } returns null

            repository.taskChecked(null, storedTask, true, false, null)
            repository.updateTask(editedTask, editBaseline = editBaseline)

            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false
            storedTask.pendingUpdate shouldBe true
            clearMocks(apiClient, answers = false, recordedCalls = true)
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } answers { serverTask }
            coEvery {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            } returns TaskDirectionData().apply { lvl = 1 }

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            storedTask.pendingScoreRefresh shouldBe true
            storedTask.pendingUpdate shouldBe true
            coVerify(exactly = 1) {
                apiClient.postTaskDirection("todo-id", "up", true, authenticatedUserID, any())
            }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }

            repository.clearPendingTodoScoreRefreshes(
                authenticatedUserID,
                listOf(storedTask),
            )
            serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                    completed = true
                }
            clearMocks(apiClient, answers = false, recordedCalls = true)
            coEvery {
                apiClient.updateTask(
                    "todo-id",
                    capture(updatePayload),
                    true,
                    authenticatedUserID,
                    any(),
                )
            } answers {
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = updatePayload.captured.priority
                    completed = true
                }
            }

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            updatePayload.captured.priority shouldBe 2.0f
            storedTask.pendingUpdate shouldBe false
            coVerify(exactly = 1) {
                apiClient.updateTask("todo-id", any(), true, authenticatedUserID, any())
            }
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
        }

        "create from the score boundary and preserve a later edit until receipt" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    priority = 1.0f
                    position = 3
                }
            val editBaseline =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = storedTask.priority
                    position = storedTask.position
                }
            val editedTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    priority = 2.0f
                    position = storedTask.position
                }
            var syncing = false
            var createdPriority = 0.0f
            every {
                localRepository.getTaskCopy("local-task-id", authenticatedUserID)
            } answers { flowOf(storedTask) }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } answers {
                flowOf(listOf(storedTask).filter { it.pendingCreate })
            }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            every { localRepository.updateTaskPositions(any()) } returns Unit
            coEvery {
                apiClient.createTask(any(), true, authenticatedUserID, any())
            } answers {
                if (!syncing) {
                    null
                } else {
                    createdPriority = firstArg<Task>().priority
                    Task().apply {
                        id = "local-task-id"
                        alias = offlineCreateAlias("local-task-id")
                        ownerID = authenticatedUserID
                        type = TaskType.TODO
                        text = "todo"
                        priority = createdPriority
                    }
                }
            }

            repository.createTask(storedTask)
            repository.taskChecked(null, storedTask, true, false, null)
            repository.updateTask(editedTask, editBaseline = editBaseline)

            storedTask.pendingCreate shouldBe true
            storedTask.pendingUpdate shouldBe true
            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false
            syncing = true
            clearMocks(apiClient, answers = false, recordedCalls = true)
            coEvery {
                apiClient.postTaskNewPosition(
                    "local-task-id",
                    3,
                    true,
                    authenticatedUserID,
                    any(),
                )
            } returns listOf("local-task-id")
            coEvery {
                apiClient.postTaskDirection(
                    "local-task-id",
                    "up",
                    true,
                    authenticatedUserID,
                    any(),
                )
            } returns TaskDirectionData().apply { lvl = 1 }

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            createdPriority shouldBe 1.0f
            storedTask.priority shouldBe 2.0f
            storedTask.pendingCreate shouldBe false
            storedTask.pendingScoreRefresh shouldBe true
            storedTask.pendingUpdate shouldBe true
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerifyOrder {
                apiClient.createTask(any(), true, authenticatedUserID, any())
                apiClient.postTaskNewPosition(
                    "local-task-id",
                    3,
                    true,
                    authenticatedUserID,
                    any(),
                )
                apiClient.postTaskDirection(
                    "local-task-id",
                    "up",
                    true,
                    authenticatedUserID,
                    any(),
                )
            }
        }

        "leave a malformed score boundary queued without mutating the server" {
            authenticatedUserID = "test-user"
            val malformedBoundary = "{not-json"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreUp = true
                    pendingScoreSnapshot = malformedBoundary
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(emptyList())
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } returns
                flowOf(task)
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } returns serverTask

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            task.pendingScoreUp shouldBe true
            task.pendingScoreSnapshot shouldBe malformedBoundary
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) {
                apiClient.postTaskNewPosition(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) {
                apiClient.scoreChecklistItem(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
        }
    }

    "offline task serialization" should {
        "send a deterministic alias and leave queued SCORE_UP incomplete" {
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    type = TaskType.TODO
                    completed = false
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
            task.completed shouldBe false
            task.pendingScoreUp shouldBe true
        }
    }

    "outbox state" should {
        "survive a concurrent successful task edit" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            val latestLocalTask =
                Task().apply {
                    id = task.id
                    ownerID = authenticatedUserID
                    type = task.type
                    completed = false
                    pendingPosition = true
                    pendingScoreUp = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = task.id
                    ownerID = authenticatedUserID
                    type = task.type
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every {
                localRepository.getTaskCopy("task-id", authenticatedUserID)
            } returns flowOf(latestLocalTask)
            coEvery { apiClient.updateTask("task-id", any()) } returns serverTask

            repository.updateTask(task, force = true) shouldBe serverTask

            serverTask.pendingPosition shouldBe true
            serverTask.pendingScoreUp shouldBe true
            serverTask.completed shouldBe false
        }
    }

    "account-pinned outbox sync" should {
        "not send another account's queued operations" {
            authenticatedUserID = "other-user"

            repository.syncPendingTaskCreations("queued-user") shouldBe false

            coVerify(exactly = 0) { apiClient.getTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
        }

        "ignore a stale personal edit reorder and delete from another account" {
            authenticatedUserID = "active-user"
            val staleTask =
                Task().apply {
                    id = "stale-task-id"
                    ownerID = "other-user"
                    type = TaskType.TODO
                    text = "stale edit"
                    position = 4
            }
            every { localRepository.getTask("stale-task-id") } returns flowOf(staleTask)
            every { localRepository.getTaskCopy("stale-task-id") } returns flowOf(staleTask)

            repository.updateTask(staleTask) shouldBe null
            repository.updateTaskPosition(TaskType.TODO, "stale-task-id", 5) shouldBe emptyList()
            repository.deleteTask("stale-task-id") shouldBe false

            verify(exactly = 0) { localRepository.save(any<Task>()) }
            verify(exactly = 0) { offlineTaskSyncScheduler.enqueue(any(), any()) }
            coVerify(exactly = 0) { apiClient.getTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) {
                apiClient.postTaskNewPosition(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
        }

        "leave an operation queued if the account changes during a request" {
            authenticatedUserID = "queued-user"
            hostConfig.updateAuthentication("queued-user", "fake-api-key")
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingUpdate = true
                    pendingEditFields = "text"
                    text = "local text"
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                }
            every { localRepository.getPendingTaskActions("queued-user") } returns flowOf(listOf(task))
            coEvery { apiClient.getTask("todo-id", true, "queued-user", any()) } answers {
                authenticatedUserID = "other-user"
                hostConfig.updateAuthentication("other-user", "other-fake-api-key")
                serverTask
            }

            repository.syncPendingTaskCreations("queued-user") shouldBe false

            task.pendingUpdate shouldBe true
            coVerify(exactly = 1) {
                apiClient.getTask("todo-id", true, "queued-user", expectedServerOrigin)
            }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
        }
    }

    "score refresh receipts" should {
        "not replay or resurrect an acknowledged receipt as a mutation" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreRefresh = true
                    completed = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getPendingTaskScoreRefreshes(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.getUnmanagedCopy(task) } returns task

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            repository.getPendingTodoScoreRefreshes(authenticatedUserID) shouldBe listOf(task)
            coVerify(exactly = 0) { apiClient.getTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
        }

        "clear an old receipt without swallowing a newer opposite score" {
            authenticatedUserID = "test-user"
            val receiptTime = Date(1_000L)
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "todo"
                    completed = true
                    pendingScoreRefresh = true
                    updatedAt = receiptTime
                    outboxServerOrigin = expectedServerOrigin
                }
            val refreshedReceipt =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    completed = true
                    updatedAt = Date(receiptTime.time)
                }
            val serverTask =
                Task().apply {
                    id = storedTask.id
                    ownerID = storedTask.ownerID
                    type = storedTask.type
                    text = storedTask.text
                    completed = true
                    updatedAt = Date(receiptTime.time)
                }
            var syncing = false
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } answers {
                flowOf(listOf(storedTask))
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.getUser(authenticatedUserID) } returns
                flowOf(User().apply { stats = Stats() })
            every { localRepository.save(any<Task>()) } answers {
                (firstArg<BaseObject>() as? Task)?.let { storedTask = it }
            }
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } returns serverTask
            coEvery {
                apiClient.postTaskDirection("todo-id", "down", true, authenticatedUserID, any())
            } answers {
                TaskDirectionData().apply { lvl = 1 }.takeIf { syncing }
            }

            repository.taskChecked(null, storedTask, false, false, null)

            storedTask.pendingScoreRefresh shouldBe true
            storedTask.pendingScoreDown shouldBe true
            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false

            repository.clearPendingTodoScoreRefreshes(
                authenticatedUserID,
                listOf(refreshedReceipt),
            )

            storedTask.pendingScoreRefresh shouldBe false
            storedTask.pendingScoreDown shouldBe true
            storedTask.pendingScoreSnapshot.isNullOrBlank() shouldBe false
            syncing = true
            clearMocks(apiClient, answers = false, recordedCalls = true)

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            storedTask.completed shouldBe false
            storedTask.pendingScoreDown shouldBe false
            storedTask.pendingScoreSnapshot shouldBe null
            storedTask.pendingScoreRefresh shouldBe true
            coVerify(exactly = 1) {
                apiClient.postTaskDirection("todo-id", "down", true, authenticatedUserID, any())
            }
        }

        "retain a remotely missing scored task until alarm cleanup is acknowledged" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreRefresh = true
                    completed = false
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } returns
                flowOf(task)
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } returns null
            coEvery {
                apiClient.getTaskServerState("todo-id", authenticatedUserID, any())
            } returns TaskServerState.MISSING

            val refreshed =
                repository.refreshPendingTodoScoreTasks(listOf(task), authenticatedUserID)

            refreshed.size shouldBe 1
            refreshed.single().missingDuringScoreRefresh shouldBe true
            refreshed.single().pendingDeleteAfterScore shouldBe true
            task.pendingDeleteAfterScore shouldBe true
            verify(exactly = 0) {
                localRepository.deleteTask("todo-id", authenticatedUserID)
            }
            verify(exactly = 1) { localRepository.save(task) }
        }

        "recreate a remotely missing scored task when newer local writes exist" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "edited offline"
                    pendingScoreRefresh = true
                    pendingUpdate = true
                    pendingEditFields = "text"
                    outboxServerOrigin = expectedServerOrigin
                }
            val missingResult =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                    text = task.text
                    pendingScoreRefresh = true
                    pendingUpdate = true
                    pendingEditFields = task.pendingEditFields
                    outboxServerOrigin = expectedServerOrigin
                }
            val recoveryTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                    text = task.text
                    pendingScoreRefresh = true
                    pendingUpdate = true
                    pendingEditFields = task.pendingEditFields
                    outboxServerOrigin = expectedServerOrigin
                }
            val savedTask = slot<Task>()
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } returns
                flowOf(task)
            every { localRepository.getUnmanagedCopy(task) } returnsMany
                listOf(missingResult, recoveryTask)
            every { localRepository.save(capture(savedTask)) } returns Unit
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, any())
            } returns null
            coEvery {
                apiClient.getTaskServerState("todo-id", authenticatedUserID, any())
            } returns TaskServerState.MISSING

            val refreshed =
                repository.refreshPendingTodoScoreTasks(listOf(task), authenticatedUserID)

            refreshed.size shouldBe 1
            refreshed.single().missingDuringScoreRefresh shouldBe true
            savedTask.captured.pendingCreate shouldBe true
            savedTask.captured.pendingScoreRefresh shouldBe false
            savedTask.captured.pendingScoreUp shouldBe false
            savedTask.captured.pendingScoreDown shouldBe false
            savedTask.captured.pendingUpdate shouldBe true
            savedTask.captured.pendingEditFields shouldBe "text"
            savedTask.captured.pendingPosition shouldBe true
            savedTask.captured.alias shouldBe offlineCreateAlias("todo-id")
            verify(exactly = 0) {
                localRepository.deleteTask("todo-id", authenticatedUserID)
            }
        }

        "preserve a receipt when the authenticated account changes during refresh" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreRefresh = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = task.id
                    ownerID = task.ownerID
                    type = task.type
                }
            coEvery {
                apiClient.getTask("todo-id", true, "test-user", any())
            } answers {
                authenticatedUserID = "other-user"
                serverTask
            }

            repository.refreshPendingTodoScoreTasks(listOf(task), "test-user") shouldBe emptyList()

            task.pendingScoreRefresh shouldBe true
            verify(exactly = 0) { localRepository.save(any<Task>()) }
            verify(exactly = 0) { localRepository.deleteTask(any(), any()) }
        }
    }

    "checklist conflict handling" should {
        "resolve a remotely missing checklist item without retrying forever" {
            authenticatedUserID = "test-user"
            var storedTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingChecklist = true
                    outboxServerOrigin = expectedServerOrigin
                    checklist?.add(
                        ChecklistItem("missing-item", "removed elsewhere", true).apply {
                            pendingSync = true
                        }
                    )
                }
            val serverTask =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(storedTask))
            every { localRepository.getTaskCopy("todo-id") } answers { flowOf(storedTask) }
            every { localRepository.getTaskCopy("todo-id", authenticatedUserID) } answers {
                flowOf(storedTask)
            }
            every { localRepository.getUnmanagedCopy(any<Task>()) } answers { firstArg() }
            every { localRepository.save(any<Task>()) } answers {
                storedTask = firstArg()
            }
            coEvery { apiClient.getTask("todo-id", true, any(), any()) } returns serverTask

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            storedTask.pendingChecklist shouldBe false
            storedTask.checklist.orEmpty() shouldBe emptyList()
            coVerify(exactly = 0) { apiClient.scoreChecklistItem(any(), any(), any(), any(), any()) }
        }
    }

    "identity-safe server reconciliation" should {
        "not mutate an alias collision returned for a queued action direct ID" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "local edit"
                    pendingUpdate = true
                    pendingEditFields = "text"
                    outboxServerOrigin = expectedServerOrigin
                }
            val collidingTask =
                Task().apply {
                    id = "other-task-id"
                    alias = task.id
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    text = "unrelated task"
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, expectedServerOrigin)
            } returns collidingTask
            coEvery {
                apiClient.getTaskServerState(
                    "todo-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            verify(exactly = 0) { localRepository.save(any<Task>()) }
            verify(exactly = 0) { localRepository.replaceTask(any(), any(), any()) }
            verify(exactly = 0) { localRepository.deleteTask(any(), any()) }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
        }

        "not accept an alias collision as a score receipt direct ID" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    completed = true
                    pendingScoreRefresh = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val collidingTask =
                Task().apply {
                    id = "other-task-id"
                    alias = task.id
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, expectedServerOrigin)
            } returns collidingTask
            coEvery {
                apiClient.getTaskServerState(
                    "todo-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.UNKNOWN

            repository.refreshPendingTodoScoreTasks(listOf(task), authenticatedUserID) shouldBe
                emptyList()

            task.pendingScoreRefresh shouldBe true
            verify(exactly = 0) { localRepository.save(any<Task>()) }
            verify(exactly = 0) { localRepository.deleteTask(any(), any()) }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
        }

        "not create after a queued CREATE direct-ID probe returns another task" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias(requireNotNull(id))
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    pendingPosition = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val collidingTask =
                Task().apply {
                    id = "other-task-id"
                    alias = task.id
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery {
                apiClient.getTask(
                    task.alias.orEmpty(),
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns null
            coEvery {
                apiClient.getTask(
                    "local-task-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns collidingTask
            coEvery {
                apiClient.getTaskServerState(
                    task.alias.orEmpty(),
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.MISSING
            coEvery {
                apiClient.getTaskServerState(
                    "local-task-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) {
                apiClient.postTaskNewPosition(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
        }

        "require a deterministic-alias response to carry that alias" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias(requireNotNull(id))
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val collidingTask =
                Task().apply {
                    id = "other-task-id"
                    alias = "another-alias"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskCreations(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery {
                apiClient.getTask(
                    task.alias.orEmpty(),
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns collidingTask
            coEvery {
                apiClient.getTask(
                    "local-task-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns null
            coEvery {
                apiClient.getTaskServerState(
                    task.alias.orEmpty(),
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.UNKNOWN
            coEvery {
                apiClient.getTaskServerState(
                    "local-task-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.MISSING

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            verify(exactly = 0) { localRepository.replaceTask(any(), any(), any()) }
        }

        "not score an alias collision returned for a foreground direct ID" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            val collidingTask =
                Task().apply {
                    id = "other-task-id"
                    alias = task.id
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.save(any<Task>()) } returns Unit
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, expectedServerOrigin)
            } returns collidingTask

            repository.taskChecked(null, task, true, false, null) shouldBe null

            task.pendingScoreUp shouldBe true
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
        }
    }

    "outbox scope and persistence" should {
        "not create score receipts for group or challenge To Dos" {
            authenticatedUserID = "test-user"
            val user =
                User().apply {
                    id = authenticatedUserID
                    stats = Stats()
                }
            val groupTask =
                Task().apply {
                    id = "group-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    group = TaskGroupPlan().apply { groupID = "group-id" }
                }
            val challengeTask =
                Task().apply {
                    id = "challenge-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    challengeID = "challenge-id"
                }
            every { localRepository.save(any<Task>()) } returns Unit
            every { localRepository.save(any<User>()) } returns Unit
            coEvery {
                apiClient.postTaskDirection(any(), "up", false, null, null)
            } returns TaskDirectionData().apply { lvl = 1 }

            repository.taskChecked(user, groupTask, true, false, null)
            repository.taskChecked(user, challengeTask, true, false, null)

            groupTask.pendingScoreRefresh shouldBe false
            challengeTask.pendingScoreRefresh shouldBe false
            groupTask.pendingScoreRefresh = true
            challengeTask.pendingScoreRefresh = true
            every { localRepository.getPendingTaskScoreRefreshes(authenticatedUserID) } returns
                flowOf(listOf(groupTask, challengeTask))
            repository.getPendingTodoScoreRefreshes(authenticatedUserID) shouldBe emptyList()
            verify(exactly = 0) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "include both pending-delete states in worker snapshots" {
            authenticatedUserID = "test-user"
            val pendingDelete =
                Task().apply {
                    id = "pending-delete"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    this.pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val pendingDeleteAfterScore =
                Task().apply {
                    id = "pending-delete-after-score"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    this.pendingDeleteAfterScore = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getTasksIncludingPending(authenticatedUserID) } returns
                flowOf(listOf(pendingDelete, pendingDeleteAfterScore))

            repository.getTodoCopiesForSync(authenticatedUserID).map { it.id } shouldBe
                listOf("pending-delete", "pending-delete-after-score")
        }

        "reject structurally valid boundaries with missing or null collections before mutation" {
            authenticatedUserID = "test-user"
            val taskJson =
                """
                "task": {
                    "id": "%s",
                    "alias": null,
                    "ownerID": "test-user",
                    "text": "todo",
                    "notes": null,
                    "priority": 1.0,
                    "attribute": null,
                    "tagIDs": %s,
                    "dueDate": null,
                    "value": 0.0,
                    "completed": false,
                    "position": 0,
                    "checklist": [],
                    "reminders": []
                }
                """.trimIndent()
            val missingTargets =
                Task().apply {
                    id = "missing-targets"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreUp = true
                    pendingScoreSnapshot =
                        """{"version":1,"direction":"up","requiresCreate":false,"editFields":[],"position":null,${
                            taskJson.format(id, "[]")
                        }}"""
                    outboxServerOrigin = expectedServerOrigin
                }
            val nullNestedCollection =
                Task().apply {
                    id = "null-task-tags"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingScoreUp = true
                    pendingScoreSnapshot =
                        """{"version":1,"direction":"up","requiresCreate":false,"editFields":[],"position":null,"checklistTargets":[],${
                            taskJson.format(id, "null")
                        }}"""
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskActions(authenticatedUserID) } returns
                flowOf(listOf(missingTargets, nullNestedCollection))
            every { localRepository.getTaskCopy(any(), authenticatedUserID) } answers {
                val taskID = firstArg<String>()
                flowOf(listOf(missingTargets, nullNestedCollection).first { it.id == taskID })
            }
            coEvery {
                apiClient.getTask(any(), true, authenticatedUserID, expectedServerOrigin)
            } answers {
                Task().apply {
                    id = firstArg()
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            }

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            coVerify(exactly = 0) { apiClient.updateTask(any(), any(), any(), any(), any()) }
            coVerify(exactly = 0) {
                apiClient.postTaskNewPosition(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) {
                apiClient.scoreChecklistItem(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) {
                apiClient.postTaskDirection(any(), any(), any(), any(), any())
            }
            coVerify(exactly = 0) { apiClient.createTask(any(), any(), any(), any()) }
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
        }
    }

    "task deletion" should {
        "hide a personal task immediately and queue its deletion" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every {
                localRepository.getTaskCopy("local-task-id", authenticatedUserID)
            } returns flowOf(task)
            every { localRepository.save(task) } returns Unit

            repository.deleteTask("local-task-id") shouldBe true

            task.pendingDelete shouldBe true
            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
            verify(exactly = 1) { offlineTaskSyncScheduler.enqueue(any(), any()) }
        }

        "delete an acknowledged pending CREATE by its deterministic alias" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    alias = "android_local-task-id"
                    pendingCreate = true
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val serverTask =
                Task().apply {
                    id = "server-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("local-task-id", authenticatedUserID) } returns Unit
            every { localRepository.deleteTask("server-task-id", authenticatedUserID) } returns Unit
            coEvery { apiClient.deleteTask("local-task-id", true, any(), any()) } returns false
            coEvery { apiClient.getTaskServerState("local-task-id", any(), any()) } returns
                TaskServerState.MISSING
            coEvery { apiClient.getTask("android_local-task-id", true, any(), any()) } returns serverTask
            coEvery { apiClient.deleteTask("server-task-id", true, any(), any()) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.deleteTask("server-task-id", true, any(), any()) }
            verify(exactly = 1) { localRepository.deleteTask("local-task-id", authenticatedUserID) }
            verify(exactly = 1) { localRepository.deleteTask("server-task-id", authenticatedUserID) }
        }

        "delete a pending CREATE directly when its local ID exists" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("local-task-id", authenticatedUserID) } returns Unit
            coEvery {
                apiClient.getTask("local-task-id", true, authenticatedUserID, any())
            } returns task
            coEvery { apiClient.deleteTask("local-task-id", true, any(), any()) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.deleteTask("local-task-id", true, any(), any()) }
            verify(exactly = 1) { localRepository.deleteTask("local-task-id", authenticatedUserID) }
        }

        "remove a missing direct-ID task without touching a colliding deterministic alias" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("server-task-id", authenticatedUserID) } returns Unit
            coEvery { apiClient.deleteTask("server-task-id", true, authenticatedUserID, any()) } returns
                false
            coEvery {
                apiClient.getTaskServerState("server-task-id", authenticatedUserID, any())
            } returns TaskServerState.MISSING

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe true

            verify(exactly = 1) {
                localRepository.deleteTask("server-task-id", authenticatedUserID)
            }
            coVerify(exactly = 0) {
                apiClient.getTask("android_server-task-id", true, authenticatedUserID, any())
            }
            coVerify(exactly = 0) {
                apiClient.deleteTask("android_server-task-id", true, authenticatedUserID, any())
            }
            coVerify(exactly = 0) {
                apiClient.getTaskServerState("android_server-task-id", authenticatedUserID, any())
            }
        }

        "remove a synchronized task after the server accepts deletion" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("server-task-id", authenticatedUserID) } returns Unit
            coEvery {
                apiClient.getTask("server-task-id", true, authenticatedUserID, any())
            } returns task
            coEvery { apiClient.deleteTask("server-task-id", true, any(), any()) } returns true

            repository.syncPendingTaskCreations() shouldBe true

            coVerify(exactly = 1) { apiClient.deleteTask("server-task-id", true, any(), any()) }
            verify(exactly = 1) { localRepository.deleteTask("server-task-id", authenticatedUserID) }
        }

        "keep a deletion queued while server state is unknown" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "server-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery { apiClient.deleteTask("server-task-id", true, any(), any()) } returns false
            coEvery { apiClient.getTaskServerState("server-task-id", any(), any()) } returns
                TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations() shouldBe false

            verify(exactly = 0) { localRepository.deleteTask(any()) }
        }

        "create and delete a pending CREATE tombstone when both probes are missing" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val recreatedTask =
                Task().apply {
                    id = "server-task-id"
                    alias = task.alias
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("local-task-id", authenticatedUserID) } returns Unit
            coEvery {
                apiClient.createTask(task, true, authenticatedUserID, expectedServerOrigin)
            } returns recreatedTask
            coEvery {
                apiClient.deleteTask(
                    "server-task-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns true

            repository.syncPendingTaskCreations(
                authenticatedUserID,
                expectedServerOrigin,
            ) shouldBe true

            coVerifyOrder {
                apiClient.createTask(task, true, authenticatedUserID, expectedServerOrigin)
                apiClient.deleteTask(
                    "server-task-id",
                    true,
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            }
            verify(exactly = 1) {
                localRepository.deleteTask("local-task-id", authenticatedUserID)
            }
        }

        "retain a deleted pending CREATE after ambiguous lost-response recovery" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "local-task-id"
                    alias = offlineCreateAlias("local-task-id")
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingCreate = true
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery {
                apiClient.createTask(task, true, authenticatedUserID, expectedServerOrigin)
            } returns null

            repository.syncPendingTaskCreations(
                authenticatedUserID,
                expectedServerOrigin,
            ) shouldBe false

            task.pendingDelete shouldBe true
            verify(exactly = 0) { localRepository.deleteTask(any(), any()) }
        }

        "not delete when the direct-ID preflight returns another task" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "todo-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            val collidingTask =
                Task().apply {
                    id = "other-task-id"
                    alias = task.id
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            coEvery {
                apiClient.getTask("todo-id", true, authenticatedUserID, expectedServerOrigin)
            } returns collidingTask
            coEvery {
                apiClient.getTaskServerState(
                    "todo-id",
                    authenticatedUserID,
                    expectedServerOrigin,
                )
            } returns TaskServerState.UNKNOWN

            repository.syncPendingTaskCreations(authenticatedUserID) shouldBe false

            coVerify(exactly = 0) { apiClient.deleteTask(any(), any(), any(), any()) }
            verify(exactly = 0) { localRepository.deleteTask(any(), any()) }
        }

        "remove a legacy local task after the server confirms it is missing" {
            authenticatedUserID = "test-user"
            val task =
                Task().apply {
                    id = "legacy-task-id"
                    ownerID = authenticatedUserID
                    type = TaskType.TODO
                    pendingDelete = true
                    outboxServerOrigin = expectedServerOrigin
                }
            every { localRepository.getPendingTaskDeletions(authenticatedUserID) } returns
                flowOf(listOf(task))
            every { localRepository.deleteTask("legacy-task-id", authenticatedUserID) } returns Unit
            coEvery { apiClient.deleteTask("legacy-task-id", true, any(), any()) } returns false
            coEvery { apiClient.getTaskServerState("legacy-task-id", any(), any()) } returns
                TaskServerState.MISSING

            repository.syncPendingTaskCreations() shouldBe true

            verify(exactly = 1) { localRepository.deleteTask("legacy-task-id", authenticatedUserID) }
        }
    }
})
