package com.habitrpg.android.habitica.data.implementation

import android.content.Context
import com.habitrpg.android.habitica.data.ApiClient
import com.habitrpg.android.habitica.data.TaskRepository
import com.habitrpg.android.habitica.data.local.UserLocalRepository
import com.habitrpg.android.habitica.helpers.AppConfigManager
import com.habitrpg.android.habitica.models.tasks.TaskList
import com.habitrpg.android.habitica.models.user.Preferences
import com.habitrpg.android.habitica.models.user.User
import com.habitrpg.android.habitica.modules.AuthenticationHandler
import com.habitrpg.shared.habitica.models.tasks.TasksOrder
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import java.util.TimeZone
import java.util.concurrent.TimeUnit

/** Guards whole-user responses against Todo edits acknowledged before the main-thread save. */
@OptIn(ExperimentalCoroutinesApi::class)
class UserRepositoryTaskReadTest : WordSpec({
    "user response generation" should {
        "reject a response invalidated between API return and its dispatched Realm save" {
            runTest {
                Dispatchers.setMain(StandardTestDispatcher(testScheduler))
                try {
                    val fixture = UserTaskReadFixture()
                    val response = fixture.user(0)
                    coEvery { fixture.api.retrieveUser(true) } returns response
                    val request = async(start = CoroutineStart.UNDISPATCHED) {
                        fixture.repository.retrieveUser(true, true, false)
                    }
                    request.isCompleted shouldBe false
                    fixture.generation = 1

                    runCurrent()

                    request.await() shouldBe null
                    verify(exactly = 0) { fixture.local.saveUser(any(), any()) }
                    verify(exactly = 0) { fixture.tasks.saveTasks(any(), any(), any()) }
                } finally {
                    Dispatchers.resetMain()
                }
            }
        }

        "save current-generation user and tasks together" {
            runTest {
                Dispatchers.setMain(StandardTestDispatcher(testScheduler))
                try {
                    val fixture = UserTaskReadFixture()
                    fixture.generation = 3
                    val response = fixture.user(3)
                    coEvery { fixture.api.retrieveUser(true) } returns response

                    fixture.repository.retrieveUser(true, true, false) shouldBe response

                    verify(exactly = 1) { fixture.local.saveUser(response, any()) }
                    verify(exactly = 1) {
                        fixture.tasks.saveTasks("fixture-user", response.tasksOrder!!, response.tasks!!)
                    }
                } finally {
                    Dispatchers.resetMain()
                }
            }
        }

        "reject stale stats-only responses after an outbox acknowledgement" {
            runTest {
                Dispatchers.setMain(StandardTestDispatcher(testScheduler))
                try {
                    val fixture = UserTaskReadFixture()
                    fixture.generation = 2
                    coEvery { fixture.api.retrieveUser(false) } returns fixture.user(1)

                    fixture.repository.retrieveUser(false, true, false) shouldBe null

                    verify(exactly = 0) { fixture.local.saveUser(any(), any()) }
                    verify(exactly = 0) { fixture.tasks.saveTasks(any(), any(), any()) }
                } finally {
                    Dispatchers.resetMain()
                }
            }
        }
    }
})

/** Supplies fictional user responses and a generation counter without network or Realm files. */
private class UserTaskReadFixture {
    val local = mockk<UserLocalRepository>(relaxed = true)
    val api = mockk<ApiClient>(relaxed = true)
    val tasks = mockk<TaskRepository>(relaxed = true)
    var generation = 0L
    val repository: UserRepositoryImpl

    init {
        val authentication = mockk<AuthenticationHandler>(relaxed = true)
        every { authentication.currentUserID } returns "fixture-user"
        every { api.taskReadGeneration } answers { generation }
        repository = UserRepositoryImpl(
            local,
            api,
            authentication,
            tasks,
            mockk<AppConfigManager>(relaxed = true),
            mockk<Context>(relaxed = true),
        )
    }

    /** Creates an API fixture whose timezone already matches to avoid unrelated profile writes. */
    fun user(readGeneration: Long): User = User().apply {
        id = "fixture-user"
        taskReadGeneration = readGeneration
        tasksOrder = TasksOrder()
        tasks = TaskList().apply { this.readGeneration = readGeneration }
        preferences = Preferences().apply {
            timezoneOffset = -TimeUnit.MILLISECONDS.toMinutes(
                TimeZone.getDefault().getOffset(System.currentTimeMillis()).toLong(),
            ).toInt()
        }
    }
}
