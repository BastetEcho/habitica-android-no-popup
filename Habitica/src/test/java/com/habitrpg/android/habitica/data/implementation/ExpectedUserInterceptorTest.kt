package com.habitrpg.android.habitica.data.implementation

import com.habitrpg.android.habitica.data.TaskServerState
import com.habitrpg.android.habitica.models.tasks.Task
import com.habitrpg.common.habitica.api.HostConfig
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import okhttp3.Interceptor
import okhttp3.Request
import okhttp3.Response
import java.io.IOException

private const val EXPECTED_USER_HEADER_FOR_TEST = "X-Habitica-Expected-User"
private const val EXPECTED_SERVER_ORIGIN_HEADER_FOR_TEST = "X-Habitica-Expected-Server-Origin"

class ExpectedUserInterceptorTest : WordSpec({
    "expected-user application interceptor" should {
        "pin matching authentication and require network revalidation" {
            val hostConfig =
                HostConfig("https://example.com", "", "fake-api-key", "expected-user")
            val request =
                Request.Builder()
                    .url("https://example.com/api/v4/tasks/task-id")
                    .header(EXPECTED_USER_HEADER_FOR_TEST, "expected-user")
                    .header(EXPECTED_SERVER_ORIGIN_HEADER_FOR_TEST, hostConfig.serverOrigin())
                    .build()
            val chain = mockk<Interceptor.Chain>()
            val forwardedRequest = slot<Request>()
            val response = mockk<Response>()
            every { chain.request() } returns request
            every { chain.proceed(capture(forwardedRequest)) } returns response

            ExpectedUserInterceptor(hostConfig).intercept(chain) shouldBe response

            forwardedRequest.captured.header(EXPECTED_USER_HEADER_FOR_TEST) shouldBe null
            forwardedRequest.captured.header(EXPECTED_SERVER_ORIGIN_HEADER_FOR_TEST) shouldBe null
            forwardedRequest.captured.cacheControl.noCache shouldBe true
            forwardedRequest.captured.cacheControl.noStore shouldBe true
            forwardedRequest.captured.tag(OutboxAuthentication::class.java)?.snapshot?.userID shouldBe
                "expected-user"
            forwardedRequest.captured.tag(OutboxAuthentication::class.java)?.snapshot?.apiKey shouldBe
                "fake-api-key"
        }

        "reject a different current identity before proceeding" {
            val hostConfig =
                HostConfig("https://example.com", "", "fake-current-key", "current-user")
            val request =
                Request.Builder()
                    .url("https://example.com/api/v4/tasks/task-id")
                    .header(EXPECTED_USER_HEADER_FOR_TEST, "queued-user")
                    .header(EXPECTED_SERVER_ORIGIN_HEADER_FOR_TEST, hostConfig.serverOrigin())
                    .build()
            val chain = mockk<Interceptor.Chain>()
            every { chain.request() } returns request

            shouldThrow<IOException> {
                ExpectedUserInterceptor(hostConfig).intercept(chain)
            }

            verify(exactly = 0) { chain.proceed(any()) }
        }

        "reject queued work without a pinned server origin" {
            val hostConfig =
                HostConfig("https://example.com", "", "fake-api-key", "expected-user")
            val request =
                Request.Builder()
                    .url("https://example.com/api/v4/tasks/task-id")
                    .header(EXPECTED_USER_HEADER_FOR_TEST, "expected-user")
                    .build()
            val chain = mockk<Interceptor.Chain>()
            every { chain.request() } returns request

            shouldThrow<IOException> {
                ExpectedUserInterceptor(hostConfig).intercept(chain)
            }

            verify(exactly = 0) { chain.proceed(any()) }
        }

        "reject a request whose URL does not match the pinned server origin" {
            val hostConfig =
                HostConfig("https://example.com", "", "fake-api-key", "expected-user")
            val request =
                Request.Builder()
                    .url("https://other.example/api/v4/tasks/task-id")
                    .header(EXPECTED_USER_HEADER_FOR_TEST, "expected-user")
                    .header(EXPECTED_SERVER_ORIGIN_HEADER_FOR_TEST, hostConfig.serverOrigin())
                    .build()
            val chain = mockk<Interceptor.Chain>()
            every { chain.request() } returns request

            shouldThrow<IOException> {
                ExpectedUserInterceptor(hostConfig).intercept(chain)
            }

            verify(exactly = 0) { chain.proceed(any()) }
        }

        "leave ordinary requests unchanged" {
            val hostConfig = HostConfig("current-user", "fake-current-key")
            val request = Request.Builder().url("https://example.com/api/v3/user").build()
            val chain = mockk<Interceptor.Chain>()
            val forwardedRequest = slot<Request>()
            val response = mockk<Response>()
            every { chain.request() } returns request
            every { chain.proceed(capture(forwardedRequest)) } returns response

            ExpectedUserInterceptor(hostConfig).intercept(chain) shouldBe response

            forwardedRequest.captured shouldBe request
            forwardedRequest.captured.tag(OutboxAuthentication::class.java) shouldBe null
        }

        "reject side effects from a response after credentials change" {
            val hostConfig =
                HostConfig("https://example.com", "", "old-fake-key", "expected-user")
            val request =
                Request.Builder()
                    .url("https://example.com/api/v4/tasks/task-id")
                    .tag(
                        OutboxAuthentication::class.java,
                        OutboxAuthentication(hostConfig.authenticationSnapshot()),
                    )
                    .build()

            request.matchesCurrentOutboxAuthentication(hostConfig) shouldBe true
            hostConfig.updateAuthentication("new-user", "new-fake-key")
            request.matchesCurrentOutboxAuthentication(hostConfig) shouldBe false
        }

        "invalidate credentials atomically when the server origin changes" {
            val hostConfig =
                HostConfig("https://example.com", "", "old-fake-key", "expected-user")

            hostConfig.updateServerAddress("https://other.example")

            hostConfig.authenticationSnapshot().userID shouldBe ""
            hostConfig.authenticationSnapshot().apiKey shouldBe ""
            hostConfig.serverOrigin() shouldBe "https://other.example/api/v4/"
        }
    }

    "task server state classification" should {
        "treat a mismatched successful response as unknown" {
            val task =
                Task().apply {
                    id = "different-task-id"
                    alias = "different-alias"
                }

            taskServerStateForResponse("requested-task-id", task) shouldBe TaskServerState.UNKNOWN
            taskServerStateForResponse("android_requested-task-id", task) shouldBe
                TaskServerState.UNKNOWN
        }
    }
})
