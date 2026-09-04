package com.habitrpg.android.habitica

import android.content.SharedPreferences
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify

class HabiticaBaseApplicationTest : WordSpec({
    "logout preference persistence" should {
        "clear credentials with a synchronous commit while preserving app settings" {
            val preferences = mockk<SharedPreferences>()
            val editor = mockk<SharedPreferences.Editor>()
            every { preferences.all } returns
                mapOf(
                    "UserID" to "old-user",
                    "APIToken" to "old-token",
                )
            every { preferences.edit() } returns editor
            every { editor.clear() } returns editor
            every { editor.putString(any(), any()) } returns editor
            every { editor.putBoolean(any(), any()) } returns editor
            every { editor.commit() } returns true

            persistLogoutPreferences(
                preferences,
                "https://example.com",
                true,
                "18:30",
                "dark",
                "todos",
            ) shouldBe true

            verify(exactly = 1) { editor.clear() }
            verify(exactly = 1) { editor.putString("server_url", "https://example.com") }
            verify(exactly = 1) { editor.putBoolean("use_reminder", true) }
            verify(exactly = 1) { editor.putString("reminder_time", "18:30") }
            verify(exactly = 1) { editor.putString("theme_mode", "dark") }
            verify(exactly = 1) { editor.putString("launch_screen", "todos") }
            verify(exactly = 1) { editor.putBoolean("analytics_consent_given", false) }
            verify(exactly = 1) { editor.commit() }
            verify(exactly = 0) { editor.apply() }
        }

        "report a failed durable credential clear" {
            val preferences = mockk<SharedPreferences>()
            val logoutEditor = mockk<SharedPreferences.Editor>()
            val rollbackEditor = mockk<SharedPreferences.Editor>()
            every { preferences.all } returns
                mapOf(
                    "UserID" to "old-user",
                    "APIToken" to "old-token",
                    "unrelated_preference" to true,
                )
            every { preferences.edit() } returnsMany listOf(logoutEditor, rollbackEditor)
            every { logoutEditor.clear() } returns logoutEditor
            every { logoutEditor.putString(any(), any()) } returns logoutEditor
            every { logoutEditor.putBoolean(any(), any()) } returns logoutEditor
            every { logoutEditor.commit() } returns false
            every { rollbackEditor.clear() } returns rollbackEditor
            every { rollbackEditor.putString(any(), any()) } returns rollbackEditor
            every { rollbackEditor.putBoolean(any(), any()) } returns rollbackEditor
            every { rollbackEditor.commit() } returns true

            persistLogoutPreferences(
                preferences,
                null,
                false,
                "19:00",
                "system",
                "",
            ) shouldBe false

            verify(exactly = 1) { logoutEditor.commit() }
            verify(exactly = 0) { logoutEditor.apply() }
            verify(exactly = 1) { rollbackEditor.clear() }
            verify(exactly = 1) { rollbackEditor.putString("UserID", "old-user") }
            verify(exactly = 1) { rollbackEditor.putString("APIToken", "old-token") }
            verify(exactly = 1) { rollbackEditor.putBoolean("unrelated_preference", true) }
            verify(exactly = 1) { rollbackEditor.commit() }
            verify(exactly = 0) { rollbackEditor.apply() }
        }
    }
})
