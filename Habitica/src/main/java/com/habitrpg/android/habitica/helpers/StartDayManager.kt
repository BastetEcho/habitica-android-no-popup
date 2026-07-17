package com.habitrpg.android.habitica.helpers

import com.habitrpg.android.habitica.data.UserRepository
import com.habitrpg.common.habitica.helpers.ExceptionHandler
import kotlinx.coroutines.MainScope
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean

object StartDayManager {
    private val isRunning = AtomicBoolean(false)

    fun runIfNeeded(userRepository: UserRepository?) {
        if (userRepository == null || !isRunning.compareAndSet(false, true)) {
            return
        }

        MainScope().launch(ExceptionHandler.coroutine()) {
            try {
                if (!userRepository.isClosed && userRepository.getUser().firstOrNull()?.needsCron == true) {
                    userRepository.runCron()
                }
            } finally {
                isRunning.set(false)
            }
        }
    }
}
