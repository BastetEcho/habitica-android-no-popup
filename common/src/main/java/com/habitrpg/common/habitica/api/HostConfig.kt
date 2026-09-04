package com.habitrpg.common.habitica.api

import android.content.Context
import android.content.SharedPreferences
import android.os.Build
import androidx.core.content.edit
import com.habitrpg.common.habitica.BuildConfig
import com.habitrpg.common.habitica.helpers.KeyHelper
import java.net.URI
import java.util.Locale

/** Returns one credential-free, normalized API base URL for outbox identity checks. */
fun normalizeServerOrigin(address: String): String? {
    return runCatching {
        val uri = URI(Server(address.trim()).toString()).normalize()
        val scheme = uri.scheme?.lowercase(Locale.US) ?: return null
        val host = uri.host?.lowercase(Locale.US) ?: return null
        val normalizedPort =
            uri.port.takeUnless { port ->
                port == -1 || (scheme == "http" && port == 80) ||
                    (scheme == "https" && port == 443)
            }
        val normalizedPath =
            uri.rawPath.orEmpty().ifBlank { "/" }.let { path ->
                if (path.endsWith('/')) path else "$path/"
            }
        buildString {
            append(scheme)
            append("://")
            if (host.contains(':')) append('[')
            append(host)
            if (host.contains(':')) append(']')
            normalizedPort?.let { append(":$it") }
            append(normalizedPath)
        }
    }.getOrNull()
}

class HostConfig {
    private val authenticationLock = Any()
    private lateinit var serverAddress: String

    var address: String
        get() = synchronized(authenticationLock) { serverAddress }
        private set(value) {
            synchronized(authenticationLock) { serverAddress = value }
        }
    var port: String
    var apiKey: String = ""
        private set
    var userID: String = ""
        private set

    constructor(userID: String, apiKey: String) {
        this.port = BuildConfig.PORT
        this.address = BuildConfig.BASE_URL
        this.userID = userID
        this.apiKey = apiKey
    }

    constructor(sharedPreferences: SharedPreferences, keyHelper: KeyHelper?, context: Context) {
        this.port = BuildConfig.PORT
        val address = sharedPreferences.getString("server_url", null)
        val addressValid = address.isNullOrBlank().not()
        if (BuildConfig.DEBUG) {
            this.address = if (addressValid) address else BuildConfig.BASE_URL
            if (BuildConfig.TEST_USER_ID.isNotBlank()) {
                userID = BuildConfig.TEST_USER_ID
                apiKey = BuildConfig.TEST_USER_KEY
                return
            }
        } else {
            if (addressValid) {
                this.address = address
            } else {
                this.address = context.getString(com.habitrpg.common.habitica.R.string.base_url)
            }
        }
        this.userID = sharedPreferences.getString(context.getString(com.habitrpg.common.habitica.R.string.SP_userID), null) ?: ""
        this.apiKey = loadAPIKey(sharedPreferences, keyHelper)
    }

    private fun loadAPIKey(
        sharedPreferences: SharedPreferences,
        keyHelper: KeyHelper?
    ): String {
        return if (sharedPreferences.contains(userID)) {
            val encryptedKey = sharedPreferences.getString(userID, null)
            if (encryptedKey?.isNotBlank() == true) {
                keyHelper?.decrypt(encryptedKey)
            } else {
                ""
            }
        } else {
            val key = sharedPreferences.getString("APIToken", null)
            if (key?.isNotBlank() == true) {
                val encryptedKey = keyHelper?.encrypt(key)
                sharedPreferences.edit {
                    putString(userID, encryptedKey)
                    remove("APIToken")
                }
            }
            key
        } ?: ""
    }

    constructor(address: String, port: String, api: String, user: String) {
        this.address = address
        this.port = port
        this.apiKey = api
        this.userID = user
    }

    fun hasAuthentication(): Boolean {
        val authentication = authenticationSnapshot()
        return authentication.userID.isNotEmpty() && authentication.apiKey.isNotEmpty()
    }

    /** Replaces the user and API key as one atomic authentication identity. */
    fun updateAuthentication(
        userID: String,
        apiKey: String,
    ) {
        synchronized(authenticationLock) {
            this.userID = userID
            this.apiKey = apiKey
        }
    }

    /** Atomically changes API origin and invalidates credentials that belong to the old server. */
    fun updateServerAddress(address: String) {
        synchronized(authenticationLock) {
            val oldOrigin = normalizeServerOrigin(serverAddress)
            val newOrigin = normalizeServerOrigin(address)
            if (oldOrigin != newOrigin) {
                userID = ""
                apiKey = ""
            }
            serverAddress = address
        }
    }

    /** Returns a mutually consistent user and API-key snapshot. */
    fun authenticationSnapshot(): AuthenticationSnapshot {
        return synchronized(authenticationLock) {
            AuthenticationSnapshot(userID, apiKey, normalizeServerOrigin(serverAddress).orEmpty())
        }
    }

    /** Returns the credential-free API origin used by the current server configuration. */
    fun serverOrigin(): String =
        synchronized(authenticationLock) { normalizeServerOrigin(serverAddress).orEmpty() }
}

data class AuthenticationSnapshot(
    val userID: String,
    val apiKey: String,
    val serverOrigin: String,
)
