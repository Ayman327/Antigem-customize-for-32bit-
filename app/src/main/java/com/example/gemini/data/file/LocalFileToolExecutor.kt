package com.example.gemini.data.file

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.security.MessageDigest

/**
 * Local file tools for Direct Cloud Mode — the same hash-locked read/write/edit
 * contract as [FileToolExecutor], but with direct on-device File I/O instead of
 * SSH. The SSH path in [FileToolExecutor] is untouched.
 *
 * Why a separate executor: [FileToolExecutor] executes every method over SSH
 * (TermuxSshManager) and needs host/port/user/pass. In Direct Cloud Mode there
 * is no server to SSH into — the files live in the Termux home directory on
 * this phone — so these methods work on local files directly.
 *
 * Safety: every path is resolved inside [homeDir] (the Termux home directory,
 * `filesDir/home`). Absolute paths and `..` segments that would escape it are
 * rejected. The hash lock is kept exactly like [FileToolExecutor]: a write or
 * edit needs the MD5 hash from the last read of the same file, so the model
 * cannot silently overwrite a file that changed since it was read.
 */
object LocalFileToolExecutor {

    private const val TAG = "LocalFileTools"

    /** Largest file content read into memory; larger files are reported truncated. */
    private const val MAX_READ_CHARS = 1_000_000

    /** Largest directory listing returned. */
    private const val MAX_LIST_ENTRIES = 500

    /**
     * Resolve [path] inside [homeDir]. `~` means the home directory itself;
     * relative paths resolve under it; absolute paths must already be inside
     * it. Anything escaping the home directory is rejected.
     */
    fun resolveInHome(path: String, homeDir: File): File {
        val trimmed = path.trim()
        val base = when {
            trimmed == "~" || trimmed.startsWith("~/") ->
                File(homeDir, trimmed.removePrefix("~").removePrefix("/"))
            trimmed.startsWith("/") -> File(trimmed)
            else -> File(homeDir, trimmed)
        }
        val homeCanonical = homeDir.canonicalFile
        val target = base.canonicalFile
        require(target == homeCanonical || target.path.startsWith(homeCanonical.path + File.separator)) {
            "Path is outside the workspace home directory: $path"
        }
        return target
    }

    private fun md5Hex(bytes: ByteArray): String {
        val digest = MessageDigest.getInstance("MD5").digest(bytes)
        return digest.joinToString("") { "%02x".format(it) }
    }

    /** MD5 hex of a file, or null when it does not exist / cannot be read. */
    private fun md5Hex(file: File): String? {
        if (!file.isFile) return null
        return try {
            val digest = MessageDigest.getInstance("MD5")
            file.inputStream().use { input ->
                val buffer = ByteArray(8192)
                var read: Int
                while (input.read(buffer).also { read = it } != -1) {
                    digest.update(buffer, 0, read)
                }
            }
            digest.digest().joinToString("") { "%02x".format(it) }
        } catch (_: Exception) {
            null
        }
    }

    /**
     * Read a file (or line range) from the home directory.
     * Returns content with line numbers prepended + MD5 hash of the FULL file.
     * The hash must be sent back with any write/edit call.
     */
    suspend fun readFile(
        path: String,
        homeDir: File,
        startLine: Int? = null,
        endLine: Int? = null
    ): Result<FileToolExecutor.ReadResult> = withContext(Dispatchers.IO) {
        try {
            Log.d(TAG, "[read_file] path=$path")

            val file = resolveInHome(path, homeDir)
            val hash = md5Hex(file)
                ?: return@withContext Result.failure(Exception("File not found or unreadable: $path"))
            val fullText = String(file.readBytes(), Charsets.UTF_8)
            val truncated = fullText.length > MAX_READ_CHARS
            val visibleText = if (truncated) fullText.take(MAX_READ_CHARS) else fullText
            // Line count like `wc -l` (newline count), not Kotlin's lines().size.
            val totalLines = fullText.count { it == '\n' }
            val allLines = visibleText.lines()

            val effectiveStart = startLine ?: 1
            val selected = when {
                startLine != null && endLine != null ->
                    allLines.subList(
                        (startLine - 1).coerceAtLeast(0),
                        endLine.coerceAtMost(allLines.size)
                    )
                startLine != null -> allLines.drop((startLine - 1).coerceAtLeast(0))
                endLine != null -> allLines.take(endLine.coerceAtLeast(0))
                else -> allLines
            }
            val numberedContent = selected.mapIndexed { idx, line ->
                "${effectiveStart + idx}: $line"
            }.joinToString("\n")

            Log.d(TAG, "[read_file] SUCCESS path=$path hash=$hash lines=$totalLines truncated=$truncated")
            Result.success(
                FileToolExecutor.ReadResult(
                    path = path,
                    content = numberedContent,
                    hash = hash,
                    totalLines = totalLines,
                    truncated = truncated
                )
            )
        } catch (e: Exception) {
            Log.e(TAG, "[read_file] Exception: ${e.message}", e)
            Result.failure(e)
        }
    }

    /**
     * Overwrite a file completely. Requires expectedHash from a prior read_file call.
     * Pass expectedHash = "NEW" for new files that don't exist yet. A missing file
     * is written the same way [FileToolExecutor] writes it (its hash check is
     * skipped when the file does not exist yet).
     */
    suspend fun writeFile(
        path: String,
        content: String,
        expectedHash: String,
        homeDir: File
    ): Result<FileToolExecutor.WriteResult> = withContext(Dispatchers.IO) {
        try {
            Log.d(TAG, "[write_file] path=$path expectedHash=$expectedHash")

            val file = resolveInHome(path, homeDir)
            if (expectedHash.uppercase() != "NEW") {
                val currentHash = md5Hex(file)
                if (currentHash != null && currentHash != expectedHash) {
                    Log.w(TAG, "[write_file] HASH MISMATCH expected=$expectedHash current=$currentHash")
                    return@withContext Result.failure(
                        Exception(
                            "WRITE REJECTED: File has changed since your last read.\n" +
                                "Expected hash: $expectedHash\n" +
                                "Current hash:  $currentHash\n" +
                                "Please use read_file again to get the latest version before writing."
                        )
                    )
                }
            }

            file.parentFile?.mkdirs()
            val bytes = content.toByteArray(Charsets.UTF_8)
            file.writeBytes(bytes)
            val newHash = md5Hex(file) ?: md5Hex(bytes)

            Log.d(TAG, "[write_file] SUCCESS path=$path newHash=$newHash bytes=${bytes.size}")
            Result.success(
                FileToolExecutor.WriteResult(
                    path = path,
                    bytesWritten = bytes.size,
                    newHash = newHash
                )
            )
        } catch (e: Exception) {
            Log.e(TAG, "[write_file] Exception: ${e.message}", e)
            Result.failure(e)
        }
    }

    /**
     * Search-and-replace edit — safest way for AI to make partial edits.
     * No line numbers: the AI provides the exact string to find and replacement.
     * If old_str is not found -> file changed -> AI must re-read.
     */
    suspend fun editFile(
        path: String,
        oldStr: String,
        newStr: String,
        expectedHash: String,
        homeDir: File,
        replaceAll: Boolean = false
    ): Result<FileToolExecutor.EditResult> = withContext(Dispatchers.IO) {
        try {
            Log.d(TAG, "[edit_file] path=$path expectedHash=$expectedHash replaceAll=$replaceAll")

            val file = resolveInHome(path, homeDir)
            val currentHash = md5Hex(file)
                ?: return@withContext Result.failure(Exception("File not found: $path"))
            if (currentHash != expectedHash) {
                Log.w(TAG, "[edit_file] HASH MISMATCH expected=$expectedHash current=$currentHash")
                return@withContext Result.failure(
                    Exception(
                        "EDIT REJECTED: File has changed since your last read.\n" +
                            "Expected hash: $expectedHash\n" +
                            "Current hash:  $currentHash\n" +
                            "Please use read_file again before editing."
                    )
                )
            }

            val currentContent = String(file.readBytes(), Charsets.UTF_8)
            if (!currentContent.contains(oldStr)) {
                Log.w(TAG, "[edit_file] old_str NOT FOUND in $path")
                return@withContext Result.success(
                    FileToolExecutor.EditResult(path = path, replaced = false)
                )
            }

            val newContent =
                if (replaceAll) currentContent.replace(oldStr, newStr)
                else currentContent.replaceFirst(oldStr, newStr)
            file.writeBytes(newContent.toByteArray(Charsets.UTF_8))
            val newHash = md5Hex(file) ?: ""

            Log.d(TAG, "[edit_file] SUCCESS path=$path newHash=$newHash")
            Result.success(
                FileToolExecutor.EditResult(path = path, replaced = true, newHash = newHash)
            )
        } catch (e: Exception) {
            Log.e(TAG, "[edit_file] Exception: ${e.message}", e)
            Result.failure(e)
        }
    }

    /**
     * List a directory inside the home directory. Returns a plain-text listing
     * (directories first, then files, alphabetical) for the model to read.
     */
    suspend fun listDir(path: String, homeDir: File): Result<String> =
        withContext(Dispatchers.IO) {
            try {
                val dir = resolveInHome(path, homeDir)
                if (!dir.isDirectory) {
                    return@withContext Result.failure(Exception("Not a directory: $path"))
                }
                val entries = dir.listFiles()
                    ?.sortedWith(compareBy({ !it.isDirectory }, { it.name.lowercase() }))
                    ?: emptyList()
                val shown = entries.take(MAX_LIST_ENTRIES)
                val out = buildString {
                    append("Directory: $path\n")
                    append("Entries: ${entries.size}\n")
                    for (entry in shown) {
                        if (entry.isDirectory) append("[DIR] ${entry.name}/\n")
                        else append("${entry.name} (${entry.length()} bytes)\n")
                    }
                    if (entries.size > shown.size) {
                        append("... and ${entries.size - shown.size} more entries\n")
                    }
                }
                Result.success(out.trimEnd())
            } catch (e: Exception) {
                Log.e(TAG, "[list_dir] Exception: ${e.message}", e)
                Result.failure(e)
            }
        }
}
