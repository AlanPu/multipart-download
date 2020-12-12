import com.google.gson.Gson
import model.DownloadProgress
import model.ThreadProgress
import java.util.concurrent.CountDownLatch
import org.slf4j.Logger

import org.slf4j.LoggerFactory
import java.io.*
import java.net.HttpURLConnection
import java.net.Proxy
import java.net.URL
import java.util.concurrent.Executors


class Downloader constructor(private var threadCount: Int, private var uri: String, private var localPath: String) :
    Thread() {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(Downloader::class.java)
        private const val DEFAULT_BUFFER_SIZE: Int = 8096
        private val gson = Gson()
    }

    private val fileLock = Any()
    private val progressLock = Any()
    private lateinit var latch: CountDownLatch
    private var targetFile: RandomAccessFile? = null
    private lateinit var downloadProgress: DownloadProgress
    private var progressFile: RandomAccessFile? = null
    private val progressFilePath = "$localPath.dpg"
    private val tempDownloadFilePath = "$localPath.download"

    @Volatile
    var isStopped = false

    override fun run() {
        val url = URL(uri)
        var contentLength: Long
        lateinit var threadProgress: Array<ThreadProgress?>
        var totalProgress = ""
        var remainingLength: Long
        val isFromResume: Boolean

        // Delete it if target file exists already
        File(localPath).apply {
            if (this.exists()) {
                this.delete()
            }
        }

        var completion: Long = 0

        // If progress file exists, read the meta data from file
        if (File(progressFilePath).exists()) {
            BufferedReader(FileReader(progressFilePath)).let { reader ->
                val json = reader.readLine()
                gson.fromJson(json, DownloadProgress::class.java).let {
                    contentLength = it.contentLength
                    threadCount = it.threadCount
                    threadProgress = it.threads
                    totalProgress = it.totalProgress
                    remainingLength = contentLength - it.completion
                    completion = it.completion
                }
                reader.close()
            }
            isFromResume = true
        } else {  // Otherwise initialize with initial values
            contentLength = getContentLength(url)
            threadProgress = arrayOfNulls(threadCount)
            createTempFile(contentLength)
            remainingLength = contentLength
            isFromResume = false
        }

        targetFile = RandomAccessFile(tempDownloadFilePath, "rw")
        downloadProgress = DownloadProgress(contentLength, completion, totalProgress, threadCount, threadProgress)

        progressFile = RandomAccessFile(progressFilePath, "rw")

        latch = CountDownLatch(threadCount)
        val service = Executors.newFixedThreadPool(threadCount + 1)
        val segmentLength: Long = contentLength / threadCount
        var currentPos: Long = 0

        // Start a thread to update progress every 1 second
        downloadProgress.let {
            service.execute(ProgressUpdateThread(it))
        }

        // Start threads to download
        if (isFromResume) { // Resume the download progress
            for (i in 0 until threadCount) {
                val startPos = threadProgress[i]!!.currentPos
                val length = threadProgress[i]!!.endPos - startPos
                service.execute(DownloadThread(i, url, startPos, length))
            }
        } else {  // Start download from beginning
            for (i in 0 until threadCount) {
                val length =
                    when {
                        threadCount == 1 -> contentLength  // Single thread
                        i == threadCount - 1 -> segmentLength + remainingLength  // Last thread
                        else -> segmentLength
                    }
                val endPos = currentPos + length
                threadProgress[i] =
                    ThreadProgress(i, currentPos, if (endPos > contentLength) contentLength else endPos, currentPos)
                service.execute(DownloadThread(i, url, currentPos, length))
                remainingLength -= segmentLength
                currentPos += segmentLength
            }
        }

        // Wait for all download threads complete, then close resources
        latch.await()
        service.shutdown()
        targetFile?.close()
        sleep(1500)  // Wait for progress log file to complete
        isStopped = true

        // If download completed, rename temp file and delete progress log file
        // Otherwise, i.e. download is explicitly stopped, keep the files
        if (downloadProgress.completion == contentLength) {
            renameTempFile()
            File(progressFilePath).delete()
        }
    }

    /**
     * Rename temp file to final name, i.e. remove the suffix ".download".
     */
    private fun renameTempFile() {
        val f = File(tempDownloadFilePath)
        try {
            val s = f.renameTo(File(localPath))
            LOGGER.debug("Rename result: $s")
        } catch (e: Exception) {
            LOGGER.error("Failed to rename file", e)
        }
    }

    /**
     * Create a temp file with name ends with ".download", and pre-fill the file with "0x00" to target size.
     */
    private fun createTempFile(length: Long): String {
        val f = File(tempDownloadFilePath)

        // Create a new temp file if not yet exists
        if (!f.exists()) {
            try {
                f.createNewFile()
            } catch (e: IOException) {
                LOGGER.error("Failed to create temp file", e)
                return ""
            }
        }

        BufferedOutputStream(FileOutputStream(f)).let{
            var offset = 0L
            var len = length
            val bytes = ByteArray(DEFAULT_BUFFER_SIZE) { 0x00 }
            while (len > 0) {
                it.write(bytes, 0, when {
                    len > DEFAULT_BUFFER_SIZE -> DEFAULT_BUFFER_SIZE
                    else -> len.toInt()
                })
                offset += DEFAULT_BUFFER_SIZE
                len -= DEFAULT_BUFFER_SIZE
            }
            it.flush()
            it.close()
        }

        return f.absolutePath
    }

    /**
     * Get content length from HTTP header "Content-Length"
     */
    private fun getContentLength(url: URL): Long {
        val urlConnection = url.openConnection(Proxy.NO_PROXY) as HttpURLConnection
        urlConnection.requestMethod = "HEAD"
        val contentLength: Long = urlConnection.getHeaderField("Content-Length").toLong()
        if (LOGGER.isDebugEnabled) {
            LOGGER.debug("Content length: $contentLength bytes")
        }
        urlConnection.disconnect()
        return contentLength
    }

    /**
     * Progress printing thread.
     */
    inner class ProgressUpdateThread constructor(private val downloadProgress: DownloadProgress) : Runnable {
        override fun run() {
            while (!isStopped) {
                val progress: String
                synchronized(progressLock) {
                    progress =
                        String.format(
                            "%.2f%%",
                            ((downloadProgress.completion.toFloat() / downloadProgress.contentLength) * 100)
                        )
                    downloadProgress.totalProgress = progress
                    progressFile?.apply {
                        seek(0)
                        writeBytes(gson.toJson(downloadProgress))
                    }
                }
                val progressInfo = "$progress completed."
                print(progressInfo)

                sleep(1000)

                // Remove current printed progress from console
                for (i in progressInfo.indices) {
                    // Print "\b" will remove the last char printed in console
                    print("\b")
                }
            }

            // Close the file when download threads are stopped
            progressFile?.close()
        }
    }

    /**
     * Thread for download.
     */
    inner class DownloadThread constructor(
        private val index: Int,
        private val url: URL,
        private val startPos: Long,
        private val length: Long
    ) : Runnable {

        override fun run() {
            val buf = ByteArray(DEFAULT_BUFFER_SIZE)

            // Only resume the download if the resumed task has not yet completed
            if (length > 0) {
                try {
                    val urlConnection: HttpURLConnection = url.openConnection(Proxy.NO_PROXY) as HttpURLConnection
                    urlConnection.setRequestProperty("Range", "bytes=" + startPos + "-" + (startPos + length - 1))
                    val bis = BufferedInputStream(urlConnection.inputStream)
                    var len = 0
                    var offset: Long = startPos

                    while (!isStopped && (bis.readNBytes(buf, 0, DEFAULT_BUFFER_SIZE)
                            .let {  // while # of bytes read != -1
                                len = it
                                it > 0
                            })
                    ) {
                        synchronized(fileLock) {
                            targetFile?.seek(offset)
                            targetFile?.write(buf, 0, len)
                        }
                        offset += len
                        synchronized(progressLock) {
                            downloadProgress.threads[index]?.currentPos = offset
                            downloadProgress.completion += len
                        }
                    }
                    bis.close()
                    urlConnection.disconnect()
                } catch (e: Exception) {
                    LOGGER.error("Download failed", e)
                }
            }
            latch.countDown()
        }
    }
}
