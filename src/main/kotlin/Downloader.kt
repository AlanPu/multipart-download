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

    private val lock = Any()
    private lateinit var latch: CountDownLatch
    private var targetFile: RandomAccessFile? = null
    private var completion: Long = 0
    private var downloadProgress: DownloadProgress? = null
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
        var isFromResume: Boolean

        // If progress file exists, read the meta data from file
        if (File(progressFilePath).exists()) {
            BufferedReader(FileReader(progressFilePath)).let {
                val json = it.readLine()
                gson.fromJson(json, DownloadProgress::class.java).let {
                    contentLength = it.contentLength
                    threadCount = it.threadCount
                    threadProgress = it.threads
                    completion = it.completion
                    totalProgress = it.totalProgress
                    remainingLength = contentLength - completion
                }
                it.close()
            }
            isFromResume = true
        } else {
            contentLength = getContentLength(url)
            threadProgress = arrayOfNulls(threadCount)
            createTempFile()
            remainingLength = contentLength
            isFromResume = false
        }

        downloadProgress = DownloadProgress(contentLength, completion, totalProgress, threadCount, threadProgress)

        progressFile = RandomAccessFile(progressFilePath, "rw")

        latch = CountDownLatch(threadCount)
        val service = Executors.newFixedThreadPool(threadCount + 1)
        val segmentLength: Long = contentLength / threadCount
        var currentPos: Long = 0

        // Start a thread to update progress every 1 second
        downloadProgress?.let {
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
        renameTempFile()

        // If download completed, delete progress log file
        // Otherwise, i.e. download is explicitly stopped, keep the file
        if (completion == contentLength) {
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
     * Create a temp file with name ends with ".download".
     */
    private fun createTempFile(): String {
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

        targetFile = RandomAccessFile(f, "rw")

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
                val progress =
                    String.format("%.2f%%", ((completion.toFloat() / downloadProgress.contentLength) * 100))
                downloadProgress.totalProgress = progress
                downloadProgress.completion = completion
                progressFile?.apply {
                    seek(0)
                    writeBytes(gson.toJson(downloadProgress))
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

            try {
                val urlConnection: HttpURLConnection = url.openConnection(Proxy.NO_PROXY) as HttpURLConnection
                urlConnection.setRequestProperty("Range", "bytes=" + startPos + "-" + (startPos + length))
                val bis = BufferedInputStream(urlConnection.inputStream)
                var len = 0
                var offset: Long = startPos

                while (!isStopped && (bis.readNBytes(buf, 0, DEFAULT_BUFFER_SIZE).let {  // while # of bytes read != -1
                        len = it
                        it > 0
                    })) {
                    synchronized(lock) {
                        targetFile?.seek(offset)
                        targetFile?.write(buf, 0, len)
                        offset += len
                        downloadProgress?.threads?.get(index)?.currentPos = offset
                        completion += len
                    }
                }
                bis.close()
                urlConnection.disconnect()
                latch.countDown()
            } catch (e: Exception) {
                LOGGER.error("Download failed", e)
            }
        }

    }
}
