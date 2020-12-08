import java.util.concurrent.CountDownLatch
import org.slf4j.Logger

import org.slf4j.LoggerFactory
import java.io.RandomAccessFile
import java.net.HttpURLConnection
import java.net.Proxy
import java.net.URL
import java.io.BufferedInputStream
import java.io.File
import java.io.IOException
import java.util.concurrent.Executors


class Downloader constructor(private var threadCount: Int, private var uri: String, private var localPath: String) {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(Downloader::class.java)
        private const val DEFAULT_BUFFER_SIZE: Int = 8096
    }

    private val lock = Any()
    private lateinit var latch: CountDownLatch
    private var file: RandomAccessFile? = null

    fun execute() {
        val url = URL(uri)
        val contentLength = getContentLength(url)
        createTempFile()

        val service = Executors.newFixedThreadPool(threadCount)
        latch = CountDownLatch(threadCount)
        val segmentLength: Long = contentLength / threadCount
        var currentPos: Long = 0
        var remainingLength: Long = contentLength
        for (i in 0 until threadCount) {
            service.execute(DownloadThread(url, currentPos, when (i) {
                in 1 until threadCount -> segmentLength + remainingLength
                else -> segmentLength
            }))
            remainingLength -= segmentLength
            currentPos += segmentLength
        }
        latch.await()
        file?.close()
        service.shutdown()

        renameTempFile()
    }

    /**
     * Rename temp file to final name, i.e. remove the suffix ".download".
     */
    private fun renameTempFile() {
        var f = File("$localPath.download")
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
        val f = File("$localPath.download")

        // Create a new temp file if not yet exists
        if (!f.exists()) {
            try {
                f.createNewFile()
            } catch (e: IOException) {
                LOGGER.error("Failed to create temp file", e)
                return ""
            }
        }

        file = RandomAccessFile(f, "rw")

        return f.absolutePath
    }

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

    inner class DownloadThread constructor(
        private var url: URL,
        private var startPos: Long,
        private var length: Long
    ) : Runnable {

        override fun run() {
            val buf = ByteArray(DEFAULT_BUFFER_SIZE)

            try {
                val urlConnection: HttpURLConnection = url.openConnection(Proxy.NO_PROXY) as HttpURLConnection
                urlConnection.setRequestProperty("Range", "bytes=" + startPos + "-" + (startPos + length))
                val bis = BufferedInputStream(urlConnection.inputStream)
                var len: Int
                var offset: Long = startPos

                while (bis.readNBytes(buf, 0, DEFAULT_BUFFER_SIZE).let {  // while # of bytes read != -1
                        len = it
                        it > 0
                    }) {
                    synchronized(lock) {
                        file?.seek(offset)
                        file?.write(buf, 0, len)
                        offset += len
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
