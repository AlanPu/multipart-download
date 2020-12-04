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


class Downloader constructor(var threadCount: Int, var uri: String, var localPath: String) {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(Downloader::class.java)
        private const val DEFAULT_BUFFER_SIZE: Int = 8096
    }

    private val lock = Any()
    private lateinit var latch: CountDownLatch
    private var file: RandomAccessFile? = null

    fun execute() {
        val url = URL(uri)
        val contentLenght = getContentLength(url)
        createTempFile()
    }

    private fun createTempFile(): String {
        val f = File("$localPath.download");

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
        var url: URL,
        var startPos: Long,
        var length: Int,
        var localFile: RandomAccessFile
    ) : Runnable {

        override fun run() {
            val buf = ByteArray(DEFAULT_BUFFER_SIZE)

            try {
                val urlConnection: HttpURLConnection = url.openConnection(Proxy.NO_PROXY) as HttpURLConnection
                urlConnection.setRequestProperty("Range", "bytes=" + startPos + "-" + (startPos + length))
                val bis = BufferedInputStream(urlConnection.getInputStream())
                var len: Int = 0
                var offset: Int = 0

                while (bis.readNBytes(buf, offset, length).let {  // while # of bytes read != -1
                        len = it
                        it != -1
                    }) {
                    synchronized(lock) {
                        localFile.write(buf, startPos.toInt(), length)
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

fun main() {
    val downloader = Downloader(1, "https://web.alanpu.top:19443/content/trace1.data", "/Users/Alan/Desktop/test.txt").execute()
}