package model

data class DownloadProgress(
    val contentLength: Long,
    var completion: Long,
    var totalProgress: String,
    val threadCount: Int,
    var threads: Array<ThreadProgress?>
)
