import io.qross.util.OpenResourceFile

val TEMPLATES: Map[String, String] = Map[String, String](
    "NEW" -> OpenResourceFile("/templates/new.html").toString(),
    "READY" -> OpenResourceFile("/templates/ready.html").toString(),
    "BEATS" -> OpenResourceFile("/templates/beats.html").toString(),
    "CHECKING_LIMIT" -> OpenResourceFile("/templates/checking_limit.html").toString(),
    "EXCEPTION" -> OpenResourceFile("/templates/exception.html").toString(),
    "FAILED" -> OpenResourceFile("/templates/failed.html").toString(),
    "INCORRECT" -> OpenResourceFile("/templates/incorrect.html").toString(),
    "SUCCESS" -> OpenResourceFile("/templates/success.html").toString(),
    "TIMEOUT" -> OpenResourceFile("/templates/timeout.html").toString()
)