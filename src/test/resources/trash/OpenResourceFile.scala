import io.qross.util.OpenResourceFile

private lazy val lines: Iterator[String] = source.getLines()

def foreach(callback: (String) => Unit): OpenResourceFile = {
    lines.foreach(line => callback(line))
    this
}