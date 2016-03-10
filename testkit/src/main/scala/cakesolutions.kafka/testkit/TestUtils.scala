package cakesolutions.kafka.testkit

import java.io.File

import scala.util.Random

object TestUtils {
  private val random = new Random()

  def randomString(length: Int = 10): String =
    random.alphanumeric.take(length).mkString

  def constructTempDir(dirPrefix: String) = {
    val f = new File(System.getProperty("java.io.tmpdir"), dirPrefix + randomString())
    f.deleteOnExit()
    f
  }

  def deleteFile(path: File): Unit = {
    if (path.isDirectory)
      path.listFiles().foreach(deleteFile)

    path.delete()
  }
}
