package eu.neverblink.jelly.cli.util.io

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files
import scala.util.Using

class IoUtilSpec extends AnyWordSpec, Matchers:
  "IoUtil.outputStream" should {
    "truncate an existing file" in {
      val file = Files.createTempFile("jelly-cli", ".out")
      try {
        Files.write(file, "old content that is longer than the new one".getBytes)
        Using(IoUtil.outputStream(file.toString)) { os =>
          os.write("new".getBytes)
        }
        Files.readAllBytes(file) should be("new".getBytes)
      } finally file.toFile.delete()
    }

    "truncate an existing file even when nothing is written" in {
      val file = Files.createTempFile("jelly-cli", ".out")
      try {
        Files.write(file, "old content".getBytes)
        Using(IoUtil.outputStream(file.toString)) { _ => () }
        Files.size(file) should be(0)
      } finally file.toFile.delete()
    }

    "create a file that does not exist yet" in {
      val dir = Files.createTempDirectory("jelly-cli")
      val file = dir.resolve("new-file.out")
      try {
        Using(IoUtil.outputStream(file.toString)) { os =>
          os.write("content".getBytes)
        }
        Files.readAllBytes(file) should be("content".getBytes)
      } finally
        file.toFile.delete()
        dir.toFile.delete()
    }
  }
