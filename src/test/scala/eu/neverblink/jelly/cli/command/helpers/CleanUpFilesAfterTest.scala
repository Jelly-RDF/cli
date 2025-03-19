package eu.neverblink.jelly.cli.command.helpers

import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

trait CleanUpFilesAfterTest extends BeforeAndAfterEach {
  this: AnyWordSpec =>
  override def afterEach(): Unit = {
    DataGenHelper.cleanUpFile()
  }
}
