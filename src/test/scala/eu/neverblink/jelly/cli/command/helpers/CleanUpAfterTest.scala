package eu.neverblink.jelly.cli.command.helpers

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.wordspec.AnyWordSpec

trait CleanUpAfterTest extends BeforeAndAfterEach, BeforeAndAfterAll {
  this: AnyWordSpec =>
  override def afterEach(): Unit = {
    DataGenHelper.resetInputStream()
  }
  override def afterAll(): Unit = {
    DataGenHelper.cleanUpFiles()
  }
}
