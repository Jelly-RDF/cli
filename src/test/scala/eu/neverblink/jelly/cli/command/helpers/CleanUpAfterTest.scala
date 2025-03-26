package eu.neverblink.jelly.cli.command.helpers

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.wordspec.AnyWordSpec

trait CleanUpAfterTest extends BeforeAndAfterEach, BeforeAndAfterAll {
  this: AnyWordSpec =>

  protected val dHelper: DataGenHelper

  override def beforeEach(): Unit = {
    dHelper.setInputStream(Array())
    dHelper.setOutputStream()
  }

  override def afterEach(): Unit = {
    dHelper.resetStreams()
  }
  override def afterAll(): Unit = {
    dHelper.cleanUpFiles()
  }
}
