package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.command.helpers.DataGenHelper
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

trait CleanUpFilesAfterTest extends BeforeAndAfterEach {
  this: AnyWordSpec =>
  override def afterEach(): Unit = {
    DataGenHelper.cleanUpFile()
  }
}

class RDFFromJellySpec extends AnyWordSpec with Matchers with CleanUpFilesAfterTest:
  "rdf from-jelly command" should {
    "be able to convert a Jelly file to NTriples" in {
      val jellyFile = DataGenHelper.generateJellyFile(3)
    }
  }
