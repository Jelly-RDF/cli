package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.command.helpers.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RdfFromJellySpec extends AnyWordSpec with Matchers with CleanUpFilesAfterTest:
  "rdf from-jelly command" should {
    "be able to convert a Jelly file to NTriples" in {
      val jellyFile = DataGenHelper.generateJellyFile(3)
    }
  }
