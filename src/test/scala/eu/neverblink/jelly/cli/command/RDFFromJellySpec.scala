package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.command.helpers.DataGenHelper
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RDFFromJellySpec extends AnyWordSpec, Matchers:
  "rdf-from-jelly command" should {
    "be able to convert a jelly stream to ntriples" in {
      val jellyStream = DataGenHelper.generateJelly(3)
    }
  }
