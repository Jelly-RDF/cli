package eu.neverblink.jelly.cli

import eu.neverblink.jelly.cli.command.helpers.CleanUpAfterTest
import eu.neverblink.jelly.cli.command.rdf.RdfFromJelly
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ErrorHandlerSpec extends AnyWordSpec with Matchers with CleanUpAfterTest:
  "Error Handler" should {
    "handle nonexistent input file" in {
      val (out, err) =
        RdfFromJelly.runCommand(List("rdf", "from-jelly", "wrongFile.jelly"))
      err should include("File wrongFile.jelly does not exist")
    }

  }
