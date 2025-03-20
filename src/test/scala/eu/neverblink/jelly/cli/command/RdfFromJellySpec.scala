package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.command.helpers.*
import eu.neverblink.jelly.cli.command.rdf.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RdfFromJellySpec extends AnyWordSpec with Matchers with CleanUpAfterTest:
  "rdf from-jelly command" should {
    "be able to convert a Jelly file to NTriples" in {
      val jellyFile = DataGenHelper.generateJellyFile(3)
      val nQuadString = DataGenHelper.generateNQuadString(3)
      val options = RdfFromJellyOptions(inputFile = Some(jellyFile))
      val (out, err) = RdfFromJelly.runTest(options)
      val sortedOut = out.split("\n").map(_.trim).sorted
      val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
      sortedOut should contain theSameElementsAs sortedQuads
    }
    "be able to convert a Jelly stream to NTriples" in {
      val jellyStream = DataGenHelper.generateJellyInputStream(3)
      System.setIn(jellyStream)
      val nQuadString = DataGenHelper.generateNQuadString(3)
      val options = RdfFromJellyOptions(inputFile = None)
      val (out, err) = RdfFromJelly.runTest(options)
      val sortedOut = out.split("\n").map(_.trim).sorted
      val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
      sortedOut should contain theSameElementsAs sortedQuads
    }
  }
