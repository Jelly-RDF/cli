package eu.neverblink.jelly.cli.command

import eu.neverblink.jelly.cli.command.helpers.*
import eu.neverblink.jelly.cli.command.rdf.*
import org.apache.jena.riot.RDFLanguages
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.io.Source
import scala.util.Using

class RdfFromJellySpec extends AnyWordSpec with Matchers with CleanUpAfterTest:
  "rdf from-jelly command" should {
    "be able to convert a Jelly file to NTriples output stream" in {
      val jellyFile = DataGenHelper.generateJellyFile(3)
      val nQuadString = DataGenHelper.generateNQuadString(3)
      val options = RdfFromJellyOptions(inputFile = Some(jellyFile), outputFile = None)
      val (out, err) = RdfFromJelly.runTest(options)
      val sortedOut = out.split("\n").map(_.trim).sorted
      val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
      sortedOut should contain theSameElementsAs sortedQuads
    }
    "be able to convert a Jelly stream to NTriples output stream" in {
      val jellyStream = DataGenHelper.generateJellyInputStream(3)
      System.setIn(jellyStream)
      val nQuadString = DataGenHelper.generateNQuadString(3)
      val options = RdfFromJellyOptions(inputFile = None, outputFile = None)
      val (out, err) = RdfFromJelly.runTest(options)
      val sortedOut = out.split("\n").map(_.trim).sorted
      val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
      sortedOut should contain theSameElementsAs sortedQuads
    }
    "be able to convert a Jelly file to NTriples file" in {
      val jellyFile = DataGenHelper.generateJellyFile(3)
      val nQuadString = DataGenHelper.generateNQuadString(3)
      val outputFile = DataGenHelper.generateOutputFile(RDFLanguages.NQUADS)
      val options = RdfFromJellyOptions(inputFile = Some(jellyFile), outputFile = Some(outputFile))
      val (out, err) = RdfFromJelly.runTest(options)
      val sortedOut = Using.resource(Source.fromFile(outputFile)) { content =>
        content.getLines().toList.map(_.trim).sorted
      }
      val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
      sortedOut should contain theSameElementsAs sortedQuads
      out.length should be(0)
    }
    "be able to convert a Jelly stream to NTriples file" in {
      val jellyStream = DataGenHelper.generateJellyInputStream(3)
      val outputFile = DataGenHelper.generateOutputFile(RDFLanguages.NQUADS)
      val nQuadString = DataGenHelper.generateNQuadString(3)
      val options = RdfFromJellyOptions(inputFile = None, outputFile = Some(outputFile))
      val (out, err) = RdfFromJelly.runTest(options)
      val sortedOut = Using.resource(Source.fromFile(outputFile)) { content =>
        content.getLines().toList.map(_.trim).sorted
      }
      val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
      sortedOut should contain theSameElementsAs sortedQuads
      out.length should be(0)
    }
  }
