package eu.neverblink.jelly.cli.command

import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*

import eu.neverblink.jelly.cli.command.helpers.*
import eu.neverblink.jelly.cli.command.rdf.*
import org.apache.jena.riot.RDFLanguages
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using

class RdfFromJellySpec extends AnyWordSpec with Matchers with CleanUpAfterTest:

  "rdf from-jelly command" should {
    "handle conversion of Jelly to NTriples" when {
      "a file to output stream" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val nQuadString = DataGenHelper.generateNQuadString(3)
        val (out, err) =
          RdfFromJelly.runTestCommand(List("rdf", "from-jelly", jellyFile))
        val sortedOut = out.split("\n").map(_.trim).sorted
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
      }

      "input stream to output stream" in {
        DataGenHelper.generateJellyInputStream(3)
        val nQuadString = DataGenHelper.generateNQuadString(3)
        val (out, err) = RdfFromJelly.runTestCommand(List("rdf", "from-jelly"))
        val sortedOut = out.split("\n").map(_.trim).sorted
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
      }
      "a file to file" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val nQuadString = DataGenHelper.generateNQuadString(3)
        val outputFile = DataGenHelper.generateOutputFile(RDFLanguages.NQUADS)
        val (out, err) =
          RdfFromJelly.runTestCommand(
            List("rdf", "from-jelly", jellyFile, "--to", outputFile),
          )
        val sortedOut = Using.resource(Source.fromFile(outputFile)) { content =>
          content.getLines().toList.map(_.trim).sorted
        }
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
        out.length should be(0)
      }
      "an input stream to file" in {
        DataGenHelper.generateJellyInputStream(3)
        val outputFile = DataGenHelper.generateOutputFile(RDFLanguages.NQUADS)
        val nQuadString = DataGenHelper.generateNQuadString(3)
        val (out, err) =
          RdfFromJelly.runTestCommand(List("rdf", "from-jelly", "--to", outputFile))
        val sortedOut = Using.resource(Source.fromFile(outputFile)) { content =>
          content.getLines().toList.map(_.trim).sorted
        }
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
        out.length should be(0)
      }
    }
    "handle conversion of Jelly binary to text" when {
      "a file to file" in {}

    }
    "throw proper exception" when {
      "input file is not found" in {
        val nonExist = "non-existing-file"
        val exception =
          intercept[ExitException] {
            RdfFromJelly.runTestCommand(List("rdf", "from-jelly", nonExist))
          }
        val msg = InputFileNotFound(nonExist).getMessage
        RdfFromJelly.getErrContent should include(msg)
        exception.code should be(1)
      }
      "input file is not accessible" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val permissions = PosixFilePermissions.fromString("---------")
        Files.setPosixFilePermissions(
          Paths.get(jellyFile),
          permissions,
        )
        val exception =
          intercept[ExitException] {

            RdfFromJelly.runTestCommand(List("rdf", "from-jelly", jellyFile))
          }
        val msg = InputFileInaccessible(jellyFile).getMessage
        RdfFromJelly.getErrContent should include(msg)
        exception.code should be(1)
      }
      "output file cannot be created" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val unreachableDir = DataGenHelper.makeTestDir()
        Paths.get(unreachableDir).toFile.setWritable(false)
        val quadFile = DataGenHelper.generateOutputFile()
        val exception =
          intercept[ExitException] {

            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", jellyFile, "--to", quadFile),
            )
          }
        val msg = OutputFileCannotBeCreated(quadFile).getMessage
        RdfFromJelly.getErrContent should include(msg)
        exception.code should be(1)
      }
      "parsing error occurs" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val quadFile = DataGenHelper.generateOutputFile()
        RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", jellyFile, "--to", quadFile),
        )
        val exception =
          intercept[ExitException] {
            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", quadFile),
            )
          }
        val msg = InvalidJellyFile(new InvalidProtocolBufferException("")).getMessage
        val errContent = RdfFromJelly.getErrContent
        errContent should include(msg)
        errContent should include("Run with --debug to see the complete stack trace.")
        exception.code should be(1)
      }
      "parsing error occurs with debug set" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val quadFile = DataGenHelper.generateOutputFile()
        RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", jellyFile, "--to", quadFile),
        )
        val exception =
          intercept[ExitException] {
            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", quadFile, "--debug"),
            )
          }
        val msg = InvalidJellyFile(new InvalidProtocolBufferException("")).getMessage
        val errContent = RdfFromJelly.getErrContent
        errContent should include(msg)
        errContent should include("eu.neverblink.jelly.cli.InvalidJellyFile")
        exception.code should be(1)
      }
      "invalid output format supplied" in {
        val jellyFile = DataGenHelper.generateJellyFile(3)
        val quadFile = DataGenHelper.generateOutputFile()
        val exception =
          intercept[ExitException] {
            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", jellyFile, "--to", quadFile, "--out-format", "invalid"),
            )
          }
        val msg = InvalidFormatSpecified("invalid", RdfFromJelly.getCorrectOutputFormats)
        RdfFromJelly.getErrContent should include(msg.getMessage)
        exception.code should be(1)
      }
    }
  }
