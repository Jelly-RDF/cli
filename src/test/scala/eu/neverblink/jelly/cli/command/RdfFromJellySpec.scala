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

  protected val dHelper: DataGenHelper = DataGenHelper("testRdfFromJelly")

  "rdf from-jelly command" should {
    "handle conversion of Jelly to NTriples" when {
      "a file to output stream" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val nQuadString = dHelper.generateNQuadString(3)
        val (out, err) =
          RdfFromJelly.runTestCommand(List("rdf", "from-jelly", jellyFile))
        val sortedOut = out.split("\n").map(_.trim).sorted
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
      }

      "input stream to output stream" in {
        val input = dHelper.generateJellyInputStream(3)
        RdfFromJelly.setStdIn(input)
        val nQuadString = dHelper.generateNQuadString(3)
        val (out, err) = RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", "--out-format", RdfFormatOption.NQuads.cliOptions.head),
        )
        val sortedOut = out.split("\n").map(_.trim).sorted
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
      }
      "a file to file" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val nQuadString = dHelper.generateNQuadString(3)
        val outputFile = dHelper.generateFile(RDFLanguages.NQUADS)
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
        val input = dHelper.generateJellyInputStream(3)
        RdfFromJelly.setStdIn(input)
        val outputFile = dHelper.generateFile(RDFLanguages.NQUADS)
        val nQuadString = dHelper.generateNQuadString(3)
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
      "a file to output stream" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val (out, err) =
          RdfFromJelly.runTestCommand(
            List(
              "rdf",
              "from-jelly",
              jellyFile,
              "--out-format",
              RdfFormatOption.JellyText.cliOptions.head,
            ),
          )
        val outString = """# Frame 0
                          |rows {
                          |  options {
                          |    stream_name: ""
                          |    physical_type: PHYSICAL_STREAM_TYPE_TRIPLES
                          |    generalized_statements: true
                          |    rdf_star: true
                          |    max_name_table_size: 128
                          |    max_prefix_table_size: 16
                          |    max_datatype_table_size: 16
                          |    logical_type: LOGICAL_STREAM_TYPE_FLAT_TRIPLES
                          |    version: 1
                          |  }
                          |}""".stripMargin
        out should include(outString)
        "rows".r.findAllIn(out).length should be(10)
        "http://example.org/predicate/".r.findAllIn(out).length should be(1)
      }
    }
    "throw proper exception" when {
      "input file is not found" in {
        val nonExist = "non-existing-file"
        val exception =
          intercept[ExitException] {
            RdfFromJelly.runTestCommand(List("rdf", "from-jelly", nonExist))
          }
        val msg = InputFileNotFound(nonExist).getMessage
        RdfFromJelly.getErrString should include(msg)
        exception.code should be(1)
      }
      "input file is not accessible" in {
        val jellyFile = dHelper.generateJellyFile(3)
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
        RdfFromJelly.getErrString should include(msg)
        exception.code should be(1)
      }
      "output file cannot be created" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val unreachableDir = dHelper.makeTestDir()
        Paths.get(unreachableDir).toFile.setWritable(false)
        val quadFile = dHelper.generateFile()
        val exception =
          intercept[ExitException] {

            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", jellyFile, "--to", quadFile),
            )
          }
        val msg = OutputFileCannotBeCreated(quadFile).getMessage
        Paths.get(unreachableDir).toFile.setWritable(true)
        RdfFromJelly.getErrString should include(msg)
        exception.code should be(1)
      }
      "deserializing error occurs" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val quadFile = dHelper.generateFile()
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
        val errContent = RdfFromJelly.getErrString
        errContent should include(msg)
        errContent should include("Run with --debug to see the complete stack trace.")
        exception.code should be(1)
      }
      "parsing error occurs with debug set" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val quadFile = dHelper.generateFile()
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
        val errContent = RdfFromJelly.getErrString
        errContent should include(msg)
        errContent should include("eu.neverblink.jelly.cli.InvalidJellyFile")
        exception.code should be(1)
      }
      "invalid output format supplied" in {
        val jellyFile = dHelper.generateJellyFile(3)
        val quadFile = dHelper.generateFile()
        val exception =
          intercept[ExitException] {
            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", jellyFile, "--to", quadFile, "--out-format", "invalid"),
            )
          }
        val msg = InvalidFormatSpecified("invalid", RdfFromJellyPrint.validFormatsString)
        RdfFromJelly.getErrString should include(msg.getMessage)
        exception.code should be(1)
      }
    }
  }
