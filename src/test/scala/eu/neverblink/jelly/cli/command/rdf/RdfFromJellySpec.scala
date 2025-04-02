package eu.neverblink.jelly.cli.command.rdf

import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.helpers.*
import org.apache.jena.riot.RDFLanguages
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using

class RdfFromJellySpec extends AnyWordSpec with Matchers with TestFixtureHelper:

  protected val testCardinality: Integer = 33

  "rdf from-jelly command" should {
    "handle conversion of Jelly to NTriples" when {
      "a file to output stream" in withFullJellyFile { j =>
        val nQuadString = DataGenHelper.generateJenaString(testCardinality)
        val (out, err) =
          RdfFromJelly.runTestCommand(List("rdf", "from-jelly", j))
        val sortedOut = out.split("\n").map(_.trim).sorted
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
      }

      "input stream to output stream" in {
        val input = DataGenHelper.generateJellyInputStream(testCardinality)
        RdfFromJelly.setStdIn(input)
        val nQuadString = DataGenHelper.generateJenaString(testCardinality)
        val (out, err) = RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", "--out-format", RdfFormat.NQuads.cliOptions.head),
        )
        val sortedOut = out.split("\n").map(_.trim).sorted
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
      }
      "a file to file" in withFullJellyFile { j =>
        withEmptyJenaFile { q =>
          val nQuadString = DataGenHelper.generateJenaString(testCardinality)
          val (out, err) =
            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", j, "--to", q),
            )
          val sortedOut = Using.resource(Source.fromFile(q)) { content =>
            content.getLines().toList.map(_.trim).sorted
          }
          val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
          sortedOut should contain theSameElementsAs sortedQuads
          out.length should be(0)
        }
      }
      "a file to file when defaulting to nQuads" in withFullJellyFile { j =>
        withEmptyRandomFile { q =>
          val nQuadString = DataGenHelper.generateJenaString(testCardinality)
          val (out, err) =
            RdfFromJelly.runTestCommand(
              List("rdf", "from-jelly", j, "--to", q),
            )
          val sortedOut = Using.resource(Source.fromFile(q)) { content =>
            content.getLines().toList.map(_.trim).sorted
          }
          val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
          sortedOut should contain theSameElementsAs sortedQuads
          out.length should be(0)
        }
      }
      "an input stream to file" in withEmptyJenaFile { q =>
        val input = DataGenHelper.generateJellyInputStream(testCardinality)
        RdfFromJelly.setStdIn(input)
        val nQuadString = DataGenHelper.generateJenaString(testCardinality)
        val (out, err) =
          RdfFromJelly.runTestCommand(List("rdf", "from-jelly", "--to", q))
        val sortedOut = Using.resource(Source.fromFile(q)) { content =>
          content.getLines().toList.map(_.trim).sorted
        }
        val sortedQuads = nQuadString.split("\n").map(_.trim).sorted
        sortedOut should contain theSameElementsAs sortedQuads
        out.length should be(0)
      }
    }
    "handle conversion of Jelly binary to text" when {
      "a file to output stream" in withFullJellyFile { j =>
        val (out, err) =
          RdfFromJelly.runTestCommand(
            List(
              "rdf",
              "from-jelly",
              j,
              "--out-format",
              RdfFormat.JellyText.cliOptions.head,
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
        "rows".r.findAllIn(out).length should be(70)
        "http://example.org/predicate/".r.findAllIn(out).length should be(1)
      }
      "a file to file when inferred type" in withFullJellyFile { j =>
        withEmptyJellyTextFile { t =>
          val (out, err) =
            RdfFromJelly.runTestCommand(
              List(
                "rdf",
                "from-jelly",
                j,
                "--to",
                t,
              ),
            )
          val inTxt = Using.resource(Source.fromFile(t)) { content =>
            content.getLines().mkString("\n")
          }
          val outString =
            """# Frame 0
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
          inTxt should include(outString)
          "rows".r.findAllIn(inTxt).length should be(70)
          "http://example.org/predicate/".r.findAllIn(inTxt).length should be(1)
        }

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
      "input file is not accessible" in withFullJellyFile { j =>
        val permissions = PosixFilePermissions.fromString("---------")
        Files.setPosixFilePermissions(
          Paths.get(j),
          permissions,
        )
        val exception =
          intercept[ExitException] {

            RdfFromJelly.runTestCommand(List("rdf", "from-jelly", j))
          }
        val msg = InputFileInaccessible(j).getMessage
        RdfFromJelly.getErrString should include(msg)
        exception.code should be(1)
      }
      "output file cannot be created" in withFullJellyFile { j =>
        withEmptyJenaFile { q =>
          Paths.get(q).toFile.setWritable(false)
          val exception =
            intercept[ExitException] {

              RdfFromJelly.runTestCommand(
                List("rdf", "from-jelly", j, "--to", q),
              )
            }
          val msg = OutputFileCannotBeCreated(q).getMessage
          Paths.get(q).toFile.setWritable(true)
          RdfFromJelly.getErrString should include(msg)
          exception.code should be(1)

        }

      }
      "deserializing error occurs" in withFullJellyFile { j =>
        withEmptyJenaFile { q =>
          RdfFromJelly.runTestCommand(
            List("rdf", "from-jelly", j, "--to", q),
          )
          val exception =
            intercept[ExitException] {
              RdfFromJelly.runTestCommand(
                List("rdf", "from-jelly", q),
              )
            }
          val msg = InvalidJellyFile(new InvalidProtocolBufferException("")).getMessage
          val errContent = RdfFromJelly.getErrString
          errContent should include(msg)
          errContent should include("Run with --debug to see the complete stack trace.")
          exception.code should be(1)
        }
      }
      "parsing error occurs with debug set" in withFullJellyFile { j =>
        withEmptyJenaFile { q =>
          RdfFromJelly.runTestCommand(
            List("rdf", "from-jelly", j, "--to", q),
          )
          val exception =
            intercept[ExitException] {
              RdfFromJelly.runTestCommand(
                List("rdf", "from-jelly", q, "--debug"),
              )
            }
          val msg = InvalidJellyFile(new InvalidProtocolBufferException("")).getMessage
          val errContent = RdfFromJelly.getErrString
          errContent should include(msg)
          errContent should include("eu.neverblink.jelly.cli.InvalidJellyFile")
          exception.code should be(1)
        }
      }
      "invalid output format supplied" in withFullJellyFile { j =>
        withEmptyJenaFile { q =>
          val exception =
            intercept[ExitException] {
              RdfFromJelly.runTestCommand(
                List("rdf", "from-jelly", j, "--to", q, "--out-format", "invalid"),
              )
            }
          val msg = InvalidFormatSpecified("invalid", RdfFromJellyPrint.validFormatsString)
          RdfFromJelly.getErrString should include(msg.getMessage)
          exception.code should be(1)
        }
      }
      "invalid but known output format supplied" in withFullJellyFile { j =>
        withEmptyJellyFile { q =>
          val exception =
            intercept[ExitException] {
              RdfFromJelly.runTestCommand(
                List(
                  "rdf",
                  "from-jelly",
                  j,
                  "--to",
                  q,
                  "--out-format",
                  RdfFormat.JellyBinary.cliOptions.head,
                ),
              )
            }
          val msg = InvalidFormatSpecified(
            RdfFormat.JellyBinary.cliOptions.head,
            RdfFromJellyPrint.validFormatsString,
          )
          RdfFromJelly.getErrString should include(msg.getMessage)
          exception.code should be(1)
        }
      }
      "readable but not writable format supplied" in withFullJellyFile { j =>
        withEmptyJenaFile(
          testCode = { q =>
            val exception =
              intercept[ExitException] {
                RdfFromJelly.runTestCommand(
                  List(
                    "rdf",
                    "from-jelly",
                    j,
                    "--to",
                    q,
                    "--out-format",
                    RdfFormat.RdfXML.cliOptions.head,
                  ),
                )
              }
            val msg = InvalidFormatSpecified(
              RdfFormat.RdfXML.cliOptions.head,
              RdfFromJellyPrint.validFormatsString,
            )
            RdfFromJelly.getErrString should include(msg.getMessage)
            exception.code should be(1)
          },
          jenaLang = RDFLanguages.RDFXML,
        )
      }
    }
  }
