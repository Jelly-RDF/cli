package eu.neverblink.jelly.cli.command.rdf

import com.google.protobuf.InvalidProtocolBufferException
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.helpers.*
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamFrame}
import eu.neverblink.jelly.core.{JellyOptions, JellyTranscoderFactory}
import org.apache.jena.riot.RDFLanguages
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.rdf.model.ModelFactory

class RdfFromJellySpec extends AnyWordSpec with Matchers with TestFixtureHelper:

  protected val testCardinality: Int = 33

  // Make a test input stream with 10 frames... all are the same, but it doesn't matter
  private val input10Frames: Array[Byte] = {
    val j1 = DataGenHelper.generateJellyBytes(testCardinality)
    val f1 = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(j1))
    val os = ByteArrayOutputStream()
    // Need to use the transcoder to make sure the lookup IDs are correct
    val transcoder = JellyTranscoderFactory.fastMergingTranscoderUnsafe(
      JellyOptions.BIG_GENERALIZED.clone
        .setPhysicalType(PhysicalStreamType.TRIPLES),
    )
    for _ <- 0 until 10 do transcoder.ingestFrame(f1).writeDelimitedTo(os)
    os.toByteArray
  }

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

      "input stream of 10 frames to output stream, --take-frames=''" in {
        RdfFromJelly.setStdIn(ByteArrayInputStream(input10Frames))
        val (out, err) = RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", "--out-format", "nt", "--take-frames", ""),
        )
        val outSize = out.split("\n").length
        outSize should be(10 * testCardinality)
      }

      "input stream of 10 frames to output stream, --take-frames=7" in {
        RdfFromJelly.setStdIn(ByteArrayInputStream(input10Frames))
        val (out, err) = RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", "--out-format", "nt", "--take-frames", "7"),
        )
        val outSize = out.split("\n").length
        outSize should be(testCardinality)
      }

      "input stream of 10 frames to output stream, --take-frames=3..=5" in {
        RdfFromJelly.setStdIn(ByteArrayInputStream(input10Frames))
        val (out, err) = RdfFromJelly.runTestCommand(
          List("rdf", "from-jelly", "--out-format", "nt", "--take-frames", "3..=5"),
        )
        val outSize = out.split("\n").length
        outSize should be(3 * testCardinality)
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
                          |    stream_name: "Stream"
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
              |    stream_name: "Stream"
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

      "input stream (10 frames) to output stream --take-frames=3..=5" in withFullJellyFile { j =>
        RdfFromJelly.setStdIn(ByteArrayInputStream(input10Frames))
        val (out, err) = RdfFromJelly.runTestCommand(
          List(
            "rdf",
            "from-jelly",
            "--out-format=jelly-text",
            "--take-frames=3..=5",
          ),
        )

        out should not include "# Frame 0"
        out should include("# Frame 3")
        out should include("# Frame 4")
        out should include("# Frame 5")
        "rows".r.findAllIn(out).length should be(3 * testCardinality)
      }
    }

    for lang <- Seq(RdfFormat.JsonLd, RdfFormat.RdfXml) do
      s"handle conversion of Jelly binary to ${lang.fullName}" when {
        "input stream to output stream" in {
          val input = DataGenHelper.generateJellyInputStream(testCardinality)
          RdfFromJelly.setStdIn(input)
          val model = DataGenHelper.generateTripleModel(testCardinality)
          val (out, err) = RdfFromJelly.runTestCommand(
            List("rdf", "from-jelly", "--out-format", lang.cliOptions.head),
          )
          val newModel = ModelFactory.createDefaultModel()
          RDFDataMgr.read(newModel, new ByteArrayInputStream(out.getBytes()), lang.jenaLang)
          model.isIsomorphicWith(newModel) shouldBe true
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
                    RdfFormat.RdfXml.cliOptions.head,
                  ),
                )
              }
            val msg = InvalidFormatSpecified(
              RdfFormat.RdfXml.cliOptions.head,
              RdfFromJellyPrint.validFormatsString,
            )
            RdfFromJelly.getErrString should include(msg.getMessage)
            exception.code should be(1)
          },
          jenaLang = RDFLanguages.RDFXML,
        )
      }

      "invalid --take-frames argument provided" in {
        val e = intercept[ExitException] {
          RdfFromJelly.runTestCommand(
            List("rdf", "from-jelly", "--out-format", "nt", "--take-frames", "invalid"),
          )
        }
        val cause = e.getCause.asInstanceOf[InvalidArgument]
        cause.argument should be("--take-frames")
        cause.argumentValue should be("invalid")
      }
    }
  }
