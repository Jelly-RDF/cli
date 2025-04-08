package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.{CriticalException, ExitException}
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.ostrzyciel.jelly.core.RdfProtoDeserializationError
import eu.ostrzyciel.jelly.core.JellyOptions
import eu.ostrzyciel.jelly.core.proto.v1.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream}
import scala.util.Using

class RdfValidateSpec extends AnyWordSpec, Matchers, TestFixtureHelper:
  protected val testCardinality: Int = 37

  "rdf validate command" should {
    "complain about empty input" in {
      val e = intercept[ExitException] {
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }
      e.cause.get shouldBe a[CriticalException]
      e.cause.get.getMessage should include("First frame in the input stream is empty")
    }

    "validate delimiting" when {
      val frame = RdfStreamFrame(
        Seq(
          RdfStreamRow(
            RdfStreamOptions(
              physicalType = PhysicalStreamType.QUADS,
              maxNameTableSize = 100,
              maxPrefixTableSize = 100,
              maxDatatypeTableSize = 100,
              logicalType = LogicalStreamType.DATASETS,
              version = 1,
            ),
          ),
        ),
      )
      val bDelimited = {
        val os = ByteArrayOutputStream()
        frame.writeDelimitedTo(os)
        os.toByteArray
      }
      val bUndelimited = {
        val os = ByteArrayOutputStream()
        frame.writeTo(os)
        os.toByteArray
      }

      "no argument specified, input delimited" in {
        RdfValidate.setStdIn(ByteArrayInputStream(bDelimited))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "no argument specified, input undelimited" in {
        RdfValidate.setStdIn(ByteArrayInputStream(bUndelimited))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "--delimited=true, input delimited" in {
        RdfValidate.setStdIn(ByteArrayInputStream(bDelimited))
        RdfValidate.runTestCommand(List("rdf", "validate", "--delimited=true"))
      }

      "--delimited=true, input undelimited" in {
        RdfValidate.setStdIn(ByteArrayInputStream(bUndelimited))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate", "--delimited=true"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include(
          "Expected delimited input, but the file was not delimited",
        )
      }

      "--delimited=false, input delimited" in {
        RdfValidate.setStdIn(ByteArrayInputStream(bDelimited))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate", "--delimited=false"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include(
          "Expected undelimited input, but the file was delimited",
        )
      }

      "--delimited=false, input undelimited" in {
        RdfValidate.setStdIn(ByteArrayInputStream(bUndelimited))
        RdfValidate.runTestCommand(List("rdf", "validate", "--delimited=false"))
      }

      "invalid argument passed to --delimited" in {
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate", "--delimited=invalid"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include("--delimited")
        e.cause.get.getMessage should include(
          "Valid values: true, false, either",
        )
      }
    }

    "validate basic stream structure" when {
      "first row in stream is not options" in {
        val f = RdfStreamFrame(
          Seq(RdfStreamRow(RdfGraphStart())),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include(
          "First row in the input stream does not contain stream options",
        )
      }

      "triple used in a QUADS stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.QUADS).withVersion(1),
            ),
            RdfStreamRow(RdfNameEntry(value = "a")),
            RdfStreamRow(
              RdfTriple(
                RdfIri(0, 1),
                RdfIri(0, 1),
                RdfIri(0, 1),
              ),
            ),
          ),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[RdfProtoDeserializationError]
        e.cause.get.getMessage should include("Unexpected triple row in stream")
      }

      val rdfStarTriple = Seq(
        RdfStreamRow(RdfNameEntry(value = "a")),
        RdfStreamRow(
          RdfTriple(
            RdfIri(0, 1),
            RdfIri(0, 1),
            RdfTriple(RdfIri(0, 1), RdfIri(0, 1), RdfIri(0, 1)),
          ),
        ),
      )
      val generalizedTriple = Seq(
        RdfStreamRow(RdfNameEntry(value = "a")),
        RdfStreamRow(
          RdfTriple(
            RdfLiteral("aaaa"),
            RdfIri(0, 1),
            RdfIri(0, 1),
          ),
        ),
      )
      val rdfStarQuad = Seq(
        RdfStreamRow(RdfNameEntry(value = "a")),
        RdfStreamRow(
          RdfQuad(
            RdfIri(0, 1),
            RdfIri(0, 1),
            RdfTriple(RdfIri(0, 1), RdfIri(0, 1), RdfIri(0, 1)),
            RdfIri(0, 1),
          ),
        ),
      )
      val generalizedQuad = Seq(
        RdfStreamRow(RdfNameEntry(value = "a")),
        RdfStreamRow(
          RdfQuad(
            RdfIri(0, 1),
            RdfIri(0, 1),
            RdfIri(0, 1),
            RdfLiteral("aaaa"),
          ),
        ),
      )

      "RDF-star triple used in an RDF-star stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallRdfStar.withPhysicalType(PhysicalStreamType.TRIPLES).withVersion(1),
            ),
          ) ++ rdfStarTriple,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "RDF-star triple used in a non-RDF-star stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.TRIPLES).withVersion(1),
            ),
          ) ++ rdfStarTriple,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include("Unexpected RDF-star triple in frame 0:")
      }

      "generalized triple used in a generalized stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallGeneralized.withPhysicalType(
                PhysicalStreamType.TRIPLES,
              ).withVersion(1),
            ),
          ) ++ generalizedTriple,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "generalized triple used in a non-generalized stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.TRIPLES).withVersion(1),
            ),
          ) ++ generalizedTriple,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include("Unexpected generalized triple in frame 0:")
      }

      "RDF-star quad used in an RDF-star stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallRdfStar.withPhysicalType(PhysicalStreamType.QUADS).withVersion(1),
            ),
          ) ++ rdfStarQuad,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "RDF-star quad used in a non-RDF-star stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.QUADS).withVersion(1),
            ),
          ) ++ rdfStarQuad,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include("Unexpected RDF-star quad in frame 0:")
      }

      "generalized quad used in a generalized stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallGeneralized.withPhysicalType(
                PhysicalStreamType.QUADS,
              ).withVersion(1),
            ),
          ) ++ generalizedQuad,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "generalized quad used in a non-generalized stream" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.QUADS).withVersion(1),
            ),
          ) ++ generalizedQuad,
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include("Unexpected generalized quad in frame 0:")
      }

      "repeated stream options (matching)" in {
        val o = JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.QUADS).withVersion(1)
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(o),
            RdfStreamRow(o),
          ),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }

      "repeated stream options (differing)" in {
        val o = JellyOptions.smallStrict.withPhysicalType(PhysicalStreamType.QUADS).withVersion(1)
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(o),
            RdfStreamRow(o.withVersion(2)),
          ),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include(
          "Later occurrence of stream options in frame 0 does not match the first",
        )
      }
    }

    "validate options" when {
      "invalid input options supplied, no validation source" in {
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallStrict,
            ),
          ),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate"))
        }
        e.cause.get shouldBe a[RdfProtoDeserializationError]
        e.cause.get.getMessage should include("Incoming physical stream type is not set")
      }

      "same input options supplied as in the validation source" in withFullJellyFile { j =>
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.smallAllFeatures
                .withPhysicalType(PhysicalStreamType.TRIPLES)
                .withLogicalType(LogicalStreamType.FLAT_TRIPLES)
                .withVersion(1),
            ),
          ),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        RdfValidate.runTestCommand(List("rdf", "validate", "--options-file", j))
      }

      "different input options supplied as in the validation source" in withFullJellyFile { j =>
        val f = RdfStreamFrame(
          Seq(
            RdfStreamRow(
              JellyOptions.bigStrict
                .withPhysicalType(PhysicalStreamType.TRIPLES)
                .withVersion(1),
            ),
          ),
        )
        RdfValidate.setStdIn(ByteArrayInputStream(f.toByteArray))
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(List("rdf", "validate", "--options-file", j))
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include("Stream options do not match the expected options")
      }
    }

    "validate content" when {
      "content matches the reference RDF file" in withFullJenaFile { jenaF =>
        withFullJellyFile { jellyF =>
          RdfValidate.runTestCommand(
            List(
              "rdf",
              "validate",
              "--compare-to-rdf-file=" + jenaF,
              jellyF,
            ),
          )
        }
      }

      "content matches the reference RDF file, ordered comparison" in withFullJenaFile { jenaF =>
        withFullJellyFile { jellyF =>
          RdfValidate.runTestCommand(
            List(
              "rdf",
              "validate",
              "--compare-to-rdf-file=" + jenaF,
              "--compare-to-format=nt",
              "--compare-ordered=true",
              jellyF,
            ),
          )
        }
      }

      "content matches the reference RDF file, using a slice of the stream" in withFullJenaFile {
        jenaF =>
          withFullJellyFile { jellyF =>
            val frame1 = Using.resource(FileInputStream(jellyF)) { is =>
              RdfStreamFrame.parseDelimitedFrom(is).get
            }
            val frames = frame1 +: (1 to 10).map { i =>
              RdfStreamFrame(
                Seq(RdfStreamRow(RdfTriple(RdfIri(0, i), RdfIri(0, i), RdfLiteral("aaaa")))),
              )
            }
            val b = {
              val os = ByteArrayOutputStream()
              frames.foreach(_.writeDelimitedTo(os))
              os.toByteArray
            }
            RdfValidate.setStdIn(ByteArrayInputStream(b))
            RdfValidate.runTestCommand(
              List(
                "rdf",
                "validate",
                "--compare-to-rdf-file=" + jenaF,
                "--compare-to-format=nt",
                "--compare-ordered=true",
                "--compare-frame-indices=0",
              ),
            )
          }
      }

      "content does not match the reference RDF file, using slices" in withFullJenaFile { jenaF =>
        withFullJellyFile { jellyF =>
          val frame1 = Using.resource(FileInputStream(jellyF)) { is =>
            RdfStreamFrame.parseDelimitedFrom(is).get
          }
          val frames = frame1 +: (1 to 10).map { i =>
            RdfStreamFrame(
              Seq(RdfStreamRow(RdfTriple(RdfIri(0, i), RdfIri(0, i), RdfLiteral("aaaa")))),
            )
          }
          val b = {
            val os = ByteArrayOutputStream()
            frames.foreach(_.writeDelimitedTo(os))
            os.toByteArray
          }
          RdfValidate.setStdIn(ByteArrayInputStream(b))
          val e = intercept[ExitException] {
            RdfValidate.runTestCommand(
              List(
                "rdf",
                "validate",
                "--compare-to-rdf-file=" + jenaF,
                "--compare-to-format=nt",
                "--compare-ordered=true",
                "--compare-frame-indices=0..4",
              ),
            )
          }
          e.cause.get shouldBe a[CriticalException]
          e.cause.get.getMessage should include(
            "Expected 37 RDF elements, but got 40",
          )
        }
      }

      "RDF file for comparison has an unrecognized format" in {
        val e = intercept[ExitException] {
          RdfValidate.runTestCommand(
            List(
              "rdf",
              "validate",
              "--compare-to-rdf-file=test.txt",
              "--compare-to-format=invalid",
              "--compare-frame-indices=0..4",
            ),
          )
        }
        e.cause.get shouldBe a[CriticalException]
        e.cause.get.getMessage should include(
          "Invalid format option: \"invalid\"",
        )
      }
    }
  }
