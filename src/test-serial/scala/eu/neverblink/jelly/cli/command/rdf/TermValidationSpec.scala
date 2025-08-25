package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.cli.command.rdf.RdfToJellySpec.readJellyFile
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat
import eu.neverblink.jelly.cli.util.jena.JenaSystemOptions
import eu.neverblink.jelly.cli.{ExitException, JellyDeserializationError}
import eu.neverblink.jelly.core.{JellyOptions, RdfProtoDeserializationError}
import eu.neverblink.jelly.core.proto.v1.PhysicalStreamType
import org.apache.jena.datatypes.DatatypeFormatException
import org.apache.jena.shared.impl.JenaParameters
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayInputStream
import scala.util.Try

class TermValidationSpec extends AnyWordSpec, Matchers, TestFixtureHelper:

  protected val testCardinality: Int = 33

  val frame = {
    import eu.neverblink.jelly.cli.command.helpers.RdfAdapter.*
    rdfStreamFrame(
      Seq(
        rdfStreamRow(
          JellyOptions.BIG_GENERALIZED.clone()
            .setPhysicalType(PhysicalStreamType.TRIPLES)
            .setVersion(1),
        ),
        rdfStreamRow(rdfNameEntry(0, "notgood://malformed iri")),
        rdfStreamRow(rdfDatatypeEntry(0, "http://www.w3.org/2001/XMLSchema#date")),
        rdfStreamRow(
          rdfTriple(
            "b1",
            rdfIri(0, 0), // malformed IRI
            rdfLiteral("2025-02-31", 1), // invalid date
          ),
        ),
      ),
    )
  }
  val frameBytes = frame.toByteArray

  "RdfFromJelly" should {
    "warm up" in {
      // Pre-test needed to properly initialize the command for testing
      Try {
        RdfFromJelly.runTestCommand(List("rdf", "from-jelly", "--help"))
      }
    }

    "term validation disabled (default)" in {
      JenaSystemOptions.resetTermValidation()
      JenaParameters.enableEagerLiteralValidation = true
      RdfFromJelly.setStdIn(ByteArrayInputStream(frameBytes))
      val (out, err) = RdfFromJelly.runTestCommand(
        List("rdf", "from-jelly", "--out-format", RdfFormat.NQuads.cliOptions.head),
      )
      out.length should be > 0
      out should include("<notgood://malformed")
      err.isEmpty shouldBe true
    }

    "term validation disabled (explicit)" in {
      JenaSystemOptions.synchronized {
        JenaSystemOptions.resetTermValidation()
        JenaParameters.enableEagerLiteralValidation = true
        RdfFromJelly.setStdIn(ByteArrayInputStream(frameBytes))
        val (out, err) = RdfFromJelly.runTestCommand(
          List(
            "rdf",
            "from-jelly",
            "--out-format",
            RdfFormat.NQuads.cliOptions.head,
            "--validate-terms=false",
          ),
        )
        out.length should be > 0
        out should include("<notgood://malformed")
        err.isEmpty shouldBe true
      }
    }

    "term validation enabled" in {
      JenaSystemOptions.synchronized {
        JenaSystemOptions.resetTermValidation()
        // This is normally not set. We use it to make sure the invalid date literal is actually detected.
        JenaParameters.enableEagerLiteralValidation = true
        RdfFromJelly.setStdIn(ByteArrayInputStream(frameBytes))
        val ex = intercept[ExitException] {
          RdfFromJelly.runTestCommand(
            List(
              "rdf",
              "from-jelly",
              "--out-format",
              RdfFormat.NQuads.cliOptions.head,
              "--validate-terms=true",
            ),
          )
        }
        ex.cause.get shouldBe a[JellyDeserializationError]
        ex.cause.get.getMessage should include("datatype")
      }
    }
  }

  "RdfToJelly" should {
    val input =
      "_:Bb1 <notgood://malformed\\u0020iri> \"lalala\"^^<http://www.w3.org/2001/XMLSchema#int> ."
        .getBytes

    "warm up" in {
      // Pre-test needed to properly initialize the command for testing
      Try {
        RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--help"))
      }
    }

    "term validation disabled (default)" in {
      JenaSystemOptions.resetTermValidation()
      JenaParameters.enableEagerLiteralValidation = true
      RdfToJelly.setStdIn(new ByteArrayInputStream(input))
      val (out, err) = RdfToJelly.runTestCommand(
        List("rdf", "to-jelly", "--in-format=nt"),
      )
      val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
      val frame = readJellyFile(newIn)
      frame.size should be(1)
      frame.head.getRows.size() should be > 3
      err shouldBe empty
    }

    "term validation disabled (explicit)" in {
      JenaSystemOptions.resetTermValidation()
      JenaParameters.enableEagerLiteralValidation = true
      RdfToJelly.setStdIn(new ByteArrayInputStream(input))
      val (out, err) = RdfToJelly.runTestCommand(
        List("rdf", "to-jelly", "--in-format=nt", "--validate-terms=false"),
      )
      val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
      val frame = readJellyFile(newIn)
      frame.size should be(1)
      frame.head.getRows.size() should be > 3
      err shouldBe empty
    }

    "term validation enabled (explicit)" in {
      JenaSystemOptions.resetTermValidation()
      // This is normally not set. We use it to make sure the invalid date literal is actually detected.
      JenaParameters.enableEagerLiteralValidation = true
      RdfToJelly.setStdIn(new ByteArrayInputStream(input))
      val e = intercept[ExitException] {
        RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format=nt", "--validate-terms=true"),
        )
      }
      e.code should be(1)
      e.cause.get shouldBe a[DatatypeFormatException]
    }
  }

  "RdfValidate" should {
    "warm up" in {
      // Pre-test needed to properly initialize the command for testing
      Try {
        RdfValidate.runTestCommand(List("rdf", "validate", "--help"))
      }
    }

    "term validation enabled (default)" in {
      JenaSystemOptions.resetTermValidation()
      JenaParameters.enableEagerLiteralValidation = true
      RdfValidate.setStdIn(ByteArrayInputStream(frameBytes))
      val e = intercept[ExitException] {
        RdfValidate.runTestCommand(List("rdf", "validate"))
      }
      e.cause.get shouldBe a[RdfProtoDeserializationError]
      e.cause.get.getMessage should include("datatype")
    }

    "term validation enabled (explicit)" in {
      JenaSystemOptions.resetTermValidation()
      JenaParameters.enableEagerLiteralValidation = true
      RdfValidate.setStdIn(ByteArrayInputStream(frameBytes))
      val e = intercept[ExitException] {
        RdfValidate.runTestCommand(List("rdf", "validate", "--validate-terms=true"))
      }
      e.cause.get shouldBe a[RdfProtoDeserializationError]
      e.cause.get.getMessage should include("datatype")
    }

    "term validation disabled" in {
      JenaSystemOptions.resetTermValidation()
      // This is normally not set. We use it to make sure the invalid date literal is actually detected.
      JenaParameters.enableEagerLiteralValidation = true
      RdfValidate.setStdIn(ByteArrayInputStream(frameBytes))
      val (out, err) = RdfValidate.runTestCommand(
        List("rdf", "validate", "--validate-terms=false"),
      )
      out shouldBe empty
      err shouldBe empty
    }
  }
