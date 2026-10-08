package eu.neverblink.jelly.cli.command.sparql

import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.core.proto.v1.sparql.{
  SparqlResultsFrame,
  SparqlResultsOptions,
  SparqlResultsTrailer,
  SparqlStreamType,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.UUID.randomUUID

class SparqlValidateSpec extends AnyWordSpec with TestFixtureHelper with Matchers:
  import SparqlSerDesSpec.*

  // Not used by these tests – the SPARQL fixtures are written out by hand.
  protected val testCardinality: Int = 0

  SparqlValidate.testMode(true)

  private val tmpDir: Path = Files.createTempDirectory("jelly-cli-sparql-validate")

  private def withFile[T](content: Array[Byte], extension: String)(testCode: String => T): T =
    val file = Files.createTempFile(tmpDir, randomUUID.toString, extension)
    Files.write(file, content)
    try testCode(file.toString)
    finally Files.deleteIfExists(file)

  private def validate(jelly: Array[Byte], args: String*): Unit =
    SparqlValidate.setStdIn(ByteArrayInputStream(jelly))
    SparqlValidate.runTestCommand(List("sparql", "validate") ++ args)

  /** Runs the validation, expecting it to fail, and returns the error. */
  private def validateFails(jelly: Array[Byte], args: String*): Throwable =
    intercept[ExitException](validate(jelly, args*)).getCause

  /** The same result set as selectJson, with the rows in reverse order and the blank node renamed.
    */
  private val selectJsonReordered: String =
    """{ "head": { "vars": [ "s", "label", "num", "bn" ] },
      |  "results": { "bindings": [
      |    { "s": {"type":"uri","value":"http://example.org/c"},
      |      "num": {"type":"literal","value":"-1",
      |              "datatype":"http://www.w3.org/2001/XMLSchema#integer"} },
      |    { "s": {"type":"uri","value":"http://example.org/b"},
      |      "label": {"type":"literal","value":"cześć","xml:lang":"pl"} },
      |    { "s": {"type":"uri","value":"http://example.org/a"},
      |      "label": {"type":"literal","value":"hello"},
      |      "num": {"type":"literal","value":"42",
      |              "datatype":"http://www.w3.org/2001/XMLSchema#integer"},
      |      "bn": {"type":"bnode","value":"other"} }
      |  ] } }""".stripMargin

  private def withTrailer(frame: SparqlResultsFrame, error: Option[String]): SparqlResultsFrame =
    val f = frame.clone()
      .setTrailer(error.map(e => SparqlResultsTrailer.newInstance().setError(e)).orNull)
    f.resetCachedSize()
    f

  "sparql validate command" should {
    "accept a valid SELECT result stream" in {
      validate(writeJelly(selectJson))
      validate(toSmallFrames(selectJson, 4))
    }

    "accept a valid ASK result stream" in {
      validate(writeJelly(askJson(true)))
      validate(writeJelly(askJson(false)))
    }

    "complain about empty input" in {
      validateFails(Array()).getMessage should include("Empty input stream")
    }

    "complain about malformed input" in {
      validateFails("definitely not Jelly".getBytes(UTF_8)) shouldBe a[InvalidJellyFile]
    }

    "check the delimiting" when {
      val delimited = writeJelly(selectJson)
      val undelimited = writeJelly(selectJson, delimited = false)

      "input is delimited" in {
        validate(delimited)
        validate(delimited, "--delimited=either")
        validate(delimited, "--delimited=true")
        validateFails(delimited, "--delimited=false").getMessage should include(
          "Expected undelimited input",
        )
      }

      "input is undelimited" in {
        validate(undelimited)
        validate(undelimited, "--delimited=false")
        validateFails(undelimited, "--delimited=true").getMessage should include(
          "Expected delimited input",
        )
      }

      "the argument is invalid" in {
        validateFails(delimited, "--delimited=maybe") shouldBe a[InvalidArgument]
      }
    }

    "check the stream structure" when {
      "the first frame has no stream options" in {
        val frames = readFrames(toSmallFrames(selectJson, 4))
        val broken = writeFrames(frames.tail)
        validateFails(broken).getMessage should include("does not contain stream options")
      }

      "the stream options are repeated and match" in {
        val frames = readFrames(writeJelly(selectJson))
        // Concatenating two streams of the same query is allowed
        validate(writeFrames(frames ++ frames))
      }

      "the stream options are repeated and differ" in {
        def withOptions(change: SparqlResultsOptions.Mutable => Unit) =
          readFrames(writeJelly(selectJson)).map { f =>
            val options = f.getOptions.clone()
            change(options)
            val c = f.clone().setOptions(options)
            c.resetCachedSize()
            c
          }
        val first = readFrames(writeJelly(selectJson))
        // Allowed, as long as the new options are valid on their own
        validate(writeFrames(first ++ withOptions(_.setStreamName("other"))))
        validateFails(writeFrames(first ++ withOptions(_.setVersion(0))))
          .getMessage should include("version")
        validateFails(
          writeFrames(first ++ withOptions(_.setStreamType(SparqlStreamType.PUNCTUATED))),
        ).getMessage should include("stream type must be the same")
      }

      "the version in the options is 0" in {
        val frames = readFrames(writeJelly(selectJson)).map { f =>
          val c = f.clone().setOptions(f.getOptions.clone().setVersion(0))
          c.resetCachedSize()
          c
        }
        validateFails(writeFrames(frames)).getMessage should include("version")
      }
    }

    "check the trailer" when {
      "the stream has no trailer" in {
        val frames = readFrames(writeJelly(selectJson)).map(withTrailer(_, None))
        val jelly = writeFrames(frames)
        validate(jelly, "--require-trailer=false")
        validateFails(jelly).getMessage should include(
          "does not end with a trailer",
        )
      }

      "one of the concatenated streams has no trailer" in {
        val frames = readFrames(writeJelly(selectJson))
        val jelly = writeFrames(frames.map(withTrailer(_, None)) ++ frames)
        validate(jelly, "--require-trailer=false")
        validateFails(jelly).getMessage should include(
          "does not end with a trailer",
        )
      }

      "the trailer reports an error" in {
        val frames = readFrames(writeJelly(selectJson)).map(withTrailer(_, Some("query timeout")))
        validateFails(writeFrames(frames)).getMessage should include("query timeout")
      }
    }

    "check a PUNCTUATED stream" when {
      val second =
        selectX("""{"type":"bnode","value":"b0"}""", """{"type":"literal","value":"1"}""")
      val jelly = writePunctuated(Seq(selectJson, askJson(true), second))

      "it is valid" in {
        validate(jelly)
        validate(writePunctuated(Seq(selectJson, second), valuesPerFrame = 2))
      }

      "the last result set has no trailer" in {
        val frames = readFrames(jelly)
        val noTrailer = writeFrames(frames.init :+ withTrailer(frames.last, None))
        validate(noTrailer, "--require-trailer=false")
        validateFails(noTrailer).getMessage should include(
          "does not end with a trailer",
        )
      }

      "a trailer reports an error" in {
        val frames = readFrames(jelly)
        val failed = writeFrames(
          frames.head +: withTrailer(frames(1), Some("query timeout")) +: frames.drop(2),
        )
        val e = validateFails(failed)
        e.getMessage should include("result set 1 is incomplete")
        e.getMessage should include("query timeout")
      }

      "each result set is compared to its own reference file" in {
        withFile(selectJson.getBytes(UTF_8), ".srj") { ref1 =>
          withFile(askJson(true).getBytes(UTF_8), ".srj") { ref2 =>
            withFile(second.getBytes(UTF_8), ".srj") { ref3 =>
              validate(
                jelly,
                "--compare-to-file",
                ref1,
                "--compare-to-file",
                ref2,
                "--compare-to-file",
                ref3,
              )
              val e = validateFails(
                jelly,
                "--compare-to-file",
                ref1,
                "--compare-to-file",
                ref2,
                "--compare-to-file",
                ref1,
              )
              e.getMessage should include("Expected variables ?s ?label ?num ?bn")
              e.getMessage should include("in result set 2")
            }
          }
        }
      }

      "the reference files have a different number of result sets" in {
        withFile(selectJson.getBytes(UTF_8), ".srj") { ref =>
          validateFails(jelly, "--compare-to-file", ref).getMessage should include(
            "Expected 1 result sets, as in the reference files, but the input stream has more",
          )
          validateFails(
            writeJelly(selectJson),
            "--compare-to-file",
            ref,
            "--compare-to-file",
            ref,
          ).getMessage should include(
            "Expected 2 result sets, as in the reference files, but got 1",
          )
        }
      }

      "a Jelly-SPARQL reference file has several result sets" in {
        withFile(writePunctuated(Seq(selectJson, askJson(true))), ".jellys") { ref1 =>
          withFile(second.getBytes(UTF_8), ".srj") { ref2 =>
            validate(jelly, "--compare-to-file", ref1, "--compare-to-file", ref2)
            validateFails(jelly, "--compare-to-file", ref2, "--compare-to-file", ref1)
              .getMessage should include("Expected variables ?x")
          }
        }
      }

      "the solutions of a later result set differ" in {
        val other =
          selectX("""{"type":"bnode","value":"b0"}""", """{"type":"literal","value":"2"}""")
        withFile(writePunctuated(Seq(selectJson, askJson(true), other)), ".jellys") { ref =>
          val e = validateFails(jelly, "--compare-to-file", ref)
          e.getMessage should include("Solution 1 in result set 2 does not match")
        }
      }

      "the same blank node label is used in different result sets" in {
        val bnode = selectX("""{"type":"bnode","value":"a"}""")
        // Blank nodes are scoped to one result set, so these need not be the same node
        withFile(
          writePunctuated(
            Seq(
              selectX("""{"type":"bnode","value":"x"}"""),
              selectX("""{"type":"bnode","value":"y"}"""),
            ),
          ),
          ".jellys",
        ) { ref =>
          validate(writePunctuated(Seq(bnode, bnode)), "--compare-to-file", ref)
        }
      }
    }

    "check the stream options against an options file" when {
      "the options match" in {
        val jelly = writeJelly(selectJson)
        withFile(writeJelly(askJson(true)), ".jellys") { optionsFile =>
          validate(jelly, "--options-file", optionsFile)
        }
      }

      "the options differ" in {
        val optionsFrames = readFrames(writeJelly(askJson(true))).map { f =>
          val c = f.clone().setOptions(f.getOptions.clone().setMaxNameTableSize(200))
          c.resetCachedSize()
          c
        }
        withFile(writeFrames(optionsFrames), ".jellys") { optionsFile =>
          val e = validateFails(writeJelly(selectJson), "--options-file", optionsFile)
          e.getMessage should include("Stream options do not match")
          e.getMessage should include("max_name_table_size: 200")
        }
      }
    }

    "compare the results to a reference file" when {
      "the results match (JSON)" in {
        withFile(selectJson.getBytes(UTF_8), ".srj") { ref =>
          validate(writeJelly(selectJson), "--compare-to-file", ref)
          validate(toSmallFrames(selectJson, 4), "--compare-to-file", ref)
        }
      }

      "the results match (Jelly-SPARQL)" in {
        withFile(toSmallFrames(selectJson, 4), ".jellys") { ref =>
          validate(writeJelly(selectJson), "--compare-to-file", ref)
        }
      }

      "the format is given explicitly" in {
        withFile(selectJson.getBytes(UTF_8), ".txt") { ref =>
          validate(writeJelly(selectJson), "--compare-to-file", ref, "--compare-to-format=json")
        }
      }

      "the format cannot be inferred" in {
        withFile(selectJson.getBytes(UTF_8), ".txt") { ref =>
          validateFails(writeJelly(selectJson), "--compare-to-file", ref) shouldBe
            a[InvalidFormatSpecified]
        }
      }

      "the rows are in a different order" in {
        withFile(selectJsonReordered.getBytes(UTF_8), ".srj") { ref =>
          val e = validateFails(writeJelly(selectJson), "--compare-to-file", ref)
          e.getMessage should include("Solution 0 does not match")
          e.getMessage should include("Expected: ?s=<http://example.org/c>")
          e.getMessage should include("Actual: ?s=<http://example.org/a>")
        }
      }

      "the blank nodes are renamed consistently" in {
        withFile(selectJson.replace("b0", "renamed").getBytes(UTF_8), ".srj") { ref =>
          validate(writeJelly(selectJson), "--compare-to-file", ref)
        }
      }

      "the blank nodes are not renamed consistently" in {
        def bnodes(labels: String*) =
          val rows = labels.map(l => s"""{ "x": {"type":"bnode","value":"$l"} }""")
          s"""{ "head": { "vars": [ "x" ] }, "results": { "bindings": [ ${rows.mkString(
              ", ",
            )} ] } }"""
        // One label on one side, two labels on the other – both ways round
        for (expected, actual) <- Seq(
            bnodes("a", "a") -> bnodes("b", "c"),
            bnodes("a", "b") -> bnodes("c", "c"),
          )
        do
          withFile(expected.getBytes(UTF_8), ".srj") { ref =>
            validateFails(writeJelly(actual), "--compare-to-file", ref)
              .getMessage should include("Solution 1 does not match")
          }
        validate(
          writeJelly(bnodes("c", "d", "c")),
          "--compare-to-file", {
            val f = Files.createTempFile(tmpDir, randomUUID.toString, ".srj");
            Files.write(f, bnodes("a", "b", "a").getBytes(UTF_8)); f.toString
          },
        )
      }

      "the variables are in a different order" in {
        val reordered = selectJson.replace(
          """"vars": [ "s", "label", "num", "bn" ]""",
          """"vars": [ "label", "s", "num", "bn" ]""",
        )
        withFile(reordered.getBytes(UTF_8), ".srj") { ref =>
          validateFails(writeJelly(selectJson), "--compare-to-file", ref)
            .getMessage should include("Expected variables ?label ?s ?num ?bn")
        }
      }

      "the results differ" in {
        val different = selectJson.replace("hello", "goodbye")
        withFile(different.getBytes(UTF_8), ".srj") { ref =>
          validateFails(writeJelly(selectJson), "--compare-to-file", ref)
            .getMessage should include("Solution 0 does not match")
        }
      }

      "the number of solutions differs" in {
        // The first solution of selectJson
        val fewer =
          """{ "head": { "vars": [ "s", "label", "num", "bn" ] },
            |  "results": { "bindings": [
            |    { "s": {"type":"uri","value":"http://example.org/a"},
            |      "label": {"type":"literal","value":"hello"},
            |      "num": {"type":"literal","value":"42",
            |              "datatype":"http://www.w3.org/2001/XMLSchema#integer"},
            |      "bn": {"type":"bnode","value":"b0"} } ] } }""".stripMargin
        withFile(fewer.getBytes(UTF_8), ".srj") { ref =>
          validateFails(writeJelly(selectJson), "--compare-to-file", ref)
            .getMessage should include("Expected 1 solutions, as in")
        }
        withFile(selectJson.getBytes(UTF_8), ".srj") { ref =>
          validateFails(writeJelly(fewer), "--compare-to-file", ref)
            .getMessage should include("Expected 3 solutions, as in")
        }
      }

      "the variables differ" in {
        val otherVars = selectJson.replace("\"bn\" ]", "\"other\" ]")
        withFile(otherVars.getBytes(UTF_8), ".srj") { ref =>
          validateFails(writeJelly(selectJson), "--compare-to-file", ref)
            .getMessage should include("Expected variables")
        }
      }

      "the ASK results match or differ" in {
        withFile(askJson(true).getBytes(UTF_8), ".srj") { ref =>
          validate(writeJelly(askJson(true)), "--compare-to-file", ref)
          validateFails(writeJelly(askJson(false)), "--compare-to-file", ref)
            .getMessage should include("Expected the ASK result to be true")
          validateFails(writeJelly(selectJson), "--compare-to-file", ref)
            .getMessage should include("Expected an ASK result")
        }
      }
    }
  }
