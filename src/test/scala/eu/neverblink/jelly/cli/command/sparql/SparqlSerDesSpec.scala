package eu.neverblink.jelly.cli.command.sparql

import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.cli.command.sparql.util.{SparqlFormat, SparqlJellyUtil}
import eu.neverblink.jelly.convert.jena.sparql.JellySparqlLanguage
import eu.neverblink.jelly.core.proto.v1.RdfVersion
import eu.neverblink.jelly.core.proto.v1.sparql.SparqlResultsFrame
import eu.neverblink.jelly.core.sparql.{JellySparqlConstants, JellySparqlOptions}
import org.apache.jena.query.{ResultSet, ResultSetFactory}
import org.apache.jena.riot.{Lang, RIOT, ResultSetMgr}
import org.apache.jena.riot.resultset.{
  ResultSetLang,
  ResultSetReaderRegistry,
  ResultSetWriterRegistry,
}
import org.apache.jena.sparql.resultset.ResultsCompare
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.UUID.randomUUID
import scala.jdk.CollectionConverters.*

object SparqlSerDesSpec:
  /** A SELECT result set testing every term type Jelly-SPARQL has to support, plus unbound cells.
    */
  val selectJson: String =
    """{ "head": { "vars": [ "s", "label", "num", "bn" ] },
      |  "results": { "bindings": [
      |    { "s": {"type":"uri","value":"http://example.org/a"},
      |      "label": {"type":"literal","value":"hello"},
      |      "num": {"type":"literal","value":"42",
      |              "datatype":"http://www.w3.org/2001/XMLSchema#integer"},
      |      "bn": {"type":"bnode","value":"b0"} },
      |    { "s": {"type":"uri","value":"http://example.org/b"},
      |      "label": {"type":"literal","value":"cześć","xml:lang":"pl"} },
      |    { "s": {"type":"uri","value":"http://example.org/c"},
      |      "num": {"type":"literal","value":"-1",
      |              "datatype":"http://www.w3.org/2001/XMLSchema#integer"} }
      |  ] } }""".stripMargin

  def askJson(value: Boolean): String = f"""{ "head": {}, "boolean": $value }"""

  def parse(bytes: Array[Byte], lang: Lang): ResultSet =
    ResultSetMgr.read(ByteArrayInputStream(bytes), lang)

  def parse(s: String, lang: Lang): ResultSet = parse(s.getBytes(UTF_8), lang)

  /** Writes the SELECT results to Jelly-SPARQL, with at most `valuesPerFrame` cells per frame. */
  def toSmallFrames(json: String, valuesPerFrame: Int): Array[Byte] =
    writeJelly(json, valuesPerFrame = valuesPerFrame)

  /** Writes SPARQL JSON results (SELECT or ASK) to Jelly-SPARQL. */
  def writeJelly(
      json: String,
      valuesPerFrame: Int = JellySparqlConstants.DEFAULT_MAX_VALUES_PER_FRAME,
      delimited: Boolean = true,
  ): Array[Byte] =
    val out = ByteArrayOutputStream()
    val context = RIOT.getContext.copy()
      .set(JellySparqlLanguage.SYMBOL_MAX_VALUES_PER_FRAME, valuesPerFrame)
      .set(JellySparqlLanguage.SYMBOL_DELIMITED_OUTPUT, delimited)
    val writer = ResultSetWriterRegistry
      .getFactory(JellySparqlLanguage.JELLY_SPARQL)
      .create(JellySparqlLanguage.JELLY_SPARQL)
    val result = ResultSetReaderRegistry
      .getFactory(ResultSetLang.RS_JSON)
      .create(ResultSetLang.RS_JSON)
      .readAny(ByteArrayInputStream(json.getBytes(UTF_8)), context)
    if result.isBoolean then writer.write(out, result.getBooleanResult.booleanValue, context)
    else writer.write(out, result.getResultSet, context)
    out.toByteArray

  /** Reads the frames of a delimited Jelly-SPARQL stream. */
  def readFrames(bytes: Array[Byte]): Seq[SparqlResultsFrame] =
    SparqlJellyUtil.iterateSparqlStream(ByteArrayInputStream(bytes)).toSeq

  /** Writes the frames as a delimited Jelly-SPARQL stream. */
  def writeFrames(frames: Seq[SparqlResultsFrame]): Array[Byte] =
    val out = ByteArrayOutputStream()
    frames.foreach(_.writeDelimitedTo(out))
    out.toByteArray

class SparqlSerDesSpec extends AnyWordSpec with TestFixtureHelper with Matchers:
  import SparqlSerDesSpec.*

  // Not used by these tests – the SPARQL fixtures are written out by hand.
  protected val testCardinality: Int = 0

  SparqlToJelly.testMode(true)
  SparqlFromJelly.testMode(true)

  private val tmpDir: Path = Files.createTempDirectory("jelly-cli-sparql")

  private def withFile[T](content: String, extension: String)(testCode: String => T): T =
    val file = Files.createTempFile(tmpDir, randomUUID.toString, extension)
    Files.write(file, content.getBytes(UTF_8))
    try testCode(file.toString)
    finally Files.deleteIfExists(file)

  private def withEmptyFile[T](extension: String)(testCode: String => T): T =
    val file = Files.createTempFile(tmpDir, randomUUID.toString, extension)
    try testCode(file.toString)
    finally Files.deleteIfExists(file)

  /** Runs `sparql to-jelly` over the given results and returns the Jelly-SPARQL bytes. */
  private def toJelly(content: String, extension: String, args: List[String] = Nil): Array[Byte] =
    withFile(content, extension) { f =>
      SparqlToJelly.runTestCommand(List("sparql", "to-jelly", f) ++ args)
      SparqlToJelly.getOutBytes
    }

  "sparql to-jelly command" should {
    "convert a SELECT result set, preserving every binding" in {
      val jelly = toJelly(selectJson, ".srj")
      val roundTripped =
        ResultSetFactory.makeRewindable(parse(jelly, JellySparqlLanguage.JELLY_SPARQL))
      // Check the shape explicitly – comparing two empty result sets would also "succeed"
      roundTripped.getResultVars.asScala.toList should contain theSameElementsInOrderAs
        List("s", "label", "num", "bn")
      roundTripped.size shouldBe 3
      roundTripped.reset()
      ResultsCompare.equalsByTermAndOrder(
        roundTripped,
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "convert an ASK result" in {
      for value <- Seq(true, false) do
        val jelly = toJelly(askJson(value), ".srj")
        ResultSetMgr.readBoolean(
          ByteArrayInputStream(jelly),
          JellySparqlLanguage.JELLY_SPARQL,
        ) shouldBe value
    }

    "infer the input format from the file name" in {
      // .srx is only recognizable from the extension – no --in-format is passed
      val xml =
        ResultSetMgr.asString(parse(selectJson, ResultSetLang.RS_JSON), ResultSetLang.RS_XML)
      val jelly = toJelly(xml, ".srx")
      ResultsCompare.equalsByTermAndOrder(
        parse(jelly, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "respect an explicit --in-format over the file name" in {
      val xml =
        ResultSetMgr.asString(parse(selectJson, ResultSetLang.RS_JSON), ResultSetLang.RS_XML)
      // File claims to be JSON, but we tell the command it's really XML
      val jelly = toJelly(xml, ".srj", List("--in-format", "xml"))
      ResultsCompare.equalsByTermAndOrder(
        parse(jelly, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "read from stdin" in {
      SparqlToJelly.setStdIn(ByteArrayInputStream(selectJson.getBytes(UTF_8)))
      SparqlToJelly.runTestCommand(List("sparql", "to-jelly"))
      ResultsCompare.equalsByTermAndOrder(
        parse(SparqlToJelly.getOutBytes, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "reject a format it cannot read" in {
      // The text table is output-only, so it must not be accepted as an input format
      val e = intercept[ExitException] {
        toJelly(selectJson, ".srj", List("--in-format", SparqlFormat.Text.cliOptions.head))
      }
      e.getCause shouldBe a[InvalidFormatSpecified]
    }

    "use the BIG preset options by default" in {
      val options = readFrames(toJelly(selectJson, ".srj")).head.getOptions
      options.getMaxNameTableSize shouldBe JellySparqlOptions.BIG.getMaxNameTableSize
      options.getMaxPrefixTableSize shouldBe JellySparqlOptions.BIG.getMaxPrefixTableSize
      options.getMaxDatatypeTableSize shouldBe JellySparqlOptions.BIG.getMaxDatatypeTableSize
      options.getRdfVersion shouldBe RdfVersion.RDF_VERSION_UNSPECIFIED
      options.getStreamName shouldBe ""
    }

    "set the stream options from --opt.* options" in {
      val jelly = toJelly(
        selectJson,
        ".srj",
        List(
          "--opt.stream-name=my-stream",
          "--opt.rdf-version=1.2-basic",
          "--opt.max-name-table-size=200",
          "--opt.max-prefix-table-size=0",
          "--opt.max-datatype-table-size=10",
        ),
      )
      val options = readFrames(jelly).head.getOptions
      options.getStreamName shouldBe "my-stream"
      options.getRdfVersion shouldBe RdfVersion.RDF_VERSION_1_2_BASIC
      options.getMaxNameTableSize shouldBe 200
      options.getMaxPrefixTableSize shouldBe 0
      options.getMaxDatatypeTableSize shouldBe 10
      options.getVersion shouldBe JellySparqlConstants.PROTO_VERSION
      ResultsCompare.equalsByTermAndOrder(
        parse(jelly, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "accept every RDF version label" in {
      for (label, version) <- Seq(
          "1.1" -> RdfVersion.RDF_VERSION_1_1,
          "1.2-basic" -> RdfVersion.RDF_VERSION_1_2_BASIC,
          "1.2" -> RdfVersion.RDF_VERSION_1_2,
          "unspecified" -> RdfVersion.RDF_VERSION_UNSPECIFIED,
        )
      do
        val jelly = toJelly(askJson(true), ".srj", List("--opt.rdf-version", label))
        readFrames(jelly).head.getOptions.getRdfVersion shouldBe version
    }

    "copy the stream options from another file with --options-from, with overrides" in {
      val source = toJelly(
        selectJson,
        ".srj",
        List("--opt.stream-name=source", "--opt.max-name-table-size=300"),
      )
      withEmptyFile(".jellys") { sourceFile =>
        Files.write(Path.of(sourceFile), source)
        val jelly = toJelly(
          selectJson,
          ".srj",
          List("--options-from", sourceFile, "--opt.stream-name=override"),
        )
        val options = readFrames(jelly).head.getOptions
        options.getStreamName shouldBe "override"
        options.getMaxNameTableSize shouldBe 300
      }
    }

    "reject an unknown RDF version" in {
      val e = intercept[ExitException] {
        toJelly(selectJson, ".srj", List("--opt.rdf-version=1.3"))
      }
      e.getCause shouldBe a[InvalidArgument]
      e.getCause.getMessage should include("--opt.rdf-version")
    }

    "reject a name table smaller than the minimum" in {
      val e = intercept[ExitException] {
        toJelly(selectJson, ".srj", List("--opt.max-name-table-size=64"))
      }
      e.getCause shouldBe a[InvalidArgument]
      e.getCause.getMessage should include("--opt.max-name-table-size")
    }

    "warn about lookup tables that default readers do not accept, unless --quiet is set" in {
      val tooBig = (JellySparqlOptions.MAX_NAME_TABLE_SIZE + 1).toString
      withFile(selectJson, ".srj") { f =>
        val (_, err) = SparqlToJelly.runTestCommand(
          List("sparql", "to-jelly", f, "--opt.max-name-table-size", tooBig),
        )
        err should include("WARNING")
        err should include("name table")
        val (_, quietErr) = SparqlToJelly.runTestCommand(
          List("sparql", "to-jelly", f, "--opt.max-name-table-size", tooBig, "--quiet"),
        )
        quietErr shouldBe empty
      }
    }

    "split the output into frames with --values-per-frame" in {
      val frames = readFrames(toJelly(selectJson, ".srj", List("--values-per-frame=4")))
      // 4 variables and 4 values per frame – one row per frame, plus the trailer
      frames.map(_.getRowCount).sum shouldBe 3
      frames.size should be > 1
    }

    "reject --values-per-frame below 1" in {
      val e = intercept[ExitException] {
        toJelly(selectJson, ".srj", List("--values-per-frame=0"))
      }
      e.getCause shouldBe a[InvalidArgument]
    }

    "write a single non-delimited frame with --delimited=false" in {
      val jelly = toJelly(selectJson, ".srj", List("--delimited=false"))
      SparqlJellyUtil.iterateSparqlStreamWithDelimitingInfo(ByteArrayInputStream(jelly))._1 shouldBe
        false
      ResultsCompare.equalsByTermAndOrder(
        parse(jelly, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }
  }

  "sparql from-jelly command" should {
    "convert a SELECT result set back to the machine-readable formats" in {
      val jelly = toJelly(selectJson, ".srj")
      for format <- Seq(SparqlFormat.Json, SparqlFormat.Xml) do
        SparqlFromJelly.setStdIn(ByteArrayInputStream(jelly))
        SparqlFromJelly.runTestCommand(
          List("sparql", "from-jelly", "--out-format", format.cliOptions.head),
        )
        ResultsCompare.equalsByTermAndOrder(
          parse(SparqlFromJelly.getOutBytes, format.jenaLang),
          parse(selectJson, ResultSetLang.RS_JSON),
        ) shouldBe true
    }

    "write the text table and CSV, which are not machine-readable round trips" in {
      val jelly = toJelly(selectJson, ".srj")
      for format <- Seq(SparqlFormat.Text, SparqlFormat.Csv, SparqlFormat.Tsv) do
        SparqlFromJelly.setStdIn(ByteArrayInputStream(jelly))
        val (out, _) = SparqlFromJelly.runTestCommand(
          List("sparql", "from-jelly", "--out-format", format.cliOptions.head),
        )
        // All three are row-oriented text, so every variable and every subject should show up
        for expected <- Seq("s", "label", "num", "bn", "http://example.org/a", "42", "cześć") do
          out should include(expected)
    }

    "convert an ASK result" in {
      for value <- Seq(true, false) do
        val jelly = toJelly(askJson(value), ".srj")
        SparqlFromJelly.setStdIn(ByteArrayInputStream(jelly))
        val (out, _) = SparqlFromJelly.runTestCommand(List("sparql", "from-jelly"))
        ResultSetMgr.readBoolean(
          ByteArrayInputStream(out.getBytes(UTF_8)),
          ResultSetLang.RS_JSON,
        ) shouldBe value
    }

    "infer the output format from the file name" in {
      val jelly = toJelly(selectJson, ".srj")
      withEmptyFile(".srx") { target =>
        SparqlFromJelly.setStdIn(ByteArrayInputStream(jelly))
        SparqlFromJelly.runTestCommand(List("sparql", "from-jelly", "--to", target))
        ResultsCompare.equalsByTermAndOrder(
          parse(Files.readAllBytes(Path.of(target)), ResultSetLang.RS_XML),
          parse(selectJson, ResultSetLang.RS_JSON),
        ) shouldBe true
      }
    }

    "reject a format it cannot write" in {
      val e = intercept[ExitException] {
        SparqlFromJelly.setStdIn(ByteArrayInputStream(Array()))
        SparqlFromJelly.runTestCommand(
          List("sparql", "from-jelly", "--out-format", "jelly-sparql"),
        )
      }
      e.getCause shouldBe a[InvalidFormatSpecified]
    }

    "report a malformed Jelly file" in {
      withFile("this is definitely not Jelly", ".jellys") { f =>
        val e = intercept[ExitException] {
          SparqlFromJelly.runTestCommand(List("sparql", "from-jelly", f))
        }
        e.getCause shouldBe a[InvalidJellyFile]
      }
    }
  }

  private def withBytesFile[T](content: Array[Byte], extension: String)(testCode: String => T): T =
    val file = Files.createTempFile(tmpDir, randomUUID.toString, extension)
    Files.write(file, content)
    try testCode(file.toString)
    finally Files.deleteIfExists(file)

  "Jelly-RDF format" should {
    import eu.neverblink.jelly.cli.command.helpers.RdfSparqlTestData.*
    import eu.neverblink.jelly.core.proto.v1.PhysicalStreamType

    "be read by to-jelly, with the variables ?s ?p ?o (?g)" in {
      SparqlToJelly.setStdIn(
        ByteArrayInputStream(writeRdf(PhysicalStreamType.QUADS, quads = quads)),
      )
      SparqlToJelly.runTestCommand(List("sparql", "to-jelly", "--in-format", "jelly-rdf"))
      val (vars, rows) = readSparql(SparqlToJelly.getOutBytes)
      vars shouldBe Seq("s", "p", "o", "g")
      rows.size shouldBe quads.size
    }

    "be inferred from the .jelly extension in to-jelly" in {
      withBytesFile(writeRdf(PhysicalStreamType.TRIPLES, triples), ".jelly") { f =>
        SparqlToJelly.runTestCommand(List("sparql", "to-jelly", f))
        readSparql(SparqlToJelly.getOutBytes)._1 shouldBe Seq("s", "p", "o")
      }
    }

    "get the RDF version from the input in to-jelly, unless --opt.rdf-version is set" in {
      val rdf = writeRdf(PhysicalStreamType.TRIPLES, triples, rdfStar = false)
      SparqlToJelly.setStdIn(ByteArrayInputStream(rdf))
      SparqlToJelly.runTestCommand(
        List("sparql", "to-jelly", "--in-format", "jelly-rdf", "--opt.stream-name=x"),
      )
      val options = readFrames(SparqlToJelly.getOutBytes).head.getOptions
      options.getRdfVersion shouldBe RdfVersion.RDF_VERSION_1_1
      options.getStreamName shouldBe "x"
      SparqlToJelly.setStdIn(ByteArrayInputStream(rdf))
      SparqlToJelly.runTestCommand(
        List("sparql", "to-jelly", "--in-format", "jelly-rdf", "--opt.rdf-version=1.2"),
      )
      readFrames(SparqlToJelly.getOutBytes).head.getOptions.getRdfVersion shouldBe
        RdfVersion.RDF_VERSION_1_2
    }

    "be written by from-jelly" in {
      val sparql = toJellyFromRdf(writeRdf(PhysicalStreamType.QUADS, quads = quads))
      SparqlFromJelly.setStdIn(ByteArrayInputStream(sparql))
      SparqlFromJelly.runTestCommand(
        List("sparql", "from-jelly", "--out-format", "jelly-rdf"),
      )
      val jelly = SparqlFromJelly.getOutBytes
      rdfStreamOptions(jelly).getPhysicalType shouldBe PhysicalStreamType.QUADS
      readRdf(jelly) shouldBe quads.toSet
    }

    "be inferred from the .jelly extension in from-jelly" in {
      val sparql = toJellyFromRdf(writeRdf(PhysicalStreamType.TRIPLES, triples))
      withEmptyFile(".jelly") { target =>
        SparqlFromJelly.setStdIn(ByteArrayInputStream(sparql))
        SparqlFromJelly.runTestCommand(List("sparql", "from-jelly", "--to", target))
        readRdf(Files.readAllBytes(Path.of(target))) shouldBe triplesAsQuads
      }
    }

    "report a result set that cannot be written as RDF in from-jelly" in {
      SparqlFromJelly.setStdIn(ByteArrayInputStream(writeSparql(Seq("a", "b"), Nil)))
      val e = intercept[ExitException] {
        SparqlFromJelly.runTestCommand(List("sparql", "from-jelly", "--out-format", "jelly-rdf"))
      }
      e.getCause.getMessage should include("must have 3 or 4 variables")
    }
  }

  /** Runs `sparql to-jelly` over Jelly-RDF and returns the Jelly-SPARQL bytes. */
  private def toJellyFromRdf(rdf: Array[Byte]): Array[Byte] =
    SparqlToJelly.setStdIn(ByteArrayInputStream(rdf))
    SparqlToJelly.runTestCommand(List("sparql", "to-jelly", "--in-format", "jelly-rdf"))
    SparqlToJelly.getOutBytes

  /** Runs `sparql from-jelly` to the text format and returns the text. */
  private def toText(jelly: Array[Byte]): String =
    SparqlFromJelly.setStdIn(ByteArrayInputStream(jelly))
    SparqlFromJelly.runTestCommand(
      List("sparql", "from-jelly", "--out-format", "jelly-sparql-text"),
    )._1

  /** Runs `sparql to-jelly` over the text format and returns the Jelly-SPARQL bytes. */
  private def fromText(text: String): Array[Byte] =
    SparqlToJelly.setStdIn(ByteArrayInputStream(text.getBytes(UTF_8)))
    SparqlToJelly.runTestCommand(
      List("sparql", "to-jelly", "--in-format", "jelly-sparql-text", "--quiet"),
    )
    SparqlToJelly.getOutBytes

  "Jelly-SPARQL text format" should {
    "be written by from-jelly, one commented block per frame" in {
      val text = toText(toJelly(selectJson, ".srj"))
      text should startWith("# Frame 0\n")
      text should not include "# Frame 1"
      for expected <- Seq("options {", "variables {", "name: \"label\"", "row_count: 3") do
        text should include(expected)
      // Non-ASCII text is written without escaping
      text should include("cześć")
    }

    "round trip a SELECT result set through text and back" in {
      val text = toText(toJelly(selectJson, ".srj"))
      ResultsCompare.equalsByTermAndOrder(
        parse(fromText(text), JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "round trip an ASK result through text and back" in {
      for value <- Seq(true, false) do
        val text = toText(toJelly(askJson(value), ".srj"))
        ResultSetMgr.readBoolean(
          ByteArrayInputStream(fromText(text)),
          JellySparqlLanguage.JELLY_SPARQL,
        ) shouldBe value
    }

    "keep frame boundaries in a multi-frame stream" in {
      val jelly = toSmallFrames(selectJson, 4)
      val text = toText(jelly)
      val frameCount = text.linesIterator.count(_.startsWith("# Frame"))
      frameCount should be > 1
      // Converting back to binary and to text again must give the same frames
      val backToJelly = fromText(text)
      toText(backToJelly) shouldBe text
      ResultsCompare.equalsByTermAndOrder(
        parse(backToJelly, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "read text without frame comments as a single frame" in {
      val text = toText(toJelly(selectJson, ".srj"))
        .linesIterator.filterNot(_.startsWith("#")).mkString("\n")
      ResultsCompare.equalsByTermAndOrder(
        parse(fromText(text), JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "infer the format from the .jellys.txt extension on both sides" in {
      val jelly = toJelly(selectJson, ".srj")
      withEmptyFile(SparqlFormat.JellySparqlText.extension) { target =>
        SparqlFromJelly.setStdIn(ByteArrayInputStream(jelly))
        SparqlFromJelly.runTestCommand(List("sparql", "from-jelly", "--to", target))
        val text = String(Files.readAllBytes(Path.of(target)), UTF_8)
        text should startWith("# Frame 0")
        SparqlToJelly.runTestCommand(List("sparql", "to-jelly", target, "--quiet"))
        ResultsCompare.equalsByTermAndOrder(
          parse(SparqlToJelly.getOutBytes, JellySparqlLanguage.JELLY_SPARQL),
          parse(selectJson, ResultSetLang.RS_JSON),
        ) shouldBe true
      }
    }

    "warn that the text format is unstable, unless --quiet is set" in {
      val text = toText(toJelly(selectJson, ".srj"))
      SparqlToJelly.setStdIn(ByteArrayInputStream(text.getBytes(UTF_8)))
      val (_, err) = SparqlToJelly.runTestCommand(
        List("sparql", "to-jelly", "--in-format", "jelly-sparql-text"),
      )
      err should include("WARNING")
      SparqlToJelly.setStdIn(ByteArrayInputStream(text.getBytes(UTF_8)))
      val (_, quietErr) = SparqlToJelly.runTestCommand(
        List("sparql", "to-jelly", "--in-format", "jelly-sparql-text", "--quiet"),
      )
      quietErr shouldBe empty
    }

    "write non-delimited output from text with --delimited=false" in {
      val text = toText(toJelly(selectJson, ".srj"))
      SparqlToJelly.setStdIn(ByteArrayInputStream(text.getBytes(UTF_8)))
      SparqlToJelly.runTestCommand(
        List(
          "sparql",
          "to-jelly",
          "--in-format",
          "jelly-sparql-text",
          "--quiet",
          "--delimited=false",
        ),
      )
      val jelly = SparqlToJelly.getOutBytes
      SparqlJellyUtil.iterateSparqlStreamWithDelimitingInfo(ByteArrayInputStream(jelly))._1 shouldBe
        false
      ResultsCompare.equalsByTermAndOrder(
        parse(jelly, JellySparqlLanguage.JELLY_SPARQL),
        parse(selectJson, ResultSetLang.RS_JSON),
      ) shouldBe true
    }

    "refuse to write several frames from text as non-delimited output" in {
      val text = toText(toSmallFrames(selectJson, 4))
      SparqlToJelly.setStdIn(ByteArrayInputStream(text.getBytes(UTF_8)))
      val e = intercept[ExitException] {
        SparqlToJelly.runTestCommand(
          List(
            "sparql",
            "to-jelly",
            "--in-format",
            "jelly-sparql-text",
            "--quiet",
            "--delimited=false",
          ),
        )
      }
      e.getCause.getMessage should include("more than one frame")
    }

    "warn that stream options are ignored for text input" in {
      val text = toText(toJelly(selectJson, ".srj"))
      SparqlToJelly.setStdIn(ByteArrayInputStream(text.getBytes(UTF_8)))
      val (_, err) = SparqlToJelly.runTestCommand(
        List("sparql", "to-jelly", "--in-format", "jelly-sparql-text", "--opt.stream-name=x"),
      )
      err should include("Stream options are ignored")
    }

    "report malformed text" in {
      val e = intercept[ExitException] {
        fromText("this is { definitely not a frame")
      }
      e.getCause shouldBe a[InvalidJellyFile]
    }
  }
