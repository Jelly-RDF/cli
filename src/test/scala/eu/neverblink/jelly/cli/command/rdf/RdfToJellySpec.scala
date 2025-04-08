package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.helpers.{DataGenHelper, TestFixtureHelper}
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat
import eu.neverblink.jelly.cli.{ExitException, InvalidArgument, InvalidFormatSpecified}
import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.core.proto.v1.{LogicalStreamType, RdfStreamFrame}
import eu.ostrzyciel.jelly.core.{IoUtils, JellyOptions}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFLanguages, RDFParser}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import scala.util.Using

class RdfToJellySpec extends AnyWordSpec with TestFixtureHelper with Matchers:

  protected val testCardinality: Int = 33

  def translateJellyBack(inputStream: InputStream): Model =
    Using(inputStream) { content =>
      val newModel = ModelFactory.createDefaultModel()
      RDFParser.source(content).lang(JellyLanguage.JELLY).parse(newModel)
      newModel
    } match {
      case scala.util.Success(value) => value
      case scala.util.Failure(exception) => throw exception
    }

  def readJellyFile(inputStream: InputStream): Seq[RdfStreamFrame] =
    Using(inputStream) { content =>
      Iterator.continually(RdfStreamFrame.parseDelimitedFrom(content))
        .takeWhile(_.nonEmpty)
        .map(_.get)
        .toSeq
    } match {
      case scala.util.Success(value) => value
      case scala.util.Failure(exception) => throw exception
    }

  "rdf to-jelly command" should {
    "handle conversion of NTriples to Jelly" when {
      "a file to output stream" in withFullJenaFile { f =>
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
        val bytes = RdfToJelly.getOutBytes
        // Make sure it's written in the delimited format
        IoUtils.autodetectDelimiting(new ByteArrayInputStream(bytes))._1 should be(true)
        val content = translateJellyBack(ByteArrayInputStream(bytes))
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }

      "a file to file" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", j, f))
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        }
      }

      "input stream to output stream" in {
        val input = DataGenHelper.generateJenaInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format", RdfFormat.NQuads.cliOptions.head),
        )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(tripleModel.listStatements())
      }

      "an input stream to file" in withEmptyJellyFile { j =>
        val input = DataGenHelper.generateJenaInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", j))
        val content = translateJellyBack(new FileInputStream(j))
        content.containsAll(tripleModel.listStatements())
      }

      "a file to file, modified stream options" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                f,
                "--opt.stream-name=testName",
                "--opt.generalized-statements=false",
                "--opt.rdf-star=false",
                "--opt.max-name-table-size=100",
                "--opt.max-prefix-table-size=100",
                "--opt.max-datatype-table-size=100",
                "--opt.logical-type=FLAT_QUADS",
                "--to",
                j,
              ),
            )
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.rows.head.row.options
          opts.streamName should be("testName")
          opts.generalizedStatements should be(false)
          opts.rdfStar should be(false)
          opts.maxNameTableSize should be(100)
          opts.maxPrefixTableSize should be(100)
          opts.maxDatatypeTableSize should be(100)
          opts.logicalType should be(LogicalStreamType.FLAT_QUADS)
          opts.version should be(1)
        }
      }

      "a file to file, modified logical type with full IRI" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                f,
                "--opt.logical-type=https://w3id.org/stax/ontology#flatQuadStream",
                "--to",
                j,
              ),
            )
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.rows.head.row.options
          opts.streamName should be("")
          opts.generalizedStatements should be(true)
          opts.rdfStar should be(true)
          opts.maxNameTableSize should be(JellyOptions.bigStrict.maxNameTableSize)
          opts.maxPrefixTableSize should be(JellyOptions.bigStrict.maxPrefixTableSize)
          opts.maxDatatypeTableSize should be(JellyOptions.bigStrict.maxDatatypeTableSize)
          opts.logicalType should be(LogicalStreamType.FLAT_QUADS)
          opts.version should be(1)
        }
      }

      "a file to file, lowered number of rows per frame" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                f,
                "--rows-per-frame=10",
                "--to",
                j,
              ),
            )
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
          val frames = readJellyFile(new FileInputStream(j))
          frames.size should be > 3
          for frame <- frames do
            // The encoder may slightly overshoot the target if it needs to pack the lookup entries
            // together with the triple.
            frame.rows.size should be <= 15
        }
      }

      "a file to file, enabled namespace declarations" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                f,
                "--enable-namespace-declarations",
                "--to",
                j,
              ),
            )
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
          // Note: no actual namespace declarations are present in the test data, because it's
          // N-Quads.
          // TODO: test if the namespace declarations are preserved with Turtle or RDF/XML input.
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.rows.head.row.options
          opts.version should be(2)
        }
      }

      "a file to file, non-delimited output" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                f,
                "--delimited=false",
                "--to",
                j,
              ),
            )
          val (delimited, is) = IoUtils.autodetectDelimiting(new FileInputStream(j))
          delimited should be(false)
          val frame = RdfStreamFrame.parseFrom(is)
          frame.rows.size should be > 0
        }
      }
    }
    "handle conversion of other formats to Jelly" when {
      "NTriples" in {
        val input = DataGenHelper.generateJenaInputStream(testCardinality, RDFLanguages.NTRIPLES)
        RdfToJelly.setStdIn(input)
        val (out, err) =
          RdfToJelly.runTestCommand(
            List("rdf", "to-jelly", "--in-format", RdfFormat.NTriples.cliOptions.head),
          )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }
      "Turtle" in {
        val input = DataGenHelper.generateJenaInputStream(testCardinality, RDFLanguages.TURTLE)
        RdfToJelly.setStdIn(input)
        val (out, err) =
          RdfToJelly.runTestCommand(
            List("rdf", "to-jelly", "--in-format", RdfFormat.Turtle.cliOptions.head),
          )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
      }
      "TriG" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.TRIG,
      )
      "RDF Protobuf" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List("rdf", "to-jelly", f, "--in-format", RdfFormat.RdfProto.cliOptions.head),
            )
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.RDFPROTO,
      )
      "RDF Thrift" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.RDFTHRIFT,
      )
      "RDF/XML" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List("rdf", "to-jelly", f, "--in-format", RdfFormat.RdfXml.cliOptions.head),
            )
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.RDFXML,
      )
      "JSON-LD" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List("rdf", "to-jelly", f, "--in-format", RdfFormat.JsonLd.cliOptions.head),
            )
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        },
        jenaLang = RDFLanguages.JSONLD,
      )

      "Jelly text format (implicit format)" in withFullJellyTextFile { inFile =>
        withEmptyJellyFile { outFile =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                inFile,
                "--to",
                outFile,
              ),
            )
          val content = translateJellyBack(new FileInputStream(outFile))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
          RdfToJelly.getErrString should include("WARNING: The Jelly text format is not stable")
        }
      }

      "Jelly text format (warning disabled)" in withFullJellyTextFile { inFile =>
        withEmptyJellyFile { outFile =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                inFile,
                "--to",
                outFile,
                "--quiet",
              ),
            )
          RdfToJelly.getErrString should be("")
        }
      }

      "Jelly text format (explicit format parameter)" in withFullJellyTextFile { inFile =>
        withEmptyJellyFile { outFile =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                inFile,
                "--in-format=jelly-text",
                "--to",
                outFile,
              ),
            )
          val content = translateJellyBack(new FileInputStream(outFile))
          content.containsAll(DataGenHelper.generateTripleModel(testCardinality).listStatements())
        }
      }

      "Jelly text format (non-delimited output)" in withFullJellyTextFile { inFile =>
        withEmptyJellyFile { outFile =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                inFile,
                "--delimited=false",
                "--to",
                outFile,
              ),
            )
          val (delimited, is) = IoUtils.autodetectDelimiting(new FileInputStream(outFile))
          delimited should be(false)
          val frame = RdfStreamFrame.parseFrom(is)
          frame.rows.size should be > 0
        }
      }

      "Jelly text format (delimited, multiple frames)" in withFullJellyTextFile { inFile =>
        withEmptyJellyFile { outFile =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                inFile,
                "--rows-per-frame=1",
                "--to",
                outFile,
              ),
            )
          val (delimited, is) = IoUtils.autodetectDelimiting(new FileInputStream(outFile))
          delimited should be(true)
          val frames = readJellyFile(new FileInputStream(outFile))
          frames.size should be > testCardinality
          for frame <- frames do frame.rows.size should be(1)
        }
      }
    }
    "throw proper exception" when {
      "invalid format is specified" in withFullJenaFile { f =>
        val e =
          intercept[ExitException] {
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--in-format", "invalid"))
          }
        e.code should be(1)
        e.cause.get shouldBe a[InvalidFormatSpecified]
        val cause = e.cause.get.asInstanceOf[InvalidFormatSpecified]
        cause.validFormats should be(RdfToJellyPrint.validFormatsString)
        cause.format should be("invalid")
      }
      "invalid format out of existing is specified" in withFullJenaFile { f =>
        val e =
          intercept[ExitException] {
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--in-format", "jelly"))
          }
        e.code should be(1)
        e.cause.get shouldBe a[InvalidFormatSpecified]
        val cause = e.cause.get.asInstanceOf[InvalidFormatSpecified]
        cause.validFormats should be(RdfToJellyPrint.validFormatsString)
        cause.format should be("jelly")
      }
      "invalid logical stream type is specified" in withFullJenaFile { f =>
        val e =
          intercept[ExitException] {
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--opt.logical-type", "test"))
          }
        e.cause.get shouldBe a[InvalidArgument]
        val cause = e.cause.get.asInstanceOf[InvalidArgument]
        cause.argument should be("--opt.logical-type")
        cause.argumentValue should be("test")
        e.code should be(1)
      }
    }
  }
