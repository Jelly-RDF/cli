package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.helpers.{DataGenHelper, TestFixtureHelper}
import eu.neverblink.jelly.cli.command.rdf.util.RdfFormat
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.convert.jena.riot.JellyLanguage
import eu.neverblink.jelly.core.proto.v1.{LogicalStreamType, PhysicalStreamType, RdfStreamFrame}
import eu.neverblink.jelly.core.JellyOptions
import eu.neverblink.jelly.core.proto.google.v1 as google
import eu.neverblink.jelly.core.utils.IoUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFLanguages, RDFParser}
import org.apache.jena.sparql.core.DatasetGraphFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import scala.jdk.CollectionConverters.*
import scala.util.Using

object RdfToJellySpec:
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
        .takeWhile(_ != null)
        .toSeq
    } match {
      case scala.util.Success(value) => value
      case scala.util.Failure(exception) => throw exception
    }

class RdfToJellySpec extends AnyWordSpec with TestFixtureHelper with Matchers:
  import RdfToJellySpec.*

  protected val testCardinality: Int = 33

  "rdf to-jelly command" should {
    "handle conversion of NQuads to Jelly" when {
      "a file to output stream" in withFullJenaFile { f =>
        val (out, err) =
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
        val bytes = RdfToJelly.getOutBytes
        // Make sure it's written in the delimited format
        IoUtils.autodetectDelimiting(new ByteArrayInputStream(bytes)).isDelimited should be(true)
        val content = translateJellyBack(ByteArrayInputStream(bytes))
        content.containsAll(
          DataGenHelper.generateTripleModel(testCardinality).listStatements(),
        ) shouldBe true
      }

      "a file to file" in withFullJenaFile { f =>
        withEmptyJellyFile { j =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", j, f))
          val content = translateJellyBack(new FileInputStream(j))
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
        content.containsAll(tripleModel.listStatements()) shouldBe true
      }

      "preserve the original blank node IDs" in {
        val inputString =
          """_:b1 <http://a.com/p> _:b2 .
            |_:b1 <http://a.com/p> _:b3 .
            |""".stripMargin
        val input = ByteArrayInputStream(inputString.getBytes)
        RdfToJelly.setStdIn(input)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format", RdfFormat.NQuads.cliOptions.head),
        )
        val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
        val content = translateJellyBack(newIn)
        content.size() should be(2)
        val statements = content.listStatements().asScala.toSeq
        statements.flatMap(s => Seq(s.getSubject, s.getObject)).toSet
          .map(_.asResource().getId.toString)
          .toSet should be(Set("b1", "b2", "b3"))
      }

      "input stream to output stream, generalized RDF (N-Triples)" in {
        val inputStream = new FileInputStream(getClass.getResource("/generalized.nt").getPath)
        RdfToJelly.setStdIn(inputStream)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format=nt"),
        )
        val bytes = RdfToJelly.getOutBytes
        val content = translateJellyBack(new ByteArrayInputStream(bytes))
        content.size() should be(4)
        val frames = readJellyFile(new ByteArrayInputStream(bytes))
        val opts = frames.head.getRows.asScala.head.getOptions
        opts.getGeneralizedStatements should be(true)
      }

      "input stream to output stream, generalized RDF (N-Quads)" in {
        val inputStream = new FileInputStream(getClass.getResource("/generalized.nq").getPath)
        RdfToJelly.setStdIn(inputStream)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format=nq"),
        )
        val bytes = RdfToJelly.getOutBytes
        val ds = DatasetGraphFactory.create()
        RDFParser.source(new ByteArrayInputStream(bytes)).lang(JellyLanguage.JELLY).parse(ds)
        ds.size() should be(4) // 4 named graphs
        ds.getDefaultGraph.size() should be(4) // 4 triples in the default graph
        val frames = readJellyFile(new ByteArrayInputStream(bytes))
        val opts = frames.head.getRows.asScala.head.getOptions
        opts.getGeneralizedStatements should be(true)
      }

      "input stream to output stream, GRAPHS stream type, RDF dataset" in {
        val inputStream = DataGenHelper.generateJenaInputStreamDataset(
          10,
          testCardinality,
          RDFLanguages.NQUADS,
        )
        RdfToJelly.setStdIn(inputStream)
        val (out, err) = RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format=nq", "--opt.physical-type=GRAPHS"),
        )
        val ds = DatasetGraphFactory.create()
        val bytes = RdfToJelly.getOutBytes
        RDFParser.source(ByteArrayInputStream(bytes)).lang(
          JellyLanguage.JELLY,
        ).parse(ds)
        ds.size() should be(10) // 10 named graphs
        ds.getDefaultGraph.size() should be(testCardinality)
        for gn <- ds.listGraphNodes().asScala do ds.getGraph(gn).size() should be(testCardinality)
        // Check the logical stream type -- should be the default one
        val frame = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(bytes))
        frame.getRows.asScala.head.getOptions.getLogicalType should be(LogicalStreamType.FLAT_QUADS)
      }

      "input stream to output stream, GRAPHS stream type, 5 RDF datasets" in {
        val bytes = (1 to 5).map(i =>
          DataGenHelper.generateJenaInputStreamDataset(
            4,
            testCardinality,
            RDFLanguages.NQUADS,
            f"dataset$i/",
          ).readAllBytes(),
        ).foldLeft(Array.empty[Byte])(_ ++ _)
        val inputStream = new ByteArrayInputStream(bytes)
        RdfToJelly.setStdIn(inputStream)
        val (out, err) = RdfToJelly.runTestCommand(
          List(
            "rdf",
            "to-jelly",
            "--in-format=nq",
            "--opt.physical-type=GRAPHS",
            "--opt.logical-type=DATASETS",
          ),
        )
        val ds = DatasetGraphFactory.create()
        val outBytes = RdfToJelly.getOutBytes
        RDFParser.source(ByteArrayInputStream(outBytes)).lang(
          JellyLanguage.JELLY,
        ).parse(ds)
        ds.size() should be(20)
        ds.getDefaultGraph.size() should be(testCardinality * 5)
        for gn <- ds.listGraphNodes().asScala do ds.getGraph(gn).size() should be(testCardinality)
        // Check the logical stream type -- should be DATASETS
        val frame = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(outBytes))
        frame.getRows.asScala.head.getOptions.getLogicalType should be(LogicalStreamType.DATASETS)
      }

      "an input stream to file" in withEmptyJellyFile { j =>
        val input = DataGenHelper.generateJenaInputStream(testCardinality)
        RdfToJelly.setStdIn(input)
        val tripleModel = DataGenHelper.generateTripleModel(testCardinality)
        val (out, err) = RdfToJelly.runTestCommand(List("rdf", "to-jelly", "--to", j))
        val content = translateJellyBack(new FileInputStream(j))
        content.containsAll(tripleModel.listStatements()) shouldBe true
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.getRows.asScala.head.getOptions
          opts.getStreamName should be("testName")
          opts.getGeneralizedStatements should be(false)
          opts.getRdfStar should be(false)
          opts.getMaxNameTableSize should be(100)
          opts.getMaxPrefixTableSize should be(100)
          opts.getMaxDatatypeTableSize should be(100)
          opts.getLogicalType should be(LogicalStreamType.FLAT_QUADS)
          opts.getVersion should be(1)
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.getRows.asScala.head.getOptions
          opts.getStreamName should be("")
          opts.getGeneralizedStatements should be(true)
          opts.getRdfStar should be(true)
          opts.getMaxNameTableSize should be(JellyOptions.BIG_STRICT.getMaxNameTableSize)
          opts.getMaxPrefixTableSize should be(JellyOptions.BIG_STRICT.getMaxPrefixTableSize)
          opts.getMaxDatatypeTableSize should be(JellyOptions.BIG_STRICT.getMaxDatatypeTableSize)
          opts.getLogicalType should be(LogicalStreamType.FLAT_QUADS)
          opts.getVersion should be(1)
        }
      }

      "a file to file, physical type set to QUADS, logical type to DATASET STREAM" in withFullJenaFile {
        f =>
          withEmptyJellyFile { j =>
            val (out, err) =
              RdfToJelly.runTestCommand(
                List(
                  "rdf",
                  "to-jelly",
                  f,
                  "--opt.physical-type=QUADS",
                  "--opt.logical-type=DATASETS",
                  "--to",
                  j,
                ),
              )
            val content = translateJellyBack(new FileInputStream(j))
            content.containsAll(
              DataGenHelper.generateTripleModel(testCardinality).listStatements(),
            ) shouldBe true
            val frames = readJellyFile(new FileInputStream(j))
            val opts = frames.head.getRows.asScala.head.getOptions
            opts.getStreamName should be("")
            opts.getGeneralizedStatements should be(true)
            opts.getRdfStar should be(true)
            opts.getMaxNameTableSize should be(JellyOptions.BIG_STRICT.getMaxNameTableSize)
            opts.getMaxPrefixTableSize should be(JellyOptions.BIG_STRICT.getMaxPrefixTableSize)
            opts.getMaxDatatypeTableSize should be(JellyOptions.BIG_STRICT.getMaxDatatypeTableSize)
            opts.getPhysicalType should be(PhysicalStreamType.QUADS)
            opts.getLogicalType should be(LogicalStreamType.DATASETS)
            opts.getVersion should be(1)
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
          val frames = readJellyFile(new FileInputStream(j))
          frames.size should be > 3
          for frame <- frames do
            // The encoder may slightly overshoot the target if it needs to pack the lookup entries
            // together with the triple.
            frame.getRows.size should be <= 15
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
          // Note: no actual namespace declarations are present in the test data, because it's
          // N-Quads.
          // TODO: test if the namespace declarations are preserved with Turtle or RDF/XML input.
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.getRows.asScala.head.getOptions
          opts.getVersion should be(2)
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
          val delimitingResponse = IoUtils.autodetectDelimiting(new FileInputStream(j))
          delimitingResponse.isDelimited should be(false)
          val frame = RdfStreamFrame.parseFrom(delimitingResponse.newInput)
          frame.getRows.size should be > 0
        }
      }
    }

    "handle conversion of other formats to Jelly" when {
      "NTriples" when {
        "base functionality expected" in {
          val input = DataGenHelper.generateJenaInputStream(testCardinality, RDFLanguages.NTRIPLES)
          RdfToJelly.setStdIn(input)
          val (out, err) =
            RdfToJelly.runTestCommand(
              List("rdf", "to-jelly", "--in-format", RdfFormat.NTriples.cliOptions.head),
            )
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
        }
        "a file to file, physical type set to TRIPLES, logical type to GRAPHS" in withFullJenaFile(
          testCode = { f =>
            withEmptyJellyFile { j =>
              val (out, err) =
                RdfToJelly.runTestCommand(
                  List(
                    "rdf",
                    "to-jelly",
                    f,
                    "--opt.physical-type=TRIPLES",
                    "--opt.logical-type=GRAPHS",
                    "--to",
                    j,
                  ),
                )
              val content = translateJellyBack(new FileInputStream(j))
              content.containsAll(
                DataGenHelper.generateTripleModel(testCardinality).listStatements(),
              ) shouldBe true
              val frames = readJellyFile(new FileInputStream(j))
              val opts = frames.head.getRows.asScala.head.getOptions
              opts.getStreamName should be("")
              opts.getGeneralizedStatements should be(true)
              opts.getRdfStar should be(true)
              opts.getMaxNameTableSize should be(JellyOptions.BIG_STRICT.getMaxNameTableSize)
              opts.getMaxPrefixTableSize should be(JellyOptions.BIG_STRICT.getMaxPrefixTableSize)
              opts.getMaxDatatypeTableSize should be(
                JellyOptions.BIG_STRICT.getMaxDatatypeTableSize,
              )
              opts.getPhysicalType should be(PhysicalStreamType.TRIPLES)
              opts.getLogicalType should be(LogicalStreamType.GRAPHS)
              opts.getVersion should be(1)
            }
          },
          jenaLang = RDFLanguages.NTRIPLES,
        )

        "a file to file, physical type unspecified, logical type set to GRAPHS" in withFullJenaFile(
          testCode = { f =>
            withEmptyJellyFile { j =>
              val (out, err) =
                RdfToJelly.runTestCommand(
                  List(
                    "rdf",
                    "to-jelly",
                    f,
                    "--opt.logical-type=GRAPHS",
                    "--to",
                    j,
                  ),
                )
              val content = translateJellyBack(new FileInputStream(j))
              content.containsAll(
                DataGenHelper.generateTripleModel(testCardinality).listStatements(),
              ) shouldBe true
              val frames = readJellyFile(new FileInputStream(j))
              val opts = frames.head.getRows.asScala.head.getOptions
              opts.getStreamName should be("")
              opts.getGeneralizedStatements should be(true)
              opts.getRdfStar should be(true)
              opts.getMaxNameTableSize should be(JellyOptions.BIG_STRICT.getMaxNameTableSize)
              opts.getMaxPrefixTableSize should be(JellyOptions.BIG_STRICT.getMaxPrefixTableSize)
              opts.getMaxDatatypeTableSize should be(
                JellyOptions.BIG_STRICT.getMaxDatatypeTableSize,
              )
              opts.getLogicalType should be(LogicalStreamType.GRAPHS)
              opts.getVersion should be(1)
              RdfToJelly.getErrString should include(
                "WARNING: Logical type setting ignored because physical type is not set.",
              )
            }
          },
          jenaLang = RDFLanguages.NTRIPLES,
        )

        "loading options from another file" in withSpecificJellyFile(
          optionsFile =>
            withFullJenaFile(
              jenaFile => {
                RdfToJelly.runTestCommand(
                  List(
                    "rdf",
                    "to-jelly",
                    "--options-from",
                    optionsFile,
                    jenaFile,
                  ),
                )
                val frames = readJellyFile(new FileInputStream(optionsFile))
                val opts = frames.head.getRows.asScala.head.getOptions
                val newFrames = readJellyFile(new ByteArrayInputStream(RdfToJelly.getOutBytes))
                val newOpts = newFrames.head.getRows.asScala.head.getOptions
                opts should equal(newOpts)

              },
              jenaLang = RDFLanguages.NTRIPLES,
            ),
          fileName = "options.jelly",
        )

        "loading options from another and overriding" in withSpecificJellyFile(
          optionsFile =>
            withFullJenaFile(
              jenaFile => {
                RdfToJelly.runTestCommand(
                  List(
                    "rdf",
                    "to-jelly",
                    "--options-from",
                    optionsFile,
                    jenaFile,
                    "--opt.rdf-star",
                    "false",
                  ),
                )
                val frames = readJellyFile(new FileInputStream(optionsFile))
                val opts = frames.head.getRows.asScala.head.getOptions
                val newFrames = readJellyFile(new ByteArrayInputStream(RdfToJelly.getOutBytes))
                val newOpts = newFrames.head.getRows.asScala.head.getOptions
                opts shouldNot equal(newOpts)
                opts.clone().setRdfStar(true) should equal(newOpts)
              },
              jenaLang = RDFLanguages.NTRIPLES,
            ),
          fileName = "options.jelly",
        )

        "loading options from non-delimited file" in withSpecificJellyFile(
          optionsFile =>
            withFullJenaFile(
              jenaFile => {
                RdfToJelly.runTestCommand(
                  List(
                    "rdf",
                    "to-jelly",
                    "--options-from",
                    optionsFile,
                    jenaFile,
                  ),
                )
                val frame = Using(new FileInputStream(optionsFile))(RdfStreamFrame.parseFrom).get
                val opts = frame.getRows.asScala.head.getOptions
                val newFrames = readJellyFile(new ByteArrayInputStream(RdfToJelly.getOutBytes))
                val newOpts = newFrames.head.getRows.asScala.head.getOptions
                opts should equal(newOpts)
              },
              jenaLang = RDFLanguages.NTRIPLES,
            ),
          fileName = "optionsNonDelimited.jelly",
        )
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
        content.containsAll(
          DataGenHelper.generateTripleModel(testCardinality).listStatements(),
        ) shouldBe true
      }
      "TriG" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
        },
        jenaLang = RDFLanguages.RDFPROTO,
      )
      "RDF Thrift" in withFullJenaFile(
        testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(List("rdf", "to-jelly", f))
          val newIn = new ByteArrayInputStream(RdfToJelly.getOutBytes)
          val content = translateJellyBack(newIn)
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
          content.containsAll(
            DataGenHelper.generateTripleModel(testCardinality).listStatements(),
          ) shouldBe true
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
          val delimitingResponse = IoUtils.autodetectDelimiting(new FileInputStream(outFile))
          delimitingResponse.isDelimited should be(false)
          val frame = RdfStreamFrame.parseFrom(delimitingResponse.newInput)
          frame.getRows.size should be > 0
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
          val delimitingResponse = IoUtils.autodetectDelimiting(new FileInputStream(outFile))
          delimitingResponse.isDelimited should be(true)
          val frames = readJellyFile(new FileInputStream(outFile))
          frames.size should be > testCardinality
          for frame <- frames do frame.getRows.size should be(1)
        }
      }
    }

    "infer stream options" when {
      "format is RDF Protobuf" in withEmptyJellyFile(j =>
        withFullJenaFile(
          testCode = { f =>
            val (out, err) =
              RdfToJelly.runTestCommand(
                List(
                  "rdf",
                  "to-jelly",
                  f,
                  "--in-format",
                  RdfFormat.RdfProto.cliOptions.head,
                  "--to",
                  j,
                ),
              )
            val frames = readJellyFile(new FileInputStream(j))
            val opts = frames.head.getRows.asScala.head.getOptions
            opts.getGeneralizedStatements should be(true)
          },
          jenaLang = RDFLanguages.RDFPROTO,
        ),
      )
      "format is RDF Thrift" in withEmptyJellyFile(j =>
        withFullJenaFile(
          testCode = { f =>
            val (out, err) =
              RdfToJelly.runTestCommand(
                List("rdf", "to-jelly", f, "--to", j),
              )
            val frames = readJellyFile(new FileInputStream(j))
            val opts = frames.head.getRows.asScala.head.getOptions
            opts.getGeneralizedStatements should be(true)
          },
          jenaLang = RDFLanguages.RDFTHRIFT,
        ),
      )
      "format is Jelly Text" in withEmptyJellyFile(j =>
        withFullJellyTextFile(testCode = { f =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List("rdf", "to-jelly", f, "--to", j),
            )
          val frames = readJellyFile(new FileInputStream(j))
          val opts = frames.head.getRows.asScala.head.getOptions
          opts.getGeneralizedStatements should be(true)
        }),
      )
      "format is Jelly Text and options present" in withSpecificJellyFile(
        initialJellyFile => {
          val initialFrames = readJellyFile(new FileInputStream(initialJellyFile))
          val initialOpts = initialFrames.head.getRows.asScala.head.getOptions
          val jellyText = google.RdfStreamFrame.parseDelimitedFrom(
            new FileInputStream(initialJellyFile),
          ).toString
          val bytes = ByteArrayInputStream(jellyText.getBytes())
          RdfToJelly.testMode(true)
          RdfToJelly.setStdIn(bytes)
          val (out, err) =
            RdfToJelly.runTestCommand(
              List("rdf", "to-jelly", "--in-format", "jelly-text"),
            )

          val newFrames = readJellyFile(new ByteArrayInputStream(RdfToJelly.getOutBytes))
          val newOpts = newFrames.head.getRows.asScala.head.getOptions
          initialOpts should equal(newOpts)
        },
        fileName = "options.jelly",
      )
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

      "name table with size < 8 specified" in withFullJenaFile { f =>
        val e = intercept[ExitException] {
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--opt.max-name-table-size=5"))
        }
        e.cause.get shouldBe a[JellySerializationError]
        val cause = e.cause.get.asInstanceOf[JellySerializationError]
        cause.message should include("name table size of 5 ")
        e.code should be(1)
      }

      "unknown physical type specified" in withFullJenaFile { f =>
        val e = intercept[ExitException] {
          RdfToJelly.runTestCommand(List("rdf", "to-jelly", f, "--opt.physical-type=UNKNOWN"))
        }
        e.cause.get shouldBe a[InvalidArgument]
        val cause = e.cause.get.asInstanceOf[InvalidArgument]
        cause.argument should be("--opt.physical-type")
        cause.argumentValue should be("UNKNOWN")
        e.code should be(1)
      }
    }

    "emit a warning" when {
      "requesting an unsupported logical / physical combination" in withFullJenaFile { f =>
        val (out, err) =
          RdfToJelly.runTestCommand(
            List(
              "rdf",
              "to-jelly",
              "--opt.logical-type=SUBJECT_GRAPHS",
              "--opt.physical-type=QUADS",
              f,
            ),
          )
        err should (
          include("WARNING") and
            include("unsupported") and
            include("SUBJECT_GRAPHS/QUADS")
        )
      }
    }

    "not emit a warning" when {
      "requesting an unsupported logical / physical combination with --quiet flag" in withFullJenaFile {
        f =>
          val (out, err) =
            RdfToJelly.runTestCommand(
              List(
                "rdf",
                "to-jelly",
                "--quiet",
                "--opt.logical-type=SUBJECT_GRAPHS",
                "--opt.physical-type=QUADS",
                f,
              ),
            )
          err should not(
            include("WARNING") and
              include("unsupported") and
              include("SUBJECT_GRAPHS/QUADS"),
          )
      }
    }

    "handle IRI resolution" when {
      "IRI resolution enabled (default), input TTL stream" in withEmptyJellyFile { j =>
        val input =
          """BASE <http://example.org/>
            |<a> <http://example.org/p> <b> .
            |""".stripMargin
        RdfToJelly.setStdIn(ByteArrayInputStream(input.getBytes))
        RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format=ttl", "--to", j),
        )
        val content = translateJellyBack(new FileInputStream(j))
        val stmts = content.listStatements().asScala.toSeq
        stmts.size should be(1)
        stmts.head.getSubject.getURI should be("http://example.org/a")
        stmts.head.getPredicate.getURI should be("http://example.org/p")
        stmts.head.getObject.asResource().getURI should be("http://example.org/b")
      }

      "IRI resolution disabled, input TTL stream" in withEmptyJellyFile { j =>
        val input =
          """BASE <http://example.org/>
            |<a> <http://example.org/p> <b> .
            |""".stripMargin
        RdfToJelly.setStdIn(ByteArrayInputStream(input.getBytes))
        RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--in-format=ttl", "--resolve-iris=false", "--to", j),
        )
        val content = translateJellyBack(new FileInputStream(j))
        val stmts = content.listStatements().asScala.toSeq
        stmts.size should be(1)
        stmts.head.getSubject.getURI should be("a")
        stmts.head.getPredicate.getURI should be("http://example.org/p")
        stmts.head.getObject.asResource().getURI should be("b")
      }

      "IRI resolution enabled (but ignored), input NT stream" in withEmptyJellyFile { j =>
        val input =
          """<a> <http://example.org/p> <b> .
            |""".stripMargin
        RdfToJelly.setStdIn(ByteArrayInputStream(input.getBytes))
        RdfToJelly.runTestCommand(
          List("rdf", "to-jelly", "--to", j),
        )
        val content = translateJellyBack(new FileInputStream(j))
        val stmts = content.listStatements().asScala.toSeq
        stmts.size should be(1)
        stmts.head.getSubject.getURI should be("a")
        stmts.head.getPredicate.getURI should be("http://example.org/p")
        stmts.head.getObject.asResource().getURI should be("b")
      }
    }
  }
