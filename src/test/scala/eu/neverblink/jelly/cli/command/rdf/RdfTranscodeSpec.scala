package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.core.JellyConstants
import eu.neverblink.jelly.cli.command.helpers.{DataGenHelper, TestFixtureHelper}
import eu.neverblink.jelly.cli.command.rdf.util.{JellyUtil, RdfJellySerializationOptions}
import eu.neverblink.jelly.core.proto.v1.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, FileInputStream}
import scala.jdk.CollectionConverters.*

class RdfTranscodeSpec extends AnyWordSpec, Matchers, TestFixtureHelper:
  protected val testCardinality: Int = 36

  private val defaultOpt = RdfJellySerializationOptions().asRdfStreamOptions

  private def checkOutputWithDefaultOptions(b: Array[Byte]): Unit =
    val outF = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(b))
    outF.getColumns.getRowCount should be(testCardinality)
    val opt = outF.getRows.asScala.head.getOptions
    opt.getPhysicalType should be(PhysicalStreamType.TRIPLES)
    opt.getStreamType should be(RdfStreamType.FLAT)
    opt.getVersion should be(JellyConstants.PROTO_VERSION_1_2_X)
    opt.getMaxNameTableSize should be(defaultOpt.getMaxNameTableSize)
    opt.getMaxPrefixTableSize should be(defaultOpt.getMaxPrefixTableSize)
    opt.getMaxDatatypeTableSize should be(defaultOpt.getMaxDatatypeTableSize)
    opt.getRdfVersion should be(RdfVersion.RDF_VERSION_UNSPECIFIED)

  "rdf transcode command" should {
    "transcode input file with no additional options" in withFullJellyFile { j =>
      RdfTranscode.runTestCommand(List("rdf", "transcode", j))
      val outB = RdfTranscode.getOutBytes
      checkOutputWithDefaultOptions(outB)
    }

    "transcode stdin with no additional options" in withFullJellyFile { j =>
      val inBytes = FileInputStream(j).readAllBytes()
      RdfTranscode.setStdIn(ByteArrayInputStream(inBytes))
      RdfTranscode.runTestCommand(List("rdf", "transcode"))
      val outB = RdfTranscode.getOutBytes
      checkOutputWithDefaultOptions(outB)
    }

    "transcode input file to output file with no additional options" in withEmptyJellyFile { jOut =>
      withFullJellyFile { jIn =>
        RdfTranscode.runTestCommand(List("rdf", "transcode", "--to", jOut, jIn))
        val outB = FileInputStream(jOut).readAllBytes()
        checkOutputWithDefaultOptions(outB)
      }
    }

    "merge 100 input streams" in withFullJellyFile { j =>
      val inBytes1 = FileInputStream(j).readAllBytes()
      val inBytes = (0 until 100).map(_ => inBytes1).reduce(_ ++ _)
      RdfTranscode.setStdIn(ByteArrayInputStream(inBytes))
      RdfTranscode.runTestCommand(List("rdf", "transcode"))
      val outB = RdfTranscode.getOutBytes
      checkOutputWithDefaultOptions(outB)
      val outFrames = JellyUtil.iterateRdfStream(ByteArrayInputStream(outB)).toSeq
      outFrames.size should be(100)
      outFrames.foreach { f =>
        f.getColumns.getRowCount should be(testCardinality)
      }
    }

    "transcode input file with changed output options" in withFullJellyFile { j =>
      RdfTranscode.runTestCommand(
        List(
          "rdf",
          "transcode",
          "--opt.max-prefix-table-size=600",
          "--opt.rdf-version=1.2",
          j,
        ),
      )
      val outB = RdfTranscode.getOutBytes
      val f = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(outB))
      f.getColumns.getRowCount should be(testCardinality)
      val opt = f.getRows.asScala.head.getOptions
      opt.getMaxPrefixTableSize should be(600)
      opt.getPhysicalType should be(PhysicalStreamType.TRIPLES)
      opt.getRdfVersion should be(RdfVersion.RDF_VERSION_1_2)
    }

    "not allow for output name table size to smaller than the input" in {
      // Jena's Jelly writer uses the big name table by default
      RdfTranscode.setStdIn(ByteArrayInputStream(DataGenHelper.generateJellyBytes(testCardinality)))
      val e = intercept[ExitException] {
        RdfTranscode.runTestCommand(List("rdf", "transcode", "--opt.max-name-table-size=200"))
      }
      val cause = e.getCause
      cause shouldBe a[JellyTranscodingError]
      cause.getMessage should include("must not be larger than the output's")
    }

    "not allow for an output name table smaller than the minimum" in withFullJellyFile { j =>
      val e = intercept[ExitException] {
        RdfTranscode.runTestCommand(List("rdf", "transcode", "--opt.max-name-table-size=60", j))
      }
      e.getCause shouldBe a[InvalidArgument]
      e.getCause.getMessage should include("Must be at least 128")
    }

    "refuse Jelly-RDF 1.1 input, pointing to rdf to-jelly" in withFullJellyFile(
      j => {
        val e = intercept[ExitException] {
          RdfTranscode.runTestCommand(List("rdf", "transcode", j))
        }
        e.getCause shouldBe a[CriticalException]
        e.getCause.getMessage should include("only supports Jelly-RDF 1.2")
        e.getCause.getMessage should include("rdf to-jelly --in-format jelly")
      },
      rowLayout = true,
    )

    "refuse to write Jelly-RDF 1.1" in withFullJellyFile { j =>
      val e = intercept[ExitException] {
        RdfTranscode.runTestCommand(List("rdf", "transcode", "--opt.jelly-version=1.1", j))
      }
      e.getCause shouldBe a[InvalidArgument]
      e.getCause.getMessage should include("only writes Jelly-RDF 1.2")
    }

    "refuse options that only exist in Jelly-RDF 1.1" in withFullJellyFile { j =>
      val e = intercept[ExitException] {
        RdfTranscode.runTestCommand(List("rdf", "transcode", "--opt.logical-type=GRAPHS", j))
      }
      e.getCause shouldBe a[InvalidArgument]
      e.getCause.getMessage should include("only valid in Jelly-RDF 1.1 output")
    }
  }
