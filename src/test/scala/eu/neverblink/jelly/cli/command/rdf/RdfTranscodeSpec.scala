package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.{ExitException, JellyTranscodingError}
import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
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
    outF.getRows.size should be > 36
    val opt = outF.getRows.asScala.head.getOptions
    opt.getPhysicalType should be(PhysicalStreamType.TRIPLES)
    opt.getLogicalType should be(LogicalStreamType.FLAT_TRIPLES)
    opt.getMaxNameTableSize should be(defaultOpt.getMaxNameTableSize)
    opt.getMaxPrefixTableSize should be(defaultOpt.getMaxPrefixTableSize)
    opt.getMaxDatatypeTableSize should be(defaultOpt.getMaxDatatypeTableSize)
    opt.getRdfStar should be(defaultOpt.getRdfStar)
    opt.getGeneralizedStatements should be(defaultOpt.getGeneralizedStatements)

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
        f.getRows.size should be >= testCardinality
      }
    }

    "transcode input file with changed output options" in withFullJellyFile { j =>
      RdfTranscode.runTestCommand(
        List(
          "rdf",
          "transcode",
          "--opt.max-prefix-table-size=600",
          "--opt.logical-type=GRAPHS",
          j,
        ),
      )
      val outB = RdfTranscode.getOutBytes
      val f = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(outB))
      f.getRows.size should be > testCardinality
      val opt = f.getRows.asScala.head.getOptions
      opt.getMaxPrefixTableSize should be(600)
      opt.getPhysicalType should be(PhysicalStreamType.TRIPLES)
      opt.getLogicalType should be(LogicalStreamType.GRAPHS)
    }

    "not allow for output name table size to smaller than the input" in withFullJellyFile { j =>
      val e = intercept[ExitException] {
        RdfTranscode.runTestCommand(List("rdf", "transcode", "--opt.max-name-table-size=60", j))
      }
      val cause = e.getCause
      cause shouldBe a[JellyTranscodingError]
      cause.getMessage should include("Input lookup size cannot be greater")
    }
  }
