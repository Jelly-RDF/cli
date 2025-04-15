package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.cli.command.rdf.util.{JellyUtil, RdfJellySerializationOptions}
import eu.ostrzyciel.jelly.core.proto.v1.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, FileInputStream}

class RdfTranscodeSpec extends AnyWordSpec, Matchers, TestFixtureHelper:
  protected val testCardinality: Int = 36

  private val defaultOpt = RdfJellySerializationOptions().asRdfStreamOptions

  private def checkOutputWithDefaultOptions(b: Array[Byte]): Unit =
    val outF = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(b))
    outF.get.rows.size should be > 36
    val opt = outF.get.rows.head.row.options
    opt.physicalType should be(PhysicalStreamType.TRIPLES)
    opt.logicalType should be(LogicalStreamType.FLAT_TRIPLES)
    opt.maxNameTableSize should be(defaultOpt.maxNameTableSize)
    opt.maxPrefixTableSize should be(defaultOpt.maxPrefixTableSize)
    opt.maxDatatypeTableSize should be(defaultOpt.maxDatatypeTableSize)
    opt.rdfStar should be(defaultOpt.rdfStar)
    opt.generalizedStatements should be(defaultOpt.generalizedStatements)

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
      f.rows.size should be >= testCardinality
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
    val f = RdfStreamFrame.parseDelimitedFrom(ByteArrayInputStream(outB)).get
    f.rows.size should be > testCardinality
    val opt = f.rows.head.row.options
    opt.maxPrefixTableSize should be(600)
    opt.physicalType should be(PhysicalStreamType.TRIPLES)
    opt.logicalType should be(LogicalStreamType.GRAPHS)
  }
