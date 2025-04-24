package eu.neverblink.jelly.cli.command.rdf.util

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder.*
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamOptions

import java.io.OutputStream

/** This class is used to store the metrics for a single frame
  */
final class FrameInfo(val frameIndex: Long, val metadata: Map[String, ByteString]):
  var frameCount: Long = 1
  var optionCount: Long = 0
  var nameCount: Long = 0
  var namespaceCount: Long = 0
  var tripleCount: Long = 0
  var quadCount: Long = 0
  var prefixCount: Long = 0
  var datatypeCount: Long = 0
  var graphStartCount: Long = 0
  var graphEndCount: Long = 0

  def +=(other: FrameInfo): FrameInfo = {
    this.frameCount += 1
    this.optionCount += other.optionCount
    this.nameCount += other.nameCount
    this.namespaceCount += other.namespaceCount
    this.tripleCount += other.tripleCount
    this.quadCount += other.quadCount
    this.prefixCount += other.prefixCount
    this.datatypeCount += other.datatypeCount
    this.graphStartCount += other.graphStartCount
    this.graphEndCount += other.graphEndCount
    this
  }

end FrameInfo

object MetricsPrinter:

  def printPerFrame(
      options: RdfStreamOptions,
      iterator: Iterator[FrameInfo],
      o: OutputStream,
  ): Unit =
    printOptions(options, o)
    val builder =
      YamlDocBuilder.build(
        YamlMap(
          "frames" -> YamlBlank(),
        ),
      )
    val fullString = builder.getString
    o.write(fullString.getBytes)
    iterator.foreach { frame =>
      val yamlFrame = YamlListElem(formatStatsIndex(frame))
      val fullString = YamlDocBuilder.build(yamlFrame, builder.currIndent).getString
      o.write(fullString.getBytes)
      o.write(System.lineSeparator().getBytes)
    }

  def printAggregate(
      options: RdfStreamOptions,
      iterator: Iterator[FrameInfo],
      o: OutputStream,
  ): Unit = {
    printOptions(options, o)
    val sumCounts = iterator.reduce((a, b) => a += b)
    val fullString =
      YamlDocBuilder.build(
        YamlMap(
          "frames" -> formatStatsCount(sumCounts),
        ),
      ).getString
    o.write(fullString.getBytes)
  }

  private def printOptions(
      printOptions: RdfStreamOptions,
      o: OutputStream,
  ): Unit =
    val options = formatOptions(options = printOptions)
    val fullString =
      YamlDocBuilder.build(
        YamlMap(
          "stream_options" -> options,
        ),
      ).getString
    o.write(fullString.getBytes)
    o.write(System.lineSeparator().getBytes)

  private def formatOptions(
      options: RdfStreamOptions,
  ): YamlMap =
    YamlMap(
      "stream_name" -> YamlString(options.streamName),
      "physical_type" -> YamlEnum(options.physicalType.toString, options.physicalType.value),
      "generalized_statements" -> YamlBool(options.generalizedStatements),
      "rdf_star" -> YamlBool(options.rdfStar),
      "max_name_table_size" -> YamlInt(options.maxNameTableSize),
      "max_prefix_table_size" -> YamlInt(options.maxPrefixTableSize),
      "max_datatype_table_size" -> YamlInt(options.maxDatatypeTableSize),
      "logical_type" -> YamlEnum(options.logicalType.toString, options.logicalType.value),
      "version" -> YamlInt(options.version),
    )

  private def formatStatsIndex(
      frame: FrameInfo,
  ): YamlMap =
    YamlMap(
      Seq(("frame_index", YamlLong(frame.frameIndex))) ++
        formatMetadata(frame.metadata).map(("metadata", _)) ++
        formatStats(frame)*,
    )

  private def formatStatsCount(
      frame: FrameInfo,
  ): YamlMap =
    // Not printing metadata in this case, as there is no upper bound on the number of frames
    // and thus on the size of the collected metadata.
    YamlMap(Seq(("frame_count", YamlLong(frame.frameCount))) ++ formatStats(frame)*)

  private def formatMetadata(
      metadata: Map[String, ByteString],
  ): Option[YamlMap] =
    if metadata.isEmpty then None
    else
      Some(
        YamlMap(
          metadata.map { case (k, v) =>
            k -> YamlString(v.toByteArray.map("%02x" format _).mkString)
          }.toSeq*,
        ),
      )

  private def formatStats(
      frame: FrameInfo,
  ): Seq[(String, YamlValue)] =
    Seq(
      ("option_count", YamlLong(frame.optionCount)),
      ("triple_count", YamlLong(frame.tripleCount)),
      ("quad_count", YamlLong(frame.quadCount)),
      ("graph_start_count", YamlLong(frame.graphStartCount)),
      ("graph_end_count", YamlLong(frame.graphEndCount)),
      ("namespace_count", YamlLong(frame.namespaceCount)),
      ("name_count", YamlLong(frame.nameCount)),
      ("prefix_count", YamlLong(frame.prefixCount)),
      ("datatype_count", YamlLong(frame.datatypeCount)),
    )

end MetricsPrinter
