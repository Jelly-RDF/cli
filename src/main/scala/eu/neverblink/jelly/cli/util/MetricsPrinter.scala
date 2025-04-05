package eu.neverblink.jelly.cli.util

import eu.neverblink.jelly.cli.util.YamlDocBuilder.*
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamOptions

import java.io.OutputStream

/** This class is used to store the metrics for a single frame
  */
final class FrameInfo(val frameIndex: Int):
  var frameCount: Int = 1
  var optionCount: Int = 0
  var nameCount: Int = 0
  var namespaceCount: Int = 0
  var tripleCount: Int = 0
  var quadCount: Int = 0
  var prefixCount: Int = 0
  var datatypeCount: Int = 0
  var graphStartCount: Int = 0
  var graphEndCount: Int = 0

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
    val (fullString, indent) =
      YamlDocBuilder.build(
        YamlMap(
          "frames" -> YamlBlank(),
        ),
      )
    o.write(fullString.getBytes)
    iterator.foreach { frame =>
      val yamlFrame = YamlListElem(formatStatsIndex(frame))
      val (fullString, _) = YamlDocBuilder.build(yamlFrame, indent)
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
    val (fullString, _) =
      YamlDocBuilder.build(
        YamlMap(
          "frames" -> formatStatsCount(sumCounts),
        ),
      )
    o.write(fullString.getBytes)
  }

  private def printOptions(
      printOptions: RdfStreamOptions,
      o: OutputStream,
  ): Unit =
    val options = formatOptions(options = printOptions)
    val (fullString, _) =
      YamlDocBuilder.build(
        YamlMap(
          "stream_options" -> options,
        ),
      )
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
    YamlMap(Seq(("frame_index", YamlInt(frame.frameIndex))) ++ formatStats(frame)*)

  private def formatStatsCount(
      frame: FrameInfo,
  ): YamlMap =
    YamlMap(Seq(("frame_count", YamlInt(frame.frameCount))) ++ formatStats(frame)*)

  private def formatStats(
      frame: FrameInfo,
  ): Seq[(String, YamlValue)] =
    Seq(
      ("option_count", YamlInt(frame.optionCount)),
      ("triple_count", YamlInt(frame.tripleCount)),
      ("quad_count", YamlInt(frame.quadCount)),
      ("graph_start_count", YamlInt(frame.graphStartCount)),
      ("graph_end_count", YamlInt(frame.graphEndCount)),
      ("namespace_count", YamlInt(frame.namespaceCount)),
      ("name_count", YamlInt(frame.nameCount)),
      ("prefix_count", YamlInt(frame.prefixCount)),
      ("datatype_count", YamlInt(frame.datatypeCount)),
    )

end MetricsPrinter
