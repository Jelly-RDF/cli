package eu.neverblink.jelly.cli.util

import eu.neverblink.jelly.cli.util.MetricsPrinter.{formatOptions, formatStats}
import eu.neverblink.jelly.cli.util.YamlDocBuilder.YamlMap
import eu.ostrzyciel.jelly.core.IoUtils
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamOptions}

import java.io.{InputStream, OutputStream}
import scala.collection.mutable.ListBuffer

class FrameInfo:
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

class MetricsPrinter:

  var printOptions: Option[RdfStreamOptions] = None
  var frameInfo: ListBuffer[FrameInfo] = ListBuffer.empty

  def printPerFrame(o: OutputStream): Unit = {
    // I don't really like this approach, better create the printer with the options
    val options = formatOptions(options = printOptions.getOrElse(RdfStreamOptions()))
    val yamlFrames = YamlDocBuilder.YamlList(frameInfo.map { frame =>
      formatStats(frame)
    }.toSeq)
    val fullString =
      YamlDocBuilder.build(
        YamlMap(
          "stream_options" -> options,
          "frames" -> yamlFrames,
        ),
      )
    o.write(fullString.getBytes)

  }

  def printAggregate(o: OutputStream): Unit = {
    val sumCounts = frameInfo.reduce(_ += _)
    val options = formatOptions(options = printOptions.getOrElse(RdfStreamOptions()))
    val fullString =
      YamlDocBuilder.build(
        YamlMap(
          "stream_options" -> options,
          "frames" -> formatStats(sumCounts),
        ),
      )
    o.write(fullString.getBytes)
  }

end MetricsPrinter

object MetricsPrinter:

  /** This method converts a boolean to an integer
    * @param b
    * @return
    */
  private def boolToInt(b: Boolean): Int =
    if b then 1 else 0

  def formatOptions(
      options: RdfStreamOptions,
  ): YamlDocBuilder.YamlMap =
    YamlDocBuilder.YamlMap(
      "rdf_star" -> YamlDocBuilder.YamlInt(boolToInt(options.rdfStar)),
      "stream_name" -> YamlDocBuilder.YamlString(options.streamName),
      "generalized_statements" -> YamlDocBuilder.YamlInt(boolToInt(options.generalizedStatements)),
      "version" -> YamlDocBuilder.YamlInt(options.version),
      "max_datatype_table_size" -> YamlDocBuilder.YamlInt(options.maxDatatypeTableSize),
      "max_name_table_size" -> YamlDocBuilder.YamlInt(options.maxNameTableSize),
      "max_prefix_table_size" -> YamlDocBuilder.YamlInt(options.maxPrefixTableSize),
      "logical_type" -> YamlDocBuilder.YamlInt(options.logicalType.value),
      "physical_type" -> YamlDocBuilder.YamlInt(options.physicalType.value),
    )

  def formatStats(
      frame: FrameInfo,
  ): YamlDocBuilder.YamlMap =
    YamlDocBuilder.YamlMap(
      "option_count" -> YamlDocBuilder.YamlInt(frame.optionCount),
      "name_count" -> YamlDocBuilder.YamlInt(frame.nameCount),
      "namespace_count" -> YamlDocBuilder.YamlInt(frame.namespaceCount),
      "triple_count" -> YamlDocBuilder.YamlInt(frame.tripleCount),
      "quad_count" -> YamlDocBuilder.YamlInt(frame.quadCount),
      "prefix_count" -> YamlDocBuilder.YamlInt(frame.prefixCount),
      "datatype_count" -> YamlDocBuilder.YamlInt(frame.datatypeCount),
      "graph_start_count" -> YamlDocBuilder.YamlInt(frame.graphStartCount),
      "graph_end_count" -> YamlDocBuilder.YamlInt(frame.graphEndCount),
    )

end MetricsPrinter

object JellyUtil:
  /** This method reads the Jelly file and returns an iterator of RdfStreamFrame
    *
    * @param inputStream
    * @param outputStream
    * @return
    */
  def iterateRdfStream(
      inputStream: InputStream,
  ): Iterator[RdfStreamFrame] =
    IoUtils.autodetectDelimiting(inputStream) match
      case (false, newIn) =>
        // Non-delimited Jelly file
        // In this case, we can only read one frame
        Iterator(RdfStreamFrame.parseFrom(newIn))
      case (true, newIn) =>
        // Delimited Jelly file
        // In this case, we can read multiple frames
        Iterator.continually(RdfStreamFrame.parseDelimitedFrom(newIn))
          .takeWhile(_.isDefined).map(_.get)
