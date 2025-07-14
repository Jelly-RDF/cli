package eu.neverblink.jelly.cli.command.rdf.util

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder.*
import eu.neverblink.jelly.core.proto.v1.*

import java.io.OutputStream
import scala.language.postfixOps

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

  def processStreamRow(row: RdfStreamRow): Unit = row.getRow match {
    case r: RdfTriple => handleTriple(r)
    case r: RdfQuad => handleQuad(r)
    case r: RdfNameEntry => handleNameEntry(r)
    case r: RdfPrefixEntry => handlePrefixEntry(r)
    case r: RdfNamespaceDeclaration => handleNamespaceDeclaration(r)
    case r: RdfDatatypeEntry => handleDatatypeEntry(r)
    case r: RdfGraphStart => handleGraphStart(r)
    case r: RdfGraphEnd => handleGraphEnd(r)
    case r: RdfStreamOptions => handleOption(r)
  }

  protected def handleTriple(r: RdfTriple): Unit = tripleCount += 1
  protected def handleQuad(r: RdfQuad): Unit = quadCount += 1
  protected def handleNameEntry(r: RdfNameEntry): Unit = nameCount += 1
  protected def handlePrefixEntry(r: RdfPrefixEntry): Unit = prefixCount += 1
  protected def handleNamespaceDeclaration(r: RdfNamespaceDeclaration): Unit = namespaceCount += 1
  protected def handleDatatypeEntry(r: RdfDatatypeEntry): Unit = datatypeCount += 1
  protected def handleGraphStart(r: RdfGraphStart): Unit = graphStartCount += 1
  protected def handleGraphEnd(r: RdfGraphEnd): Unit = graphEndCount += 1
  protected def handleOption(r: RdfStreamOptions): Unit = optionCount += 1

  def format(): Seq[(String, Long)] = Seq(
    ("option_count", optionCount),
    ("triple_count", tripleCount),
    ("quad_count", quadCount),
    ("graph_start_count", graphStartCount),
    ("graph_end_count", graphEndCount),
    ("namespace_count", namespaceCount),
    ("name_count", nameCount),
    ("prefix_count", prefixCount),
    ("datatype_count", datatypeCount),
  )

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
      "stream_name" -> YamlString(options.getStreamName),
      "physical_type" -> YamlEnum(options.getPhysicalType.toString, options.getPhysicalTypeValue),
      "generalized_statements" -> YamlBool(options.getGeneralizedStatements),
      "rdf_star" -> YamlBool(options.getRdfStar),
      "max_name_table_size" -> YamlInt(options.getMaxNameTableSize),
      "max_prefix_table_size" -> YamlInt(options.getMaxPrefixTableSize),
      "max_datatype_table_size" -> YamlInt(options.getMaxDatatypeTableSize),
      "logical_type" -> YamlEnum(options.getLogicalType.toString, options.getLogicalTypeValue),
      "version" -> YamlInt(options.getVersion),
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
  ): Seq[(String, YamlValue)] = frame.format().map(_ -> YamlLong(_))

end MetricsPrinter
