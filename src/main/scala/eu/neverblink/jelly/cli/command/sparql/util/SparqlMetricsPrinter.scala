package eu.neverblink.jelly.cli.command.sparql.util

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.command.rdf.util.FrameInfo
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder.*
import eu.neverblink.jelly.core.proto.v1.RdfLookupEntryPacked
import eu.neverblink.jelly.core.proto.v1.sparql.*
import eu.neverblink.protoc.java.runtime.ProtoMessage

import java.io.OutputStream
import scala.jdk.CollectionConverters.*

/** Statistics of a single frame of a Jelly-SPARQL stream, or of several frames added together.
  *
  * With the count statistic, lookup entries and column values are counted one by one (a packed
  * lookup entry holds many values). With the size statistic, their serialized size is summed.
  */
final class SparqlFrameInfo(
    val frameIndex: Long,
    val metadata: Map[String, ByteString],
    // In a PUNCTUATED stream, the index of the result set the frame belongs to
    val resultSetIndex: Option[Long] = None,
)(using
    statCollector: FrameInfo.StatisticCollector,
):
  var frameCount: Long = 1
  // Number of result sets that start in these frames
  var resultSetCount: Long = 0
  // In a PUNCTUATED stream, the variables or the ASK result of the result set that starts in this
  // frame, if any
  var resultSetHeader: Seq[(String, YamlValue)] = Seq()
  // Error message of the last trailer that reported one
  var trailerError: Option[String] = None
  private object stat:
    var frame: Long = 0
    var row: Long = 0
    var option: Long = 0
    var variable: Long = 0
    var name: Long = 0
    var prefix: Long = 0
    var datatype: Long = 0
    var iri: Long = 0
    var bnode: Long = 0
    var literal: Long = 0
    var poly: Long = 0
    var askResult: Long = 0
    var trailer: Long = 0

  def +=(other: SparqlFrameInfo): SparqlFrameInfo =
    this.frameCount += 1
    this.resultSetCount += other.resultSetCount
    if other.trailerError.isDefined then this.trailerError = other.trailerError
    this.stat.frame += other.stat.frame
    this.stat.row += other.stat.row
    this.stat.option += other.stat.option
    this.stat.variable += other.stat.variable
    this.stat.name += other.stat.name
    this.stat.prefix += other.stat.prefix
    this.stat.datatype += other.stat.datatype
    this.stat.iri += other.stat.iri
    this.stat.bnode += other.stat.bnode
    this.stat.literal += other.stat.literal
    this.stat.poly += other.stat.poly
    this.stat.askResult += other.stat.askResult
    this.stat.trailer += other.stat.trailer
    this

  private def isCount: Boolean = statCollector == FrameInfo.CountStatistic

  private def measureAll(messages: Iterable[ProtoMessage[?]]): Long =
    messages.iterator.map(statCollector.measure).sum

  private def measureLookup(entries: Iterable[RdfLookupEntryPacked]): Long =
    if isCount then entries.iterator.map(_.getValues.size.toLong).sum
    else measureAll(entries)

  /** Measures a list of columns: the number of values in them, or their size. */
  private def measureColumns[C <: ProtoMessage[?]](
      columns: Iterable[C],
      valueCount: C => Long,
  ): Long =
    if isCount then columns.iterator.map(valueCount).sum
    else measureAll(columns)

  /** @param resultSetStart
    *   whether this is the first frame of a result set
    */
  def processFrame(frame: SparqlResultsFrame, resultSetStart: Boolean = false): Unit =
    if resultSetStart then
      resultSetCount += 1
      if resultSetIndex.isDefined then resultSetHeader = SparqlMetricsPrinter.resultSetHeader(frame)
    stat.frame += statCollector.measure(frame)
    stat.row += frame.getRowCount
    Option(frame.getOptions).foreach(o => stat.option += statCollector.measure(o))
    stat.variable += measureAll(frame.getVariables.asScala)
    stat.name += measureLookup(frame.getNames.asScala)
    stat.prefix += measureLookup(frame.getPrefixes.asScala)
    stat.datatype += measureLookup(frame.getDatatypes.asScala)
    stat.iri += measureColumns(frame.getIriColumns.asScala, iriValues)
    stat.bnode += measureColumns(frame.getBnodeColumns.asScala, bnodeValues)
    stat.literal += measureColumns(frame.getLiteralColumns.asScala, literalValues)
    stat.poly += measureColumns(frame.getPolyColumns.asScala, polyValues)
    Option(frame.getAskResult).foreach(r => stat.askResult += statCollector.measure(r))
    Option(frame.getTrailer).foreach { t =>
      stat.trailer += statCollector.measure(t)
      if t.getError.nonEmpty then trailerError = Some(t.getError)
    }

  private def iriValues(c: SparqlIriColumn): Long = c.getNameIds.size
  private def bnodeValues(c: SparqlBnodeColumn): Long = c.getValues.size
  private def literalValues(c: SparqlLiteralColumn): Long = c.getLexValues.size
  private def polyValues(c: SparqlPolyColumn): Long =
    Option(c.getIris).map(iriValues).getOrElse(0L) +
      Option(c.getBnodes).map(bnodeValues).getOrElse(0L) +
      Option(c.getLiterals).map(literalValues).getOrElse(0L) +
      c.getTripleTerms.size

  def format(): Seq[(String, YamlValue)] =
    val name = statCollector.name()
    Seq(
      "frame_" + name -> YamlLong(stat.frame),
      // The number of solutions, whatever the statistic
      "row_count" -> YamlLong(stat.row),
      "option_" + name -> YamlLong(stat.option),
      "variable_" + name -> YamlLong(stat.variable),
      "name_" + name -> YamlLong(stat.name),
      "prefix_" + name -> YamlLong(stat.prefix),
      "datatype_" + name -> YamlLong(stat.datatype),
      "iri_value_" + name -> YamlLong(stat.iri),
      "bnode_value_" + name -> YamlLong(stat.bnode),
      "literal_value_" + name -> YamlLong(stat.literal),
      "poly_value_" + name -> YamlLong(stat.poly),
      "ask_result_" + name -> YamlLong(stat.askResult),
      "trailer_" + name -> YamlLong(stat.trailer),
    ) ++ trailerError.map(e => "trailer_error" -> YamlString(e))

end SparqlFrameInfo

/** Prints the statistics of a Jelly-SPARQL stream as YAML.
  *
  * This is the Jelly-SPARQL counterpart of
  * [[eu.neverblink.jelly.cli.command.rdf.util.MetricsPrinter]].
  */
object SparqlMetricsPrinter:

  /** The variables or the ASK result, from the first frame of a result set. */
  def resultSetHeader(frame: SparqlResultsFrame): Seq[(String, YamlValue)] =
    Option(frame.getAskResult) match
      case Some(ask) =>
        Seq("ask_result" -> YamlBool(ask.getValue))
      case None =>
        val vars = frame.getVariables.asScala.map(v => YamlListElem(YamlString(v.getName)))
        Seq("variables" -> YamlList(vars.toSeq))

  private def isPunctuated(firstFrame: SparqlResultsFrame): Boolean =
    firstFrame.getOptions.getStreamType == SparqlStreamType.PUNCTUATED

  /** Prints what the first frame says about the whole stream: the options, and, if the stream has
    * only one result set, its variables or the ASK result.
    */
  private def printHeader(firstFrame: SparqlResultsFrame, o: OutputStream): Unit =
    val options = firstFrame.getOptions
    val header = Seq(
      "stream_options" -> YamlMap(
        "stream_name" -> YamlString(options.getStreamName),
        "stream_type" -> YamlEnum(
          Option(options.getStreamType).map(_.toString).getOrElse("UNKNOWN"),
          options.getStreamTypeValue,
        ),
        "rdf_version" -> YamlEnum(
          Option(options.getRdfVersion).map(_.toString).getOrElse("UNKNOWN"),
          options.getRdfVersionValue,
        ),
        "max_name_table_size" -> YamlInt(options.getMaxNameTableSize),
        "max_prefix_table_size" -> YamlInt(options.getMaxPrefixTableSize),
        "max_datatype_table_size" -> YamlInt(options.getMaxDatatypeTableSize),
        "version" -> YamlInt(options.getVersion),
      ),
    ) ++ (if isPunctuated(firstFrame) then Seq() else resultSetHeader(firstFrame))
    for (key, value) <- header do
      o.write(YamlDocBuilder.build(YamlMap(key -> value)).getString.getBytes)
      o.write(System.lineSeparator().getBytes)

  def printPerFrame(
      firstFrame: SparqlResultsFrame,
      iterator: Iterator[SparqlFrameInfo],
      o: OutputStream,
  ): Unit =
    printHeader(firstFrame, o)
    val builder = YamlDocBuilder.build(YamlMap("frames" -> YamlBlank()))
    o.write(builder.getString.getBytes)
    iterator.foreach { frame =>
      val yamlFrame = YamlListElem(
        YamlMap(
          Seq("frame_index" -> YamlLong(frame.frameIndex)) ++
            frame.resultSetIndex.map("result_set_index" -> YamlLong(_)) ++
            frame.resultSetHeader ++
            formatMetadata(frame.metadata).map("metadata" -> _) ++
            frame.format()*,
        ),
      )
      o.write(YamlDocBuilder.build(yamlFrame, builder.currIndent).getString.getBytes)
      o.write(System.lineSeparator().getBytes)
    }

  def printAggregate(
      firstFrame: SparqlResultsFrame,
      iterator: Iterator[SparqlFrameInfo],
      o: OutputStream,
  ): Unit =
    printHeader(firstFrame, o)
    val sum = iterator.reduce((a, b) => a += b)
    // Not printing metadata in this case, as there is no upper bound on the number of frames
    // and thus on the size of the collected metadata.
    val stats = YamlMap(
      Seq("frame_count" -> YamlLong(sum.frameCount)) ++
        (if isPunctuated(firstFrame) then Seq("result_set_count" -> YamlLong(sum.resultSetCount))
         else Seq()) ++
        sum.format()*,
    )
    o.write(YamlDocBuilder.build(YamlMap("frames" -> stats)).getString.getBytes)

  private def formatMetadata(metadata: Map[String, ByteString]): Option[YamlMap] =
    if metadata.isEmpty then None
    else
      Some(
        YamlMap(
          metadata.map { case (k, v) =>
            k -> YamlString(v.toByteArray.map("%02x" format _).mkString)
          }.toSeq*,
        ),
      )
