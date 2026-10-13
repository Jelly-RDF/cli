package eu.neverblink.jelly.cli.command.rdf.util

import com.google.protobuf.ByteString
import eu.neverblink.jelly.core.proto.v1.{RdfColumn, RdfStreamFrame}

import scala.jdk.CollectionConverters.*

/** Statistics of the values in columns, used by Jelly-RDF 1.2 and Jelly-SPARQL. */
final class ColumnInfo(using statCollector: FrameInfo.StatisticCollector):
  private object stat:
    var iri: Long = 0
    var bnode: Long = 0
    var literal: Long = 0
    var tripleTerm: Long = 0
    var layout: Long = 0

  private def measureValues(count: Int, size: => Long): Long =
    if statCollector == FrameInfo.CountStatistic then count else size

  def process(c: RdfColumn): Unit =
    stat.iri += measureValues(
      c.getNameIds.size,
      c.getNameIds.computeUInt32SizeNoTag + c.getPrefixIds.computeUInt32SizeNoTag,
    )
    stat.literal += measureValues(
      c.getLexValues.size,
      c.getLexValues.computeStringSizeNoTag + c.getLiteralKinds.computeUInt32SizeNoTag +
        c.getLangtags.computeStringSizeNoTag + c.getLangtagDirections.computeUInt32SizeNoTag,
    )
    stat.bnode += measureValues(c.getBnodes.size, c.getBnodes.computeStringSizeNoTag)
    stat.tripleTerm += measureValues(
      c.getTripleTerms.size,
      c.getTripleTerms.asScala.iterator.map(statCollector.measure).sum,
    )
    stat.layout += measureValues(
      c.getLayouts.size,
      c.getLayouts.computeUInt32SizeNoTag + c.getKinds.size,
    )

  def +=(other: ColumnInfo): ColumnInfo =
    stat.iri += other.stat.iri
    stat.bnode += other.stat.bnode
    stat.literal += other.stat.literal
    stat.tripleTerm += other.stat.tripleTerm
    stat.layout += other.stat.layout
    this

  /** All values, without the layout. */
  def total: Long = stat.iri + stat.bnode + stat.literal + stat.tripleTerm

  def format(): Seq[(String, Long)] =
    val name = statCollector.name()
    Seq(
      "iri_value_" + name -> stat.iri,
      "bnode_value_" + name -> stat.bnode,
      "literal_value_" + name -> stat.literal,
      "triple_term_value_" + name -> stat.tripleTerm,
      "layout_" + name -> stat.layout,
    )

end ColumnInfo

/** Statistics of a frame of a Jelly-RDF 1.2 stream (column layout), or of several frames added
  * together.
  *
  * @param messages
  *   whether the stream is a stream of RDF Messages, so that the message boundaries are counted
  */
class ColumnFrameInfo(
    frameIndex: Long,
    metadata: Map[String, ByteString],
    val messages: Boolean,
    val detail: Boolean,
)(using
    statCollector: FrameInfo.StatisticCollector,
) extends FrameInfo(frameIndex, metadata):
  private object stat:
    var frame: Long = 0
    var statement: Long = 0
    var option: Long = 0
    var namespace: Long = 0
    var name: Long = 0
    var prefix: Long = 0
    var datatype: Long = 0
    var messageEnd: Long = 0
  private object column:
    val subject: ColumnInfo = ColumnInfo()
    val predicate: ColumnInfo = ColumnInfo()
    val `object`: ColumnInfo = ColumnInfo()
    val graph: ColumnInfo = ColumnInfo()
    def all: Seq[(String, ColumnInfo)] =
      Seq("subject" -> subject, "predicate" -> predicate, "object" -> `object`, "graph" -> graph)

  override def +=(other: FrameInfo): FrameInfo =
    frameCount += 1
    other match
      case o: ColumnFrameInfo =>
        stat.frame += o.stat.frame
        stat.statement += o.stat.statement
        stat.option += o.stat.option
        stat.namespace += o.stat.namespace
        stat.name += o.stat.name
        stat.prefix += o.stat.prefix
        stat.datatype += o.stat.datatype
        stat.messageEnd += o.stat.messageEnd
        for ((_, mine), (_, theirs)) <- column.all.zip(o.column.all) do mine += theirs
      case _ =>
    this

  override def processFrame(frame: RdfStreamFrame): Unit =
    stat.frame += statCollector.measure(frame)
    frame.getRows.asScala.filter(_.hasOptions).foreach { row =>
      stat.option += statCollector.measure(row.getOptions)
    }
    Option(frame.getColumns).foreach { batch =>
      stat.statement += batch.getRowCount
      stat.namespace += batch.getNamespaces.asScala.iterator.map(statCollector.measure).sum
      stat.name += FrameInfo.measureLookup(batch.getNames)
      stat.prefix += FrameInfo.measureLookup(batch.getPrefixes)
      stat.datatype += FrameInfo.measureLookup(batch.getDatatypes)
      stat.messageEnd +=
        (if statCollector == FrameInfo.CountStatistic then batch.getMessageLengths.size
         else batch.getMessageLengths.computeUInt32SizeNoTag)
      Option(batch.getSubjects).foreach(column.subject.process)
      Option(batch.getPredicates).foreach(column.predicate.process)
      Option(batch.getObjects).foreach(column.`object`.process)
      Option(batch.getGraphs).foreach(column.graph.process)
    }

  /** The values of all columns, by kind. */
  def valuesByKind: ColumnInfo =
    val sum = ColumnInfo()
    column.all.foreach((_, c) => sum += c)
    sum

  /** The values of each column, by kind. */
  def valuesByColumn: Seq[(String, ColumnInfo)] = column.all

  /** The number or size of all values in each column. */
  def formatTotalsByColumn(): Seq[(String, Long)] =
    column.all.map((name, info) => s"${name}_${statCollector.name()}" -> info.total)

  override def format(): Seq[(String, Long)] =
    val name = statCollector.name()
    Seq(
      "frame_" + name -> stat.frame,
      // The number of statements, whatever the statistic
      "statement_count" -> stat.statement,
      "option_" + name -> stat.option,
      "namespace_" + name -> stat.namespace,
      "name_" + name -> stat.name,
      "prefix_" + name -> stat.prefix,
      "datatype_" + name -> stat.datatype,
    ) ++ valuesByKind.format() ++
      (if messages then Seq("message_end_" + name -> stat.messageEnd) else Seq())

end ColumnFrameInfo
