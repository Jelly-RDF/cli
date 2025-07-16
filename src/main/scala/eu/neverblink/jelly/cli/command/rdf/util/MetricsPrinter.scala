package eu.neverblink.jelly.cli.command.rdf.util

import com.google.protobuf.{ByteString, CodedOutputStream}
import scala.jdk.CollectionConverters.*
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder.*
import eu.neverblink.jelly.core.proto.v1.*
import eu.neverblink.protoc.java.runtime.ProtoMessage

import java.io.OutputStream
import scala.language.postfixOps

object FrameInfo:
  trait StatisticCollector:
    def measure(r: ProtoMessage[?]): Long
    def measure(r: String): Long // Needed as bnodes are plain strings
    def name(): String

  case object CountStatistic extends StatisticCollector:
    override def measure(r: ProtoMessage[?]): Long = 1
    override def measure(r: String): Long = 1
    override def name(): String = "count"

  case object SizeStatistic extends StatisticCollector:
    override def measure(r: ProtoMessage[?]): Long = r.getSerializedSize
    override def measure(r: String): Long = CodedOutputStream.computeStringSizeNoTag(r)
    override def name(): String = "size"

/** This class is used to store the metrics for a single frame
  */
class FrameInfo(val frameIndex: Long, val metadata: Map[String, ByteString])(using
    statCollector: FrameInfo.StatisticCollector,
):
  var frameCount: Long = 1
  private object stat:
    var option: Long = 0
    var name: Long = 0
    var namespace: Long = 0
    var triple: Long = 0
    var quad: Long = 0
    var prefix: Long = 0
    var datatype: Long = 0
    var graphStart: Long = 0
    var graphEnd: Long = 0

  def +=(other: FrameInfo): FrameInfo = {
    this.frameCount += 1
    this.stat.option += other.stat.option
    this.stat.name += other.stat.name
    this.stat.namespace += other.stat.namespace
    this.stat.triple += other.stat.triple
    this.stat.quad += other.stat.quad
    this.stat.prefix += other.stat.prefix
    this.stat.datatype += other.stat.datatype
    this.stat.graphStart += other.stat.graphStart
    this.stat.graphEnd += other.stat.graphEnd
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

  protected def handleTriple(r: RdfTriple): Unit = stat.triple += statCollector.measure(r)
  protected def handleQuad(r: RdfQuad): Unit = stat.quad += statCollector.measure(r)
  protected def handleNameEntry(r: RdfNameEntry): Unit = stat.name += statCollector.measure(r)
  protected def handlePrefixEntry(r: RdfPrefixEntry): Unit = stat.prefix += statCollector.measure(r)
  protected def handleNamespaceDeclaration(r: RdfNamespaceDeclaration): Unit =
    stat.namespace += statCollector.measure(r)
  protected def handleDatatypeEntry(r: RdfDatatypeEntry): Unit =
    stat.datatype += statCollector.measure(r)
  protected def handleGraphStart(r: RdfGraphStart): Unit =
    stat.graphStart += statCollector.measure(r)
  protected def handleGraphEnd(r: RdfGraphEnd): Unit = stat.graphEnd += statCollector.measure(r)
  protected def handleOption(r: RdfStreamOptions): Unit = stat.option += statCollector.measure(r)

  def format(): Seq[(String, Long)] = {
    val name = statCollector.name()
    Seq(
      ("option_" + name, stat.option),
      ("triple_" + name, stat.triple),
      ("quad_" + name, stat.quad),
      ("graph_start_" + name, stat.graphStart),
      ("graph_end_" + name, stat.graphEnd),
      ("namespace_" + name, stat.namespace),
      ("name_" + name, stat.name),
      ("prefix_" + name, stat.prefix),
      ("datatype_" + name, stat.datatype),
    )
  }

end FrameInfo

/** Class containing statistics for each node type. Combines nodes allowed in triple terms (IRI,
  * blank node, literal, triple) and graph term in quads (IRI, blank node, literal, default graph).
  * For simplicity, this class does not validate these constraints.
  */
class NodeDetailInfo(using statCollector: FrameInfo.StatisticCollector):
  private object stat:
    var iri: Long = 0
    var bnode: Long = 0
    var literal: Long = 0
    var triple: Long = 0
    var defaultGraph: Long = 0

  def handle(o: Object): Unit = o match {
    case r: RdfIri => stat.iri += statCollector.measure(r)
    case r: String => stat.bnode += statCollector.measure(r) // bnodes are strings
    case r: RdfLiteral => stat.literal += statCollector.measure(r)
    case r: RdfTriple => stat.triple += statCollector.measure(r)
    case r: RdfDefaultGraph => stat.defaultGraph += statCollector.measure(r)
  }

  def format(): Seq[(String, Long)] = {
    val name = statCollector.name()
    Seq(
      ("iri_" + name, stat.iri),
      ("bnode_" + name, stat.bnode),
      ("literal_" + name, stat.literal),
      ("triple_" + name, stat.triple),
      ("default_graph_" + name, stat.defaultGraph),
    ).filter(_._2 > 0)
  }

  def +=(other: NodeDetailInfo): NodeDetailInfo = {
    this.stat.iri += other.stat.iri
    this.stat.bnode += other.stat.bnode
    this.stat.literal += other.stat.literal
    this.stat.triple += other.stat.triple
    this.stat.defaultGraph += other.stat.defaultGraph
    this
  }

  def total(): Long = stat.iri
    + stat.bnode
    + stat.literal
    + stat.triple
    + stat.defaultGraph

end NodeDetailInfo

class FrameDetailInfo(frameIndex: Long, metadata: Map[String, ByteString])(using
    statCollector: FrameInfo.StatisticCollector,
) extends FrameInfo(frameIndex, metadata):
  private object term:
    val subjectInfo = new NodeDetailInfo()
    val predicateInfo = new NodeDetailInfo()
    val objectInfo = new NodeDetailInfo()
    val graphInfo = new NodeDetailInfo()

  override def +=(other: FrameInfo): FrameInfo = {
    super.+=(other)
    other match {
      case otherDetail: FrameDetailInfo =>
        this.term.subjectInfo += otherDetail.term.subjectInfo
        this.term.predicateInfo += otherDetail.term.predicateInfo
        this.term.objectInfo += otherDetail.term.objectInfo
        this.term.graphInfo += otherDetail.term.graphInfo
      case _ =>
    }
    this
  }

  override def handleTriple(r: RdfTriple): Unit = {
    super.handleTriple(r)
    if r.hasSubject then term.subjectInfo.handle(r.getSubject)
    if r.hasPredicate then term.predicateInfo.handle(r.getPredicate)
    if r.hasObject then term.objectInfo.handle(r.getObject)
  }

  override def handleQuad(r: RdfQuad): Unit = {
    super.handleQuad(r)
    if r.hasSubject then term.subjectInfo.handle(r.getSubject)
    if r.hasPredicate then term.predicateInfo.handle(r.getPredicate)
    if r.hasObject then term.objectInfo.handle(r.getObject)
    if r.hasGraph then term.graphInfo.handle(r.getGraph)
  }

  def formatAll(): Seq[(String, Long)] =
    term.subjectInfo.format().map("subject_" ++ _ -> _) ++
      term.predicateInfo.format().map("predicate_" ++ _ -> _) ++
      term.objectInfo.format().map("object_" ++ _ -> _) ++
      term.graphInfo.format().map("graph_" ++ _ -> _)

  def formatGroupByNode(): Seq[(String, Long)] =
    val out = new NodeDetailInfo()
    out += term.subjectInfo
    out += term.predicateInfo
    out += term.objectInfo
    out += term.graphInfo
    out.format()

  def formatGroupByTerm(): Seq[(String, Long)] = {
    val name = statCollector.name()
    Seq(
      "subject_" + name -> term.subjectInfo.total(),
      "predicate_" + name -> term.predicateInfo.total(),
      "object_" + name -> term.objectInfo.total(),
      "graph_" + name -> term.graphInfo.total(),
    )
  }

end FrameDetailInfo

type Formatter = FrameInfo => Seq[(String, YamlValue)]
type DetailFormatter = FrameDetailInfo => Seq[(String, YamlValue)]

object MetricsPrinter:
  private def withFallback(formatter: DetailFormatter): Formatter = {
    case frame @ (detailInfo: FrameDetailInfo) =>
      frame.format().map(_ -> YamlLong(_)) ++ formatter(detailInfo)
    case frame @ (frameInfo: FrameInfo) => frame.format().map(_ -> YamlLong(_))
  }

  val allFormatter: Formatter = withFallback(detailInfo => {
    val splitToTriples = detailInfo.formatAll().map((k, v) => {
      val split = k.split("_", 2)
      (split(0), split(1), v)
    })
    val groupedByTerm =
      splitToTriples.groupMap((term, _, _) => term)((_, node, value) => (node, YamlLong(value)))
    val mapOfYamlMaps = groupedByTerm.map(_ -> YamlMap(_*))
    val order = IndexedSeq("subject", "predicate", "object", "graph")
    mapOfYamlMaps.toSeq.sorted(using Ordering.by[(String, Any), Int](x => order.indexOf(x._1)))
  })

  val termGroupFormatter: Formatter = withFallback(detailInfo => {
    Seq("term_details" -> YamlMap(detailInfo.formatGroupByTerm().map(_ -> YamlLong(_))*))
  })

  val nodeGroupFormatter: Formatter = withFallback(detailInfo => {
    Seq("node_details" -> YamlMap(detailInfo.formatGroupByNode().map(_ -> YamlLong(_))*))
  })

class MetricsPrinter(val formatter: Formatter):
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
  ): Seq[(String, YamlValue)] = this.formatter(frame)

end MetricsPrinter
