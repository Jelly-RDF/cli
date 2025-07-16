package eu.neverblink.jelly.cli.command.rdf.util

import com.google.protobuf.ByteString
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder
import eu.neverblink.jelly.cli.util.io.YamlDocBuilder.*
import eu.neverblink.jelly.core.proto.v1.*

import java.io.OutputStream
import scala.language.postfixOps

/** This class is used to store the metrics for a single frame
  */
class FrameInfo(val frameIndex: Long, val metadata: Map[String, ByteString]):
  var frameCount: Long = 1
  private object count:
    var a: Long = 0
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
    this.count.option += other.count.option
    this.count.name += other.count.name
    this.count.namespace += other.count.namespace
    this.count.triple += other.count.triple
    this.count.quad += other.count.quad
    this.count.prefix += other.count.prefix
    this.count.datatype += other.count.datatype
    this.count.graphStart += other.count.graphStart
    this.count.graphEnd += other.count.graphEnd
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

  protected def handleTriple(r: RdfTriple): Unit = count.triple += 1
  protected def handleQuad(r: RdfQuad): Unit = count.quad += 1
  protected def handleNameEntry(r: RdfNameEntry): Unit = count.name += 1
  protected def handlePrefixEntry(r: RdfPrefixEntry): Unit = count.prefix += 1
  protected def handleNamespaceDeclaration(r: RdfNamespaceDeclaration): Unit = count.namespace += 1
  protected def handleDatatypeEntry(r: RdfDatatypeEntry): Unit = count.datatype += 1
  protected def handleGraphStart(r: RdfGraphStart): Unit = count.graphStart += 1
  protected def handleGraphEnd(r: RdfGraphEnd): Unit = count.graphEnd += 1
  protected def handleOption(r: RdfStreamOptions): Unit = count.option += 1

  def format(): Seq[(String, Long)] = Seq(
    ("option_count", count.option),
    ("triple_count", count.triple),
    ("quad_count", count.quad),
    ("graph_start_count", count.graphStart),
    ("graph_end_count", count.graphEnd),
    ("namespace_count", count.namespace),
    ("name_count", count.name),
    ("prefix_count", count.prefix),
    ("datatype_count", count.datatype),
  )

end FrameInfo

/** Class containing statistics for each node type. Combines nodes allowed in triple terms (IRI,
  * blank node, literal, triple) and graph term in quads (IRI, blank node, literal, default graph).
  * For simplicity, this class does not validate these constraints.
  */
class NodeDetailInfo:
  private object count:
    var iri: Long = 0
    var bnode: Long = 0
    var literal: Long = 0
    var triple: Long = 0
    var defaultGraph: Long = 0

  def handle(o: Object): Unit = o match {
    case r: RdfIri => count.iri += 1
    case r: String => count.bnode += 1 // bnodes are strings
    case r: RdfLiteral => count.literal += 1
    case r: RdfTriple => count.triple += 1
    case r: RdfDefaultGraph => count.defaultGraph += 1
  }

  def format(): Seq[(String, Long)] = Seq(
    ("iri_count", count.iri),
    ("bnode_count", count.bnode),
    ("literal_count", count.literal),
    ("triple_count", count.triple),
    ("default_graph_count", count.defaultGraph),
  )

  def +=(other: NodeDetailInfo): NodeDetailInfo = {
    this.count.iri += other.count.iri
    this.count.bnode += other.count.bnode
    this.count.literal += other.count.literal
    this.count.triple += other.count.triple
    this.count.defaultGraph += other.count.defaultGraph
    this
  }

  def total(): Long = count.iri
    + count.bnode
    + count.literal
    + count.triple
    + count.defaultGraph

end NodeDetailInfo

class FrameDetailInfo(frameIndex: Long, metadata: Map[String, ByteString])
    extends FrameInfo(frameIndex, metadata):
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

  def formatFlat(): Seq[(String, Long)] =
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

  def formatGroupByTerm(): Seq[(String, Long)] = Seq(
    "subject_count" -> term.subjectInfo.total(),
    "predicate_count" -> term.predicateInfo.total(),
    "object_count" -> term.objectInfo.total(),
    "graph_count" -> term.graphInfo.total(),
  )

end FrameDetailInfo

type Formatter = FrameInfo => Seq[(String, YamlValue)]
type DetailFormatter = FrameDetailInfo => Seq[(String, YamlValue)]

object MetricsPrinter:
  private def withFallback(formatter: DetailFormatter): Formatter = {
    case frame @ (detailInfo: FrameDetailInfo) =>
      frame.format().map(_ -> YamlLong(_)) ++ formatter(detailInfo)
    case frame @ (frameInfo: FrameInfo) => frame.format().map(_ -> YamlLong(_))
  }

  val flatFormatter: Formatter = withFallback(detailInfo => {
    detailInfo.formatFlat().map(_ -> YamlLong(_))
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
